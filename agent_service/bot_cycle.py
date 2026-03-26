#Minute-cycle orchestration service.

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from statistics import mean
from typing import Any
from uuid import uuid4

from agent_service.optimizer_qpo import OptimizerQPO
from db.repositories.snapshots import create_bot_cycle_snapshot
from services.execution_router import ExecutionRouter


@dataclass(slots=True)
class BotCycleService:
    alpaca_data_client: Any
    alpaca_client: Any
    risk_guardrails: Any
    portfolio_engine: Any
    optimizer: OptimizerQPO
    execution_router: ExecutionRouter
    db_session: Any
    benchmark_symbol: str = "SPY"

    def run_cycle(self, symbols: list[str]) -> dict[str, Any]:
        cycle_id = str(uuid4())
        started_at = datetime.now(timezone.utc)

        features = self._pull_features(symbols)
        signals = self._generate_signals(features)
        target_weights = self.optimizer.optimize_target_weights(signals, benchmark_symbol=self.benchmark_symbol)

        account = self.alpaca_client.get_account()
        positions = self.alpaca_client.get_positions()
        orders = self.alpaca_client.get_orders(status="open", limit=200)
        equity = float(account.equity)

        current_positions = {p.symbol: float(p.qty) for p in positions}
        latest_prices = {symbol: payload["last_price"] for symbol, payload in features.items()}
        deltas = self.execution_router.to_rebalance_deltas(target_weights, current_positions, latest_prices, equity)

        submitted_orders: list[dict[str, Any]] = []
        blocked_orders: list[dict[str, Any]] = []
        portfolio_state = {
            "equity": equity,
            "daily_realized_pnl": 0.0,
            "open_positions": len([p for p in positions if float(p.qty) != 0]),
        }
        for delta in deltas:
            decision = self.risk_guardrails.validate_order(
                candidate_order={
                    "symbol": delta.symbol,
                    "qty": delta.qty,
                    "side": delta.side,
                    "price": delta.reference_price,
                    "creates_new_position": delta.side == "buy" and current_positions.get(delta.symbol, 0.0) == 0.0,
                },
                portfolio_state=portfolio_state,
                market_state={
                    "last_price": delta.reference_price,
                    "avg_dollar_volume": features.get(delta.symbol, {}).get("avg_dollar_volume", 0.0),
                },
            )
            if not decision["allowed"]:
                blocked_orders.append({"symbol": delta.symbol, "reason": decision["reason"]})
                continue

            order = self.alpaca_client.submit_order(
                symbol=delta.symbol,
                qty=delta.qty,
                side=delta.side,
                type="market",
                time_in_force="day",
                client_order_id=f"cycle-{cycle_id}-{delta.symbol}",
            )
            submitted_orders.append({"id": order.id, "symbol": order.symbol, "side": order.side, "qty": order.qty})

        refreshed_account = self.alpaca_client.get_account()
        refreshed_positions = self.alpaca_client.get_positions()
        refreshed_orders = self.alpaca_client.get_orders(status="all", limit=200)
        reconciliation = self.portfolio_engine.sync_account_state(
            account={"cash": refreshed_account.buying_power, "equity": refreshed_account.equity},
            positions=[asdict(p) for p in refreshed_positions],
            orders=[asdict(o) for o in refreshed_orders],
        )

        snapshot_payload = {
            "cycle_id": cycle_id,
            "started_at": started_at.isoformat(),
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "symbols": symbols,
            "benchmark_symbol": self.benchmark_symbol,
            "features": features,
            "signals": signals,
            "target_weights": target_weights,
            "submitted_orders": submitted_orders,
            "blocked_orders": blocked_orders,
            "reconciliation": reconciliation,
        }
        create_bot_cycle_snapshot(self.db_session, cycle_id=cycle_id, payload=snapshot_payload)

        return snapshot_payload

    def _pull_features(self, symbols: list[str]) -> dict[str, dict[str, float]]:
        features: dict[str, dict[str, float]] = {}
        for symbol in symbols:
            bars = self.alpaca_data_client.get_historical_bars(symbol=symbol, timeframe="1Min", limit=30)
            quote = self.alpaca_data_client.get_latest_quote(symbol)
            news_score = self._get_news_score(symbol)

            closes = [float(bar.get("c", 0.0)) for bar in bars if bar.get("c") is not None]
            last_price = float(quote.get("ap") or quote.get("bp") or (closes[-1] if closes else 0.0))
            momentum = self._momentum_score(closes)
            mean_reversion = self._mean_reversion_score(closes)
            avg_dollar_volume = mean([float(bar.get("v", 0.0)) * float(bar.get("c", 0.0)) for bar in bars]) if bars else 0.0

            features[symbol] = {
                "last_price": last_price,
                "momentum": momentum,
                "mean_reversion": mean_reversion,
                "news": news_score,
                "avg_dollar_volume": avg_dollar_volume,
            }
        return features

    @staticmethod
    def _momentum_score(closes: list[float]) -> float:
        if len(closes) < 2:
            return 0.0
        start = closes[0]
        end = closes[-1]
        if start <= 0:
            return 0.0
        return (end - start) / start

    @staticmethod
    def _mean_reversion_score(closes: list[float]) -> float:
        if len(closes) < 10:
            return 0.0
        window = closes[-10:]
        baseline = mean(window)
        if baseline <= 0:
            return 0.0
        return (baseline - closes[-1]) / baseline

    def _get_news_score(self, symbol: str) -> float:
        if hasattr(self.alpaca_data_client, "get_news_sentiment"):
            return float(self.alpaca_data_client.get_news_sentiment(symbol))
        return 0.0

    def _generate_signals(self, features: dict[str, dict[str, float]]) -> dict[str, float]:
        scored: dict[str, float] = {}
        for symbol, values in features.items():
            weighted = (0.5 * values["momentum"]) + (0.4 * values["mean_reversion"]) + (0.1 * values["news"])
            scored[symbol] = max(weighted, 0.0)
        return scored
