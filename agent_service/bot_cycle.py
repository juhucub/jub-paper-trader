#Minute-cycle orchestration service.

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any
from uuid import uuid4
import logging 
from zoneinfo import ZoneInfo
from sqlalchemy import select

from agent_service.optimizer_qpo import OptimizerQPO
from backend.core.settings import get_settings
from db.repositories.snapshots import create_bot_cycle_snapshot
from db.models.snapshots import BotCycleSnapshot
from services.execution_router import ExecutionRouter
from agent_service.debug_tools import summarize_symbol_decision, print_symbol_summary

logger = logging.getLogger(__name__)
settings = get_settings()


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

    @staticmethod
    def _get_trade_hour_type(now_utc: datetime | None = None) -> str:
        current_utc = now_utc or datetime.now(timezone.utc)
        now_et = current_utc.astimezone(ZoneInfo("America/New_York"))
        minutes_since_midnight = now_et.hour * 60 + now_et.minute

        if now_et.weekday() >= 5:
            return "overnight"
        if 4 * 60 <= minutes_since_midnight < (9 * 60 + 30):
            return "pre_market"
        if (9 * 60 + 30) <= minutes_since_midnight < 16 * 60:
            return "regular"
        if 16 * 60 <= minutes_since_midnight < 20 * 60:
            return "after_hours"
        return "overnight"

    @staticmethod
    def _build_open_sell_reservations(orders: list[Any]) -> dict[str, float]:
        reservations: dict[str, float] = {}
        for order in orders:
            if str(getattr(order, "side", "")).lower() != "sell":
                continue
            qty = float(getattr(order, "qty", 0.0))
            filled_qty = float(getattr(order, "filled_qty", 0.0) or 0.0)
            remaining_qty = max(0.0, qty - filled_qty)
            if remaining_qty <= 0:
                continue
            symbol = str(getattr(order, "symbol", ""))
            if not symbol:
                continue
            reservations[symbol] = float(reservations.get(symbol, 0.0)) + remaining_qty
        return reservations


    def run_cycle(self, symbols: list[str]) -> dict[str, Any]:
        #Create cycle metadata
        cycle_id = str(uuid4())
        started_at = datetime.now(timezone.utc)

        #Load live broker context from Alpaca and include currently held symbols in feature generation.
        account = self.alpaca_client.get_account()
        positions = self.alpaca_client.get_positions()
        orders = self.alpaca_client.get_orders(status="open", limit=200)
        symbols = sorted({*symbols, *[p.symbol for p in positions if float(p.qty) != 0.0]})

        #pull features per symbol via 30 1-minute bars, latest quote, and news sentiment (if available)
        features, decision_summaries = self._pull_features(symbols)

        #Generate raw symbol scores/symbols
        signals = self._generate_signals(features)

        for symbol, signal in signals.items():
            decision_summaries[symbol]["signal"] = signal
            decision_summaries[symbol]["decision_status"] = "SIGNAL_GENERATED"
            decision_summaries[symbol]["decision_reason"] = "signal_generated"

        #Optimize to target weights with OptimizerQPO, using SPY as the benchmark for risk calculations
        target_weights = self.optimizer.optimize_target_weights(signals, benchmark_symbol=self.benchmark_symbol)

        for symbol in decision_summaries:
            weight = float(target_weights.get(symbol, 0.0))
            decision_summaries[symbol]["target_weight"] = weight
            if symbol in signals and weight <= 0.0:
                decision_summaries[symbol]["decision_status"] = "NO_TRADE"
                decision_summaries[symbol]["decision_reason"] = "no_target_allocation"
        
        equity = float(account.equity)
        current_positions = {p.symbol: float(p.qty) for p in positions}
        open_sell_reservations = self._build_open_sell_reservations(orders)
        latest_prices = {symbol: payload["last_price"] for symbol, payload in features.items()}
        
        portfolio_actions, adjusted_target_weights = self._analyze_portfolio_actions(
            current_positions=current_positions,
            latest_prices=latest_prices,
            base_target_weights=target_weights,
            features=features,
            equity=equity,
        )

        #Compute rebalance deltas via ExecutionRouter
        deltas = self.execution_router.to_rebalance_deltas(adjusted_target_weights, current_positions, latest_prices, equity)
        no_delta_reason = self._derive_no_delta_reason(
            symbols=symbols,
            signals=signals,
            target_weights=adjusted_target_weights,
            latest_prices=latest_prices,
            deltas=deltas,
            equity=equity,
        )


        #Potential candidates to order, subject to risk guardrails
        submitted_orders: list[dict[str, Any]] = []
        blocked_orders: list[dict[str, Any]] = []
        portfolio_state = {
            "equity": equity,
            "daily_realized_pnl": 0.0,
            "open_positions": len([p for p in positions if float(p.qty) != 0]),
        }
        trade_hour_type = self._get_trade_hour_type(started_at)
        should_use_extended_hours = trade_hour_type != "regular"
        #Risk check every candidate order using guardrails
        for delta in deltas:
            if delta.side == "sell":
                available_qty = max(
                    0.0,
                    float(current_positions.get(delta.symbol, 0.0))
                    - float(open_sell_reservations.get(delta.symbol, 0.0)),
                )
                if available_qty <= 0.0:
                    decision_summaries[delta.symbol]["blocked_reason"] = "insufficient_qty_after_open_sell_reservations"
                    decision_summaries[delta.symbol]["decision_status"] = "BLOCKED"
                    decision_summaries[delta.symbol]["decision_reason"] = "insufficient_qty_after_open_sell_reservations"
                    blocked_orders.append(
                        {
                            "symbol": delta.symbol,
                            "reason": "insufficient_qty_after_open_sell_reservations",
                        }
                    )
                    continue
                if delta.qty > available_qty:
                    delta.qty = round(available_qty, 4)
                    if delta.qty <= 0.0:
                        decision_summaries[delta.symbol]["blocked_reason"] = "insufficient_qty_after_open_sell_reservations"
                        decision_summaries[delta.symbol]["decision_status"] = "BLOCKED"
                        decision_summaries[delta.symbol]["decision_reason"] = "insufficient_qty_after_open_sell_reservations"
                        blocked_orders.append(
                            {
                                "symbol": delta.symbol,
                                "reason": "insufficient_qty_after_open_sell_reservations",
                            }
                        )
                        continue

            decision_summaries[delta.symbol]["candidate_order_side"] = delta.side
            decision_summaries[delta.symbol]["candidate_order_qty"] = delta.qty
            decision_summaries[delta.symbol]["decision_status"] = "CANDIDATE_ORDER"
            decision_summaries[delta.symbol]["decision_reason"] = "candidate_order_created"
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
                decision_summaries[delta.symbol]["blocked_reason"] = decision["reason"]
                decision_summaries[delta.symbol]["decision_status"] = "BLOCKED"
                decision_summaries[delta.symbol]["decision_reason"] = decision["reason"]
                blocked_orders.append({"symbol": delta.symbol, "reason": decision["reason"]})
                continue
            
            #Submit allowed market day orders to Alpaca
            order_type = "limit" if should_use_extended_hours else "market"
            order_request: dict[str, Any] = {
                "symbol": delta.symbol,
                "qty": delta.qty,
                "side": delta.side,
                "type": order_type,
                "time_in_force": "day",
                "client_order_id": f"cycle-{cycle_id}-{delta.symbol}",
                "trade_hour_type": trade_hour_type,
            }
            if should_use_extended_hours:
                order_request["limit_price"] = delta.reference_price
                order_request["extended_hours"] = True

            order = self.alpaca_client.submit_order(**order_request)
            decision_summaries[delta.symbol]["decision_status"] = "SUBMITTED"
            decision_summaries[delta.symbol]["decision_reason"] = "order_submitted"
            submitted_orders.append(
                {
                    "id": order.id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "qty": order.qty,
                    "trade_hour_type": trade_hour_type,
                }
            )
            if delta.side == "sell":
                open_sell_reservations[delta.symbol] = float(open_sell_reservations.get(delta.symbol, 0.0)) + float(delta.qty)
        for summary in decision_summaries.values():
            print_symbol_summary(summary)

        #Reconcile brokerage account post trade-submission
        refreshed_account = self.alpaca_client.get_account()
        refreshed_positions = self.alpaca_client.get_positions()
        refreshed_orders = self.alpaca_client.get_orders(status="all", limit=200)
        reconciliation = self.portfolio_engine.sync_account_state(
            account={"cash": refreshed_account.buying_power, "equity": refreshed_account.equity},
            positions=[asdict(p) for p in refreshed_positions],
            orders=[asdict(o) for o in refreshed_orders],
        )   


        #Save full cyce payload snapshot to DB for auditing and debugging
        snapshot_payload = {
            "cycle_id": cycle_id,
            "started_at": started_at.isoformat(),
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "symbols": symbols,
            "benchmark_symbol": self.benchmark_symbol,
            "features": features,
            "signals": signals,
            "target_weights": target_weights,
            "adjusted_target_weights": adjusted_target_weights,
            "portfolio_actions": portfolio_actions,
            "submitted_orders": submitted_orders,
            "blocked_orders": blocked_orders,
            "reconciliation": reconciliation,
            "decision_summaries": decision_summaries,
            "no_delta_reason": no_delta_reason,
        }
        create_bot_cycle_snapshot(self.db_session, cycle_id=cycle_id, payload=snapshot_payload)

        return snapshot_payload
    
    #High level reasoning for why no deltas are generated
    def _derive_no_delta_reason(
        self,
        symbols: list[str],
        signals: dict[str, float],
        target_weights: dict[str, float],
        latest_prices: dict[str, float],
        deltas: list[Any],
        equity: float,
    ) -> str:
        if equity <= 0:
            return "NO_DELTAS:non_positive_equity"
        if not symbols:
            return "NO_DELTAS:no_symbols"
        if not deltas:
            if signals and all(value <= 0.0 for value in signals.values()):
                return "NO_DELTAS:all_signals_non_positive"
            if all(float(latest_prices.get(symbol, 0.0)) <= 0.0 for symbol in symbols):
                return "NO_DELTAS:missing_or_non_positive_prices"
            if sum(float(weight) for weight in target_weights.values()) <= 0.0:
                return "NO_DELTAS:no_target_allocation"
            return "NO_DELTAS:no_rebalance_deltas"
        return "HAS_DELTAS"

    def _pull_features(self, symbols: list[str]) -> dict[str, dict[str, float]]:
        features: dict[str, dict[str, float]] = {}
        end = datetime.now(timezone.utc) - timedelta(minutes=20)
        start = end - timedelta(days=5)
        decision_summaries: dict[str, dict] = {}
        for symbol in symbols:
            #pull features per symbol via:
            bars = self.alpaca_data_client.get_historical_bars(
                symbol=symbol, 
                timeframe="1Min", 
                limit=30,
                start=start.isoformat(),
                end=end.isoformat(),
            )
            quote = self.alpaca_data_client.get_latest_quote(symbol)

            summary = summarize_symbol_decision(symbol, bars, quote)
            decision_summaries[symbol] = summary
            #news_score = self._get_news_score(symbol)

           
    

            #Compute feature values
            closes = [float(bar.get("c", 0.0)) for bar in bars if bar.get("c") is not None]
            last_price = float(quote.get("ap") or quote.get("bp") or (closes[-1] if closes else 0.0))
            momentum = self._momentum_score(closes)
            mean_reversion = self._mean_reversion_score(closes)
            avg_dollar_volume = mean([float(bar.get("v", 0.0)) * float(bar.get("c", 0.0)) for bar in bars]) if bars else 0.0
            
            if not bars:
                summary["decision_status"] = "NO_TRADE"
                summary["decision_reason"] = "no_bars_returned"
                decision_summaries[symbol] = summary
                continue

            if not closes:
                summary["decision_status"] = "NO_TRADE"
                summary["decision_reason"] = "no_valid_closes"
                decision_summaries[symbol] = summary
                continue

            if last_price <= 0:
                summary["decision_status"] = "NO_TRADE"
                summary["decision_reason"] = "missing_or_non_positive_prices"
                decision_summaries[symbol] = summary
                continue
            features[symbol] = {
                "last_price": last_price,
                "momentum": momentum,
                "mean_reversion": mean_reversion,
                #"news": news_score,
                "avg_dollar_volume": avg_dollar_volume,
            }
        return features, decision_summaries

    @staticmethod
    def _momentum_score(closes: list[float]) -> float:
        #momentum = (last_close - first_close) / first_close over the 30-minute window
        if len(closes) < 2:
            return 0.0
        start = closes[0]
        end = closes[-1]
        if start <= 0:
            return 0.0
        return (end - start) / start

    @staticmethod
    def _mean_reversion_score(closes: list[float]) -> float:
        #mean reversion = (avg(last 10 closes) - last_close) / avg(last 10 closes)
        if len(closes) < 10:
            return 0.0
        window = closes[-10:]
        baseline = mean(window)
        if baseline <= 0:
            return 0.0
        return (baseline - closes[-1]) / baseline

    def _get_news_score(self, symbol: str) -> float:
        #news sentiment score between -1 and 1, where -1 is very negative, 0 is neutral, and 1 is very positive
        if hasattr(self.alpaca_data_client, "get_news_sentiment"):
            return float(self.alpaca_data_client.get_news_sentiment(symbol))
        return 0.0


    def _latest_snapshot_payload(self) -> dict[str, Any]:
        row = self.db_session.execute(
            select(BotCycleSnapshot).order_by(BotCycleSnapshot.created_at.desc()).limit(1)
        ).scalar_one_or_none()
        if not row:
            return {}
        return dict(row.payload or {})

    def _analyze_portfolio_actions(
        self,
        current_positions: dict[str, float],
        latest_prices: dict[str, float],
        base_target_weights: dict[str, float],
        features: dict[str, dict[str, float]],
        equity: float,
    ) -> tuple[dict[str, dict[str, Any]], dict[str, float]]:
        previous_payload = self._latest_snapshot_payload()
        previous_features: dict[str, dict[str, float]] = previous_payload.get("features", {})

        actions: dict[str, dict[str, Any]] = {}
        adjusted = dict(base_target_weights)
        if equity <= 0:
            return actions, adjusted

        for symbol, qty in current_positions.items():
            if qty <= 0:
                continue
            price = float(latest_prices.get(symbol, 0.0))
            if price <= 0:
                continue
            current_weight = (qty * price) / equity
            base_target = float(adjusted.get(symbol, 0.0))
            feature_row = features.get(symbol, {})
            current_score = (0.5 * feature_row.get("momentum", 0.0)) + (0.5 * feature_row.get("mean_reversion", 0.0))
            prev_row = previous_features.get(symbol, {})
            previous_score = (0.5 * float(prev_row.get("momentum", 0.0))) + (0.5 * float(prev_row.get("mean_reversion", 0.0)))

            action = "hold"
            reason = "position_healthy"
            adjusted_target = base_target

            if current_score < -0.01 or (previous_score > 0 and current_score <= 0):
                action = "close"
                reason = "score_deterioration_vs_previous_snapshot"
                adjusted_target = 0.0
            elif current_score < 0.005 and current_weight > 0:
                action = "reduce"
                reason = "weak_signal_keep_half_exposure"
                adjusted_target = min(base_target, current_weight * 0.5)

            adjusted[symbol] = adjusted_target
            actions[symbol] = {
                "action": action,
                "reason": reason,
                "current_weight": current_weight,
                "base_target_weight": base_target,
                "adjusted_target_weight": adjusted_target,
                "current_score": current_score,
                "previous_score": previous_score,
            }
        return actions, adjusted

    def _generate_signals(self, features: dict[str, dict[str, float]]) -> dict[str, float]:
        scored: dict[str, float] = {}
        for symbol, values in features.items():
            #0.5*momentum + 0.5*mean_reversion, clipped to a minimum of 0.0 to avoid negative signals
            weighted = (0.5 * values["momentum"]) + (0.5 * values["mean_reversion"])
            scored[symbol] = max(weighted, 0.0)
        return scored
