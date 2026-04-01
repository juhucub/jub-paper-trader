#Minute-cycle orchestration service.

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any
from uuid import uuid4
import logging 
from zoneinfo import ZoneInfo
from sqlalchemy import select

from agent_service.feature_vector import FeatureVector
from agent_service.normalize import normalize_and_rank_signals
from agent_service.signals import SignalGenerator
from agent_service.optimizer_qpo import OptimizerQPO
from agent_service.decision_policy import DecisionPolicy
from agent_service.exit_policy import ExitPolicy
from agent_service.debug_tools import summarize_symbol_decision, print_symbol_summary

from backend.core.settings import get_settings
from db.repositories.snapshots import create_bot_cycle_snapshot
from db.models.snapshots import BotCycleSnapshot
from services.execution_router import ExecutionRouter


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BotCycleService:
    alpaca_data_client: Any
    alpaca_client: Any
    risk_guardrails: Any
    portfolio_engine: Any
    optimizer: OptimizerQPO
    execution_router: ExecutionRouter
    db_session: Any
    decision_policy: DecisionPolicy = field(default_factory=DecisionPolicy)
    exit_policy: ExitPolicy = field(default_factory=ExitPolicy)
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
        previous_payload = self._latest_snapshot_payload()

        #pull features per symbol via 30 1-minute bars, latest quote, and news sentiment (if available)
        features, decision_summaries = self._pull_features(symbols)

        #generate raw signals_i scores from each stock i
        signals = SignalGenerator().generate(features)

        #normalize each signal_i, then rank them via z-score
        signals = normalize_and_rank_signals(signals, top_n=3, bottom_n=3)

       
        #FIXME: Convert raw symbol signals_i into relative signals
       

        #FIXME: Portfolio Construction (NVIDIA QPO Optimizer to size positions)
       
        for symbol, signal in signals.items():
            decision_summaries[symbol]["signal"] = signal
            decision_summaries[symbol]["decision_status"] = "SIGNAL_GENERATED"
            decision_summaries[symbol]["decision_reason"] = "signal_generated"

        equity = float(account.equity)
        cash = float(account.buying_power)
        current_positions = {p.symbol: float(p.qty) for p in positions}
        latest_prices = {symbol: payload["last_price"] for symbol, payload in features.items()}
        
        concentration = {
            symbol: ((qty * latest_prices.get(symbol, 0.0)) / equity) if equity > 0 else 0.0
            for symbol, qty in current_positions.items()
        }

        market_context = {
            "volatility": self._estimate_market_volatility(features),
            "liquidity": {symbol: payload.get("avg_dollar_volume", 0.0) for symbol, payload in features.items()},
        }
        policy_result = self.decision_policy.evaluate(
            signals=signals,
            portfolio_state={
                "positions": current_positions,
                "cash": cash,
                "equity": equity,
                "concentration": concentration,
            },
            market_context=market_context,
        )
        approved_signals = policy_result["approved_candidates"]
        policy_decisions = policy_result["decisions"]

        exit_policy_result = self.exit_policy.evaluate_positions(
            positions=positions,
            latest_prices=latest_prices,
            signals=signals,
            previous_payload=previous_payload,
            now_utc=started_at,
        )
        exit_policy_actions = exit_policy_result["actions"]

        for symbol, policy_decision in policy_decisions.items():
            decision_summaries[symbol]["policy_action"] = policy_decision["policy_action"]
            decision_summaries[symbol]["policy_reason"] = policy_decision["policy_reason"]
            decision_summaries[symbol]["portfolio_constraints_triggered"] = policy_decision["portfolio_constraints_triggered"]
            if policy_decision["policy_action"] == "skip":
                decision_summaries[symbol]["decision_status"] = "NO_TRADE"
                decision_summaries[symbol]["decision_reason"] = policy_decision["policy_reason"]

        for symbol, position_action in exit_policy_actions.items():
            action = str(position_action.get("action", "HOLD")).upper()
            trigger = str(position_action.get("trigger", "none"))
            decision_summaries.setdefault(symbol, {"symbol": symbol})
            decision_summaries[symbol]["position_action"] = action
            decision_summaries[symbol]["position_action_trigger"] = trigger
            decision_summaries[symbol]["position_action_trigger_type"] = position_action.get("trigger_type", "none")
            if action == "EXIT":
                approved_signals[symbol] = {
                    "direction": "flat",
                    "strength": 0.0,
                    "confidence": 1.0,
                    "expected_horizon": "immediate",
                }
                decision_summaries[symbol]["decision_status"] = "EXIT_POLICY_TRIGGERED"
                decision_summaries[symbol]["decision_reason"] = f"exit_policy:{trigger}"
            elif action == "REDUCE":
                if symbol in approved_signals and isinstance(approved_signals[symbol], dict):
                    approved_signals[symbol]["strength"] = max(
                        0.0, float(approved_signals[symbol].get("strength", 0.0)) * 0.5
                    )
                decision_summaries[symbol]["decision_status"] = "EXIT_POLICY_TRIGGERED"
                decision_summaries[symbol]["decision_reason"] = f"exit_policy:{trigger}"
            elif action == "ADD":
                if symbol in approved_signals and isinstance(approved_signals[symbol], dict):
                    approved_signals[symbol]["strength"] = float(approved_signals[symbol].get("strength", 0.0)) * 1.25
                decision_summaries[symbol]["decision_status"] = "EXIT_POLICY_TRIGGERED"
                decision_summaries[symbol]["decision_reason"] = f"exit_policy:{trigger}"

        #Optimize to target weights with OptimizerQPO for policy-approved candidates, using SPY as benchmark for risk calculations
        target_weights = self.optimizer.optimize_target_weights(approved_signals, benchmark_symbol=self.benchmark_symbol)

        for symbol, position_action in exit_policy_actions.items():
            action = str(position_action.get("action", "HOLD")).upper()
            if action == "EXIT":
                target_weights[symbol] = 0.0
            elif action == "REDUCE":
                target_weights[symbol] = float(target_weights.get(symbol, 0.0)) * 0.5
            elif action == "ADD":
                target_weights[symbol] = min(1.0, float(target_weights.get(symbol, 0.0)) * 1.25)

        for symbol in decision_summaries:
            weight = float(target_weights.get(symbol, 0.0))
            decision_summaries[symbol]["target_weight"] = weight
            if symbol in signals and weight <= 0.0:
                decision_summaries[symbol]["decision_status"] = "NO_TRADE"
                decision_summaries[symbol]["decision_reason"] = "no_target_allocation"
        
        open_sell_reservations = self._build_open_sell_reservations(orders)

        portfolio_actions, adjusted_target_weights = self._analyze_portfolio_actions(
            current_positions=current_positions,
            latest_prices=latest_prices,
            base_target_weights=target_weights,
            features=features,
            equity=equity,
            previous_payload=previous_payload,
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
            "policy_decisions": policy_decisions,
            "exit_policy_actions": exit_policy_actions,
            "exit_policy_state": exit_policy_result["state"],
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
        signals: dict[str, dict[str, float | str]],
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
            if signals and all(self._signal_strength(value) <= 0.0 for value in signals.values()):
                return "NO_DELTAS:all_signals_non_positive"
            if all(float(latest_prices.get(symbol, 0.0)) <= 0.0 for symbol in symbols):
                return "NO_DELTAS:missing_or_non_positive_prices"
            if sum(float(weight) for weight in target_weights.values()) <= 0.0:
                return "NO_DELTAS:no_target_allocation"
            return "NO_DELTAS:no_rebalance_deltas"
        return "HAS_DELTAS"

    @staticmethod
    def _estimate_market_volatility(features: dict[str, dict[str, float]]) -> float:
        if not features:
            return 0.0
        realized = [abs(float(values.get("momentum", 0.0))) for values in features.values()]
        return mean(realized) if realized else 0.0

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

            closes = [float(bar.get("c", 0.0)) for bar in bars if bar.get("c") is not None]
            sentiment_score = self._get_news_score(symbol)

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
            
            #For each stock i, build features_i via FeatureVector.build, which includes:
            feature_i = FeatureVector.build(
                bars=bars,
                quote=quote,
                sentiment_score=sentiment_score,
            )
            if feature_i["last_price"] <= 0:
                summary["decision_status"] = "NO_TRADE"
                summary["decision_reason"] = "missing_or_non_positive_prices"
                decision_summaries[symbol] = summary
                continue

            features[symbol] = feature_i

        return features, decision_summaries

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
        previous_payload: dict[str, Any],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, float]]:
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

    @staticmethod
    def _signal_strength(signal: dict[str, float | str]) -> float:
        if "score" in signal:
            return float(signal.get("score", 0.0))
        direction = str(signal.get("direction", "flat")).lower()
        strength = float(signal.get("strength", 0.0))
        if direction == "long":
            return strength
        if direction == "short":
            return -strength
        return 0.0
