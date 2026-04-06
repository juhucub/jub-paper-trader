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
from agent_service.data_quality import MarketDataValidator
from agent_service.debug_tools import summarize_symbol_decision, print_symbol_summary

from backend.core.settings import get_settings
from db.repositories.snapshots import create_bot_cycle_snapshot
from db.models.portfolio import PortfolioAccountState
from db.models.snapshots import BotCycleSnapshot
from services.execution_router import ExecutionRouter
from services.position_sizer import PositionSizer


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BotCycleService:
    alpaca_data_client: Any
    alpaca_client: Any
    risk_guardrails: Any
    portfolio_engine: Any
    optimizer: OptimizerQPO
    execution_router: ExecutionRouter
    position_sizer: PositionSizer
    db_session: Any
    decision_policy: DecisionPolicy = field(default_factory=DecisionPolicy)
    exit_policy: ExitPolicy = field(default_factory=ExitPolicy)
    benchmark_symbol: str = "SPY"
    data_validator: MarketDataValidator = field(default_factory=MarketDataValidator)
    order_ttl_seconds: int = field(default_factory=lambda: get_settings().bot_order_ttl_seconds)
    order_replace_enabled: bool = field(default_factory=lambda: get_settings().bot_order_replace_enabled)
    order_replace_slippage_bps: float = field(default_factory=lambda: get_settings().bot_order_replace_slippage_bps)
    order_replace_price_band_bps: float = field(default_factory=lambda: get_settings().bot_order_replace_price_band_bps)


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

    @staticmethod
    def _parse_order_timestamp(raw_timestamp: Any) -> datetime | None:
        if isinstance(raw_timestamp, datetime):
            return raw_timestamp if raw_timestamp.tzinfo else raw_timestamp.replace(tzinfo=timezone.utc)
        if isinstance(raw_timestamp, str):
            try:
                normalized = raw_timestamp.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(normalized)
                return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
            except ValueError:
                return None
        return None

    @staticmethod
    def _derive_replacement_limit_price(reference_price: float, side: str, slippage_bps: float) -> float:
        slippage_ratio = max(0.0, slippage_bps) / 10_000.0
        if str(side).lower() == "buy":
            return reference_price * (1.0 + slippage_ratio)
        return reference_price * (1.0 - slippage_ratio)

    def _refresh_stale_open_orders(
        self,
        cycle_id: str,
        started_at: datetime,
        orders: list[Any],
        features: dict[str, dict[str, float]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        lifecycle_actions: list[dict[str, Any]] = []
        replacement_submissions: list[dict[str, Any]] = []
        if self.order_ttl_seconds <= 0:
            return lifecycle_actions, replacement_submissions

        trade_hour_type = self._get_trade_hour_type(started_at)
        should_use_extended_hours = trade_hour_type != "regular"
        price_band_ratio = max(0.0, self.order_replace_price_band_bps) / 10_000.0

        for order in list(orders):
            created_at = self._parse_order_timestamp(getattr(order, "submitted_at", None)) or self._parse_order_timestamp(
                getattr(order, "created_at", None)
            )
            action_base: dict[str, Any] = {
                "order_id": getattr(order, "id", None),
                "symbol": getattr(order, "symbol", None),
                "side": getattr(order, "side", None),
                "status": getattr(order, "status", None),
            }
            if created_at is None:
                lifecycle_actions.append({**action_base, "action": "skipped", "reason": "missing_timestamp"})
                continue

            age_seconds = (started_at - created_at.astimezone(timezone.utc)).total_seconds()
            if age_seconds <= float(self.order_ttl_seconds):
                continue

            qty = float(getattr(order, "qty", 0.0) or 0.0)
            filled_qty = float(getattr(order, "filled_qty", 0.0) or 0.0)
            remaining_qty = round(max(0.0, qty - filled_qty), 4)
            if remaining_qty <= 0.0:
                lifecycle_actions.append({**action_base, "action": "skipped", "reason": "no_remaining_qty"})
                continue

            self.alpaca_client.cancel_order(order.id)
            lifecycle_actions.append(
                {**action_base, "action": "cancelled", "reason": "stale_ttl_exceeded", "age_seconds": age_seconds}
            )
            if not self.order_replace_enabled:
                continue

            symbol = str(getattr(order, "symbol", ""))
            reference_price = float(features.get(symbol, {}).get("last_price", 0.0) or 0.0)
            if reference_price <= 0.0:
                reference_price = self._fetch_fallback_price(symbol)
            if reference_price <= 0.0:
                lifecycle_actions.append(
                    {**action_base, "action": "replace_skipped", "reason": "missing_reference_price"}
                )
                continue

            current_limit_price = float(getattr(order, "limit_price", 0.0) or 0.0)
            if current_limit_price > 0.0 and abs(current_limit_price - reference_price) / reference_price > price_band_ratio:
                lifecycle_actions.append({**action_base, "action": "replace_skipped", "reason": "existing_order_outside_price_band"})
                continue

            replacement_limit_price = round(
                self._derive_replacement_limit_price(
                    reference_price,
                    str(getattr(order, "side", "")),
                    self.order_replace_slippage_bps,
                ),
                4,
            )
            if abs(replacement_limit_price - reference_price) / reference_price > price_band_ratio:
                lifecycle_actions.append({**action_base, "action": "replace_skipped", "reason": "replacement_price_outside_band"})
                continue

            replacement_order = self.alpaca_client.submit_order(
                symbol=symbol,
                qty=remaining_qty,
                side=str(getattr(order, "side", "")),
                type="limit",
                time_in_force=str(getattr(order, "time_in_force", "day")),
                limit_price=replacement_limit_price,
                extended_hours=should_use_extended_hours,
                trade_hour_type=trade_hour_type,
                client_order_id=f"cycle-{cycle_id}-repl-{symbol}-{str(getattr(order, 'id', 'na'))[:8]}",
            )
            replacement_submissions.append(
                {
                    "id": replacement_order.id,
                    "symbol": replacement_order.symbol,
                    "side": replacement_order.side,
                    "qty": replacement_order.qty,
                    "trade_hour_type": trade_hour_type,
                    "source": "replace_stale_order",
                }
            )
            orders.append(replacement_order)
            lifecycle_actions.append(
                {
                    **action_base,
                    "action": "replaced",
                    "replacement_order_id": replacement_order.id,
                    "replacement_limit_price": replacement_limit_price,
                    "age_seconds": age_seconds,
                }
            )
        return lifecycle_actions, replacement_submissions

    def run_cycle(self, symbols: list[str]) -> dict[str, Any]:

        cycle_context = self._load_cycle_context(symbols)

        features, decision_summaries, signals = self._build_signal_inputs(symbols)

        sizing_context = self._plan_targets_and_deltas(
            cycle_context=cycle_context,
            features=features,
            decision_summaries=decision_summaries,
            signals=signals,
        )
        order_lifecycle_actions, pre_execution_submitted_orders = self._refresh_stale_open_orders(
            cycle_id=cycle_context["cycle_id"],
            started_at=cycle_context["started_at"],
            orders=cycle_context["orders"],
            features=features,
        )
        sizing_context["open_sell_reservations"] = self._build_open_sell_reservations(cycle_context["orders"])

        submitted_orders, blocked_orders = self._execute_deltas(
            cycle_id=cycle_context["cycle_id"],
            started_at=cycle_context["started_at"],
            deltas=sizing_context["deltas"],
            equity=sizing_context["equity"],
            current_positions=sizing_context["current_positions"],
            open_sell_reservations=sizing_context["open_sell_reservations"],
            features=features,
            decision_summaries=decision_summaries,
            positions=cycle_context["positions"],
        )

        submitted_orders = pre_execution_submitted_orders + submitted_orders
        
        for summary in decision_summaries.values():
            print_symbol_summary(summary)

        reconciliation = self._reconcile_portfolio()

        snapshot_payload = {
            "cycle_id": cycle_context["cycle_id"],
            "started_at": cycle_context["started_at"].isoformat(),
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "symbols": cycle_context["symbols"],
            "benchmark_symbol": self.benchmark_symbol,
            "features": features,
            "signals": signals,
            "policy_decisions": sizing_context["policy_decisions"],
            "exit_policy_actions": sizing_context["exit_policy_actions"],
            "exit_policy_state": sizing_context["exit_policy_state"],
            "target_weights": sizing_context["target_weights"],
            "optimizer_allocation_diagnostics": sizing_context["optimizer_allocation_diagnostics"],
            "adjusted_target_weights": sizing_context["adjusted_target_weights"],
            "sized_targets": sizing_context["sized_targets"],
            "portfolio_actions": sizing_context["portfolio_actions"],
            "submitted_orders": submitted_orders,
            "blocked_orders": blocked_orders,
            "order_lifecycle_actions": order_lifecycle_actions,
            "reconciliation": reconciliation,
            "decision_summaries": decision_summaries,
            "no_delta_reason": sizing_context["no_delta_reason"],
        }
        create_bot_cycle_snapshot(self.db_session, cycle_id=cycle_context["cycle_id"], payload=snapshot_payload)

        return snapshot_payload


        #FIXME: Portfolio Construction (NVIDIA QPO Optimizer to size positions)
       
    
    #------------------------------------
    # Helper methods for cycle orchestration
    #------------------------------------
    def _load_cycle_context(self, symbols: list[str]) -> dict[str, Any]:
        #Create cycle metadata
        cycle_id = str(uuid4())
        started_at = datetime.now(timezone.utc)

        #Load live broker context from Alpaca and currently held symbols in feature generation.
        account = self.alpaca_client.get_account()
        positions = self.alpaca_client.get_positions()
        orders = self.alpaca_client.get_orders(status="open", limit=200)
        merged_symbols = sorted({*symbols, *[p.symbol for p in positions if float(p.qty) != 0.0]})
        previous_payload = self._latest_snapshot_payload()
        return {
            "cycle_id": cycle_id,
            "started_at": started_at,
            "account": account,
            "positions": positions,
            "orders": orders,
            "symbols": merged_symbols,
            "previous_payload": previous_payload,
        }
            
    def _build_signal_inputs(self, symbols: list[str])-> tuple[dict[str, dict[str, float]], dict[str, dict], dict[str, dict[str, float | str]]]:
        #pull features per symbol via 30 1-minute bars, latest quote, and news sentiment (if available)
        features, decision_summaries = self._pull_features(symbols)
        signals = SignalGenerator().generate(features)
        signals = normalize_and_rank_signals(signals, top_n=3, bottom_n=3)

        for symbol, signal in signals.items():
            decision_summaries[symbol]["signal"] = signal
            decision_summaries[symbol]["decision_status"] = "SIGNAL_GENERATED"
            decision_summaries[symbol]["decision_reason"] = "signal_generated"
        return features, decision_summaries, signals

    def _plan_targets_and_deltas(
        self,
        cycle_context: dict[str, Any],
        features: dict[str, dict[str, float]],
        decision_summaries: dict[str, dict],
        signals: dict[str, dict[str, float | str]],
    ) -> dict[str, Any]:
        account = cycle_context["account"]
        positions = cycle_context["positions"]
        orders = cycle_context["orders"]
        previous_payload = cycle_context["previous_payload"]
        started_at = cycle_context["started_at"]
        symbols = cycle_context["symbols"]

        equity = float(account.equity)
        cash = float(account.buying_power)
        current_positions = {p.symbol: float(p.qty) for p in positions}
        open_sell_reservations = self._build_open_sell_reservations(orders)
        effective_current_positions = self._apply_open_sell_reservations(
            current_positions=current_positions,
            open_sell_reservations=open_sell_reservations,
        )
        latest_prices, missing_price_symbols = self._build_latest_prices(features=features, positions=positions)
        self._annotate_missing_price_guardrails(
            decision_summaries=decision_summaries,
            missing_price_symbols=missing_price_symbols,
        )
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
        self._apply_policy_decision_annotations(decision_summaries, policy_decisions)
        self._apply_exit_policy_actions(decision_summaries, approved_signals, exit_policy_actions)

        target_weights, optimizer_allocation_diagnostics = self.optimizer.optimize_target_weights(
            approved_signals,
            benchmark_symbol=self.benchmark_symbol,
            return_diagnostics=True,
        )
        self._apply_exit_actions_to_target_weights(target_weights, exit_policy_actions)
        self._annotate_target_weights(
            decision_summaries,
            signals,
            target_weights,
            optimizer_allocation_diagnostics,
        )

        portfolio_actions, adjusted_target_weights = self._analyze_portfolio_actions(
            current_positions=current_positions,
            latest_prices=latest_prices,
            base_target_weights=target_weights,
            features=features,
            equity=equity,
            previous_payload=previous_payload,
        )
        sized_targets = self.position_sizer.size_targets(
            target_weights=adjusted_target_weights,
            signals=signals,
            current_positions=current_positions,
            latest_prices=latest_prices,
            feature_rows=features,
            equity=equity,
        )
        target_notionals = {symbol: float(payload.get("target_notional", 0.0)) for symbol, payload in sized_targets.items()}
        target_qtys = {symbol: float(payload.get("target_qty", 0.0)) for symbol, payload in sized_targets.items()}
        for symbol, sizing_payload in sized_targets.items():
            decision_summaries.setdefault(symbol, {"symbol": symbol})
            decision_summaries[symbol]["position_sizing"] = dict(sizing_payload)

        deltas = self.execution_router.to_rebalance_deltas(
            adjusted_target_weights,
            effective_current_positions,
            latest_prices,
            equity,
            target_notionals=target_notionals,
            target_qtys=target_qtys,
        )
        no_delta_reason = self._derive_no_delta_reason(
            symbols=symbols,
            signals=signals,
            target_weights=adjusted_target_weights,
            latest_prices=latest_prices,
            deltas=deltas,
            equity=equity,
        )
        return {
            "policy_decisions": policy_decisions,
            "exit_policy_actions": exit_policy_actions,
            "exit_policy_state": exit_policy_result["state"],
            "target_weights": target_weights,
            "optimizer_allocation_diagnostics": optimizer_allocation_diagnostics,
            "adjusted_target_weights": adjusted_target_weights,
            "sized_targets": sized_targets,
            "portfolio_actions": portfolio_actions,
            "deltas": deltas,
            "no_delta_reason": no_delta_reason,
            "open_sell_reservations": open_sell_reservations,
            "current_positions": current_positions,
            "equity": equity,
        }

    @staticmethod
    def _apply_open_sell_reservations(
        current_positions: dict[str, float],
        open_sell_reservations: dict[str, float],
    ) -> dict[str, float]:
        adjusted: dict[str, float] = {}
        symbols = set(current_positions) | set(open_sell_reservations)
        for symbol in symbols:
            qty = float(current_positions.get(symbol, 0.0))
            reserved_qty = float(open_sell_reservations.get(symbol, 0.0))
            adjusted[symbol] = max(0.0, qty - reserved_qty)
        return adjusted
    
    @staticmethod
    def _apply_open_sell_reservations(
        current_positions: dict[str, float],
        open_sell_reservations: dict[str, float],
    ) -> dict[str, float]:
        adjusted: dict[str, float] = {}
        symbols = set(current_positions) | set(open_sell_reservations)
        for symbol in symbols:
            qty = float(current_positions.get(symbol, 0.0))
            reserved_qty = float(open_sell_reservations.get(symbol, 0.0))
            adjusted[symbol] = max(0.0, qty - reserved_qty)
        return adjusted
    
    def _build_latest_prices(
        self,
        features: dict[str, dict[str, float]],
        positions: list[Any],
    ) -> tuple[dict[str, float], set[str]]:
        latest_prices: dict[str, float] = {}
        for symbol, payload in features.items():
            price = float(payload.get("last_price", 0.0) or 0.0)
            if price > 0.0:
                latest_prices[symbol] = price

        missing_price_symbols: set[str] = set()
        for position in positions:
            symbol = str(getattr(position, "symbol", "")).upper()
            qty = float(getattr(position, "qty", 0.0) or 0.0)
            if not symbol or qty == 0.0:
                continue
            if latest_prices.get(symbol, 0.0) > 0.0:
                continue

            broker_price = float(getattr(position, "current_price", 0.0) or 0.0)
            if broker_price > 0.0:
                latest_prices[symbol] = broker_price
                continue

            fallback_price = self._fetch_fallback_price(symbol)
            if fallback_price > 0.0:
                latest_prices[symbol] = fallback_price
                continue

            missing_price_symbols.add(symbol)

        return latest_prices, missing_price_symbols

    def _fetch_fallback_price(self, symbol: str) -> float:
        try:
            quote = self.alpaca_data_client.get_latest_quote(symbol)
        except Exception:
            logger.warning("Unable to fetch fallback quote price for symbol=%s", symbol, exc_info=True)
            return 0.0
        return float(quote.get("ap") or quote.get("bp") or 0.0)

    @staticmethod
    def _annotate_missing_price_guardrails(
        decision_summaries: dict[str, dict[str, Any]],
        missing_price_symbols: set[str],
    ) -> None:
        for symbol in sorted(missing_price_symbols):
            decision_summaries.setdefault(symbol, {"symbol": symbol})
            decision_summaries[symbol]["decision_status"] = "NO_TRADE"
            decision_summaries[symbol]["decision_reason"] = "missing_price_fallback_failed"
            decision_summaries[symbol].setdefault("reject_reasons", []).append(
                {
                    "code": "missing_price_fallback_failed",
                    "message": "No usable price from features, broker position current_price, or quote fallback.",
                    "metadata": {"symbol": symbol},
                }
            )

    @staticmethod
    def _apply_policy_decision_annotations(
        decision_summaries: dict[str, dict],
        policy_decisions: dict[str, dict[str, Any]],
    ) -> None:
        for symbol, policy_decision in policy_decisions.items():
            decision_summaries[symbol]["policy_action"] = policy_decision["policy_action"]
            decision_summaries[symbol]["policy_reason"] = policy_decision["policy_reason"]
            decision_summaries[symbol]["portfolio_constraints_triggered"] = policy_decision["portfolio_constraints_triggered"]
            if policy_decision["policy_action"] == "skip":
                decision_summaries[symbol]["decision_status"] = "NO_TRADE"
                decision_summaries[symbol]["decision_reason"] = policy_decision["policy_reason"]

    @staticmethod
    def _apply_exit_policy_actions(
        decision_summaries: dict[str, dict],
        approved_signals: dict[str, dict[str, Any]],
        exit_policy_actions: dict[str, dict[str, Any]],
    ) -> None:
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

    @staticmethod
    def _apply_exit_actions_to_target_weights(
        target_weights: dict[str, float],
        exit_policy_actions: dict[str, dict[str, Any]],
    ) -> None:
        for symbol, position_action in exit_policy_actions.items():
            action = str(position_action.get("action", "HOLD")).upper()
            if action == "EXIT":
                target_weights[symbol] = 0.0
            elif action == "REDUCE":
                target_weights[symbol] = float(target_weights.get(symbol, 0.0)) * 0.5
            elif action == "ADD":
                target_weights[symbol] = min(1.0, float(target_weights.get(symbol, 0.0)) * 1.25)

    @staticmethod
    def _annotate_target_weights(
        decision_summaries: dict[str, dict],
        signals: dict[str, dict[str, float | str]],
        target_weights: dict[str, float],
        optimizer_allocation_diagnostics: dict[str, Any] | None = None,
    ) -> None:
        per_symbol_diagnostics = (optimizer_allocation_diagnostics or {}).get("per_symbol", {})
        for symbol in decision_summaries:
            weight = float(target_weights.get(symbol, 0.0))
            decision_summaries[symbol]["target_weight"] = weight

            if symbol in per_symbol_diagnostics:
                decision_summaries[symbol]["allocation_contributions"] = {
                    "raw": float(per_symbol_diagnostics[symbol].get("raw_contribution", 0.0)),
                    "normalized": float(
                        per_symbol_diagnostics[symbol].get("normalized_allocation_contribution", 0.0)
                    ),
                    "rank_component": float(per_symbol_diagnostics[symbol].get("rank_component", 0.0)),
                    "final_relative_weight": float(per_symbol_diagnostics[symbol].get("final_relative_weight", 0.0)),
                }

            if symbol in signals and weight <= 0.0:
                signal_direction = str(signals[symbol].get("direction", "flat")).lower()
                policy_reason = str(decision_summaries[symbol].get("policy_reason", ""))
                if "short_rejected_long_only" in policy_reason or signal_direction == "short":
                    decision_summaries[symbol]["decision_status"] = "NO_TRADE"
                    decision_summaries[symbol]["decision_reason"] = "short_rejected_long_only"
                    continue
                if decision_summaries[symbol].get("decision_reason") not in {
                    "short_rejected_long_only",
                    "short_converted_to_exit_only",
                }:
                    decision_summaries[symbol]["decision_status"] = "NO_TRADE"
                    decision_summaries[symbol]["decision_reason"] = "no_target_allocation" 
                
    def _execute_deltas(
        self,
        cycle_id: str,
        started_at: datetime,
        deltas: list[Any],
        equity: float,
        current_positions: dict[str, float],
        open_sell_reservations: dict[str, float],
        features: dict[str, dict[str, float]],
        decision_summaries: dict[str, dict],
        positions: list[Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        submitted_orders: list[dict[str, Any]] = []
        blocked_orders: list[dict[str, Any]] = []
        daily_realized_pnl = self._current_daily_realized_pnl(started_at)
        portfolio_state = {
            "equity": equity,
            "daily_realized_pnl": daily_realized_pnl,
            "open_positions": len([p for p in positions if float(p.qty) != 0]),
        }
        trade_hour_type = self._get_trade_hour_type(started_at)
        should_use_extended_hours = trade_hour_type != "regular" 

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
        
        return submitted_orders, blocked_orders
   
    def _current_daily_realized_pnl(self, started_at: datetime) -> float:
        account_state = self.db_session.get(PortfolioAccountState, 1)
        if not account_state:
            return 0.0

        cycle_date = started_at.astimezone(timezone.utc).date()
        #If daily_date differs from current cycle UTC: reset, update, and flush
        if account_state.daily_date != cycle_date:
            account_state.daily_date = cycle_date
            account_state.daily_realized_pnl = 0.0
            account_state.updated_at = datetime.now(timezone.utc)
            self.db_session.flush()
            return 0.0

        return float(account_state.daily_realized_pnl or 0.0)

    def _reconcile_portfolio(self) -> dict[str, Any]:
        refreshed_account = self.alpaca_client.get_account()
        refreshed_positions = self.alpaca_client.get_positions()
        refreshed_orders = self.alpaca_client.get_orders(status="all", limit=200)

        return self.portfolio_engine.sync_account_state(
            account={"cash": refreshed_account.buying_power, "equity": refreshed_account.equity},
            positions=[asdict(p) for p in refreshed_positions],
            orders=[asdict(o) for o in refreshed_orders],
        )
   
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

            sentiment_score = self._get_news_score(symbol)
            quality_issues = self.data_validator.validate(symbol=symbol, bars=bars, quote=quote)
            summary["reject_reasons"] = [issue.as_dict() for issue in quality_issues]

            if quality_issues:
                summary["decision_status"] = "NO_TRADE"
                summary["decision_reason"] = "quality_issues"
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
                summary.setdefault("reject_reasons", []).append(
                    {
                        "code": "non_positive_price",
                        "message": "FeatureVector produced non-positive last_price.",
                        "metadata": {"symbol": symbol, "last_price": feature_i["last_price"]},
                    }
                )
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
