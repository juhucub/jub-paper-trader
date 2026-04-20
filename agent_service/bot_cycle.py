#Minute-cycle orchestration service.

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timedelta, timezone
from statistics import mean
from typing import Any, cast
from uuid import uuid4
import logging

from sqlalchemy import select
from zoneinfo import ZoneInfo

from agent_service.feature_vector import FeatureVector
from agent_service.decision_policy import DecisionPolicy
from agent_service.exit_policy import ExitPolicy
from agent_service.data_quality import MarketDataValidator
from agent_service.debug_tools import print_cycle_debug_report, summarize_symbol_decision, write_cycle_dashboard_html_file
from agent_service.interfaces import (
    AllocationLine,
    AllocationProposal,
    CycleReport,
    DecisionPolicyOutput,
    ExecutionAttempt,
    ExecutionResult,
    ExitPolicyDirective,
    ExitPolicyOutput,
    MonitoringAlert,
    MonitoringDecision,
    OrderProposal,
    PortfolioActionAnalysis,
    ReconciliationResult,
    RiskAdjustedAllocation,
    SignalBundle,
    SignalIntent,
)
from agent_service.normalize import normalize_and_rank_signals
from agent_service.optimizer_qpo import OptimizerQPO
from agent_service.scenario import ScenarioGenerator
from agent_service.signals import SignalGenerator

from backend.core.settings import get_settings
from db.models.portfolio import PortfolioAccountState
from db.models.snapshots import BotCycleSnapshot
from db.repositories.snapshots import create_bot_cycle_snapshot
from services.execution_router import ExecutionRouter
from services.position_sizer import PositionSizer


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class BotCycleService:
    # External market/broker clients
    alpaca_data_client: Any
    alpaca_client: Any

    # Core strategy / portfolio / execution components
    risk_guardrails: Any
    portfolio_engine: Any
    optimizer: OptimizerQPO
    execution_router: ExecutionRouter
    position_sizer: PositionSizer
    db_session: Any

    # Strategy policy layers
    decision_policy: DecisionPolicy = field(default_factory=DecisionPolicy)
    exit_policy: ExitPolicy = field(default_factory=ExitPolicy)
    scenario_generator: ScenarioGenerator = field(default_factory=ScenarioGenerator)

    # Global/default config
    benchmark_symbol: str = "SPY"
    data_validator: MarketDataValidator = field(default_factory=MarketDataValidator)

    # Open-order lifecycle config
    order_ttl_seconds: int = field(default_factory=lambda: get_settings().bot_order_ttl_seconds)
    order_replace_enabled: bool = field(default_factory=lambda: get_settings().bot_order_replace_enabled)
    order_replace_slippage_bps: float = field(default_factory=lambda: get_settings().bot_order_replace_slippage_bps)
    order_replace_price_band_bps: float = field(default_factory=lambda: get_settings().bot_order_replace_price_band_bps)


    # Returns one of: "pre_market", "regular", "after_hours", "overnight"
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

    # Compute how many shares are already reserved by currently open SELL orders
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
        account: Any,
        positions: list[Any],
        orders: list[Any],
        features: dict[str, dict[str, float]],
    ) -> list[ExecutionAttempt]:
        attempts: list[ExecutionAttempt] = []
        if self.order_ttl_seconds <= 0:
            return attempts

        trade_hour_type = self._get_trade_hour_type(started_at)
        should_use_extended_hours = trade_hour_type != "regular"
        price_band_ratio = max(0.0, self.order_replace_price_band_bps) / 10_000.0
        portfolio_state = self._build_portfolio_state(account=account, positions=positions, started_at=started_at)
        current_positions = {str(getattr(position, "symbol", "")): float(getattr(position, "qty", 0.0) or 0.0) for position in positions}

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
                attempts.append(
                    ExecutionAttempt(
                        cycle_id=cycle_id,
                        as_of=started_at,
                        symbol=str(action_base.get("symbol") or ""),
                        status="skipped",
                        stage="stale_order_lifecycle",
                        reason="missing_timestamp",
                        side=cast("Any", str(action_base.get("side") or "").lower() or None),
                        broker_order_id=cast("Any", action_base.get("order_id")),
                        diagnostics={"lifecycle_action": "skipped", **action_base},
                    )
                )
                continue

            age_seconds = (started_at - created_at.astimezone(timezone.utc)).total_seconds()
            if age_seconds <= float(self.order_ttl_seconds):
                continue

            qty = float(getattr(order, "qty", 0.0) or 0.0)
            filled_qty = float(getattr(order, "filled_qty", 0.0) or 0.0)
            remaining_qty = round(max(0.0, qty - filled_qty), 4)
            if remaining_qty <= 0.0:
                attempts.append(
                    ExecutionAttempt(
                        cycle_id=cycle_id,
                        as_of=started_at,
                        symbol=str(action_base.get("symbol") or ""),
                        status="skipped",
                        stage="stale_order_lifecycle",
                        reason="no_remaining_qty",
                        side=cast("Any", str(action_base.get("side") or "").lower() or None),
                        qty=remaining_qty,
                        broker_order_id=cast("Any", action_base.get("order_id")),
                        diagnostics={"lifecycle_action": "skipped", **action_base},
                    )
                )
                continue

            self.alpaca_client.cancel_order(order.id)
            if order in orders:
                orders.remove(order)
            attempts.append(
                ExecutionAttempt(
                    cycle_id=cycle_id,
                    as_of=started_at,
                    symbol=str(action_base.get("symbol") or ""),
                    status="cancelled",
                    stage="stale_order_lifecycle",
                    reason="stale_ttl_exceeded",
                    side=cast("Any", str(action_base.get("side") or "").lower() or None),
                    qty=remaining_qty,
                    broker_order_id=cast("Any", action_base.get("order_id")),
                    diagnostics={"lifecycle_action": "cancelled", "age_seconds": age_seconds, **action_base},
                )
            )
            if not self.order_replace_enabled:
                continue

            symbol = str(getattr(order, "symbol", ""))
            reference_price = float(features.get(symbol, {}).get("last_price", 0.0) or 0.0)
            if reference_price <= 0.0:
                reference_price = self._fetch_fallback_price(symbol)
            if reference_price <= 0.0:
                attempts.append(
                    ExecutionAttempt(
                        cycle_id=cycle_id,
                        as_of=started_at,
                        symbol=symbol,
                        status="skipped",
                        stage="stale_order_lifecycle",
                        reason="missing_reference_price",
                        side=cast("Any", str(action_base.get("side") or "").lower() or None),
                        qty=remaining_qty,
                        broker_order_id=cast("Any", action_base.get("order_id")),
                        reference_price=reference_price,
                        diagnostics={"lifecycle_action": "replace_skipped", **action_base},
                    )
                )
                continue

            current_limit_price = float(getattr(order, "limit_price", 0.0) or 0.0)
            if current_limit_price > 0.0 and abs(current_limit_price - reference_price) / reference_price > price_band_ratio:
                attempts.append(
                    ExecutionAttempt(
                        cycle_id=cycle_id,
                        as_of=started_at,
                        symbol=symbol,
                        status="skipped",
                        stage="stale_order_lifecycle",
                        reason="existing_order_outside_price_band",
                        side=cast("Any", str(action_base.get("side") or "").lower() or None),
                        qty=remaining_qty,
                        broker_order_id=cast("Any", action_base.get("order_id")),
                        reference_price=reference_price,
                        limit_price=current_limit_price,
                        diagnostics={"lifecycle_action": "replace_skipped", **action_base},
                    )
                )
                continue

            replacement_limit_price = round(
                self._derive_replacement_limit_price(
                    reference_price,
                    str(getattr(order, "side", "")),
                    self.order_replace_slippage_bps,
                ),
                2,
            )
            if abs(replacement_limit_price - reference_price) / reference_price > price_band_ratio:
                attempts.append(
                    ExecutionAttempt(
                        cycle_id=cycle_id,
                        as_of=started_at,
                        symbol=symbol,
                        status="skipped",
                        stage="stale_order_lifecycle",
                        reason="replacement_price_outside_band",
                        side=cast("Any", str(action_base.get("side") or "").lower() or None),
                        qty=remaining_qty,
                        broker_order_id=cast("Any", action_base.get("order_id")),
                        reference_price=reference_price,
                        limit_price=replacement_limit_price,
                        diagnostics={"lifecycle_action": "replace_skipped", **action_base},
                    )
                )
                continue

            side = str(getattr(order, "side", "")).lower()
            decision = self.risk_guardrails.validate_order(
                candidate_order={
                    "symbol": symbol,
                    "qty": remaining_qty,
                    "side": side,
                    "price": reference_price,
                    "creates_new_position": side == "buy" and current_positions.get(symbol, 0.0) == 0.0,
                },
                portfolio_state=portfolio_state,
                market_state={
                    "last_price": reference_price,
                    "avg_dollar_volume": float(features.get(symbol, {}).get("avg_dollar_volume", 0.0) or 0.0),
                },
            )
            if not decision["allowed"]:
                attempts.append(
                    ExecutionAttempt(
                        cycle_id=cycle_id,
                        as_of=started_at,
                        symbol=symbol,
                        status="blocked",
                        stage="stale_order_lifecycle",
                        reason=str(decision["reason"]),
                        side=cast("Any", side or None),
                        qty=remaining_qty,
                        broker_order_id=cast("Any", action_base.get("order_id")),
                        reference_price=reference_price,
                        limit_price=replacement_limit_price,
                        request_payload={
                            "symbol": symbol,
                            "qty": remaining_qty,
                            "side": side,
                            "price": reference_price,
                        },
                        diagnostics={"lifecycle_action": "replace_blocked", "risk_decision": dict(decision), **action_base},
                    )
                )
                continue

            attempts.append(
                ExecutionAttempt(
                    cycle_id=cycle_id,
                    as_of=started_at,
                    symbol=symbol,
                    status="skipped",
                    stage="stale_order_lifecycle",
                    reason="replacement_deferred_to_primary_execution",
                    side=cast("Any", side or None),
                    qty=remaining_qty,
                    order_type="limit",
                    reference_price=reference_price,
                    limit_price=replacement_limit_price,
                    request_payload={
                        "symbol": symbol,
                        "qty": remaining_qty,
                        "side": side,
                        "type": "limit",
                        "time_in_force": "day",
                        "limit_price": replacement_limit_price,
                        "extended_hours": should_use_extended_hours,
                        "trade_hour_type": trade_hour_type,
                    },
                    diagnostics={
                        "lifecycle_action": "replacement_deferred",
                        "risk_decision": dict(decision),
                        "replacement_limit_price": replacement_limit_price,
                        "age_seconds": age_seconds,
                        **action_base,
                    },
                    lineage={"replaces_order_id": getattr(order, "id", None)},
                )
            )
        return attempts

    # Execute one complete trading cycle
    def run_cycle(self, symbols: list[str]) -> dict[str, Any]:
        """
        Pipeline:
        1. Load current broker/account/order context
        2. Pull features and generate signals
        3. Refresh/cancel/replace stale open orders
        4. Plan target portfolio and compute order proposals
        5. Submit broker orders
        6. Build monitoring decision / health summary
        7. Reconcile portfolio state back into local storage
        8. Persist a full snapshot for auditability/debugging
        9. Return both serialized snapshot data and typed runtime objects
        """

        # Step 1: Load broker/account context and merge input symbols with held symbols
        cycle_context = self._load_cycle_context(symbols)
    
        #Step 2: Build features -> Decision summaries -> normalized ranked signals
        features, decision_summaries, signals = self._build_signal_inputs(
            cycle_context["symbols"]
        )

        # Step 3: Before placing new orders, deal with stale open orders
        # (cancel them, optionally replace them if allowed)
        stale_order_attempts = self._refresh_stale_open_orders(
            cycle_id=cycle_context["cycle_id"],
            started_at=cycle_context["started_at"],
            account=cycle_context["account"],
            positions=cycle_context["positions"],
            orders=cycle_context["orders"],
            features=features,
        )

        # Step 4: Convert signals + positions + policies into target allocations and order proposals
        sizing_context = self._plan_targets_and_deltas(
            cycle_context=cycle_context,
            features=features,
            decision_summaries=decision_summaries,
            signals=signals,
        )

        # Step 5: Submit fresh order proposals to the broker
        execution_attempts = self._submit_order_proposals(
            cycle_id=cycle_context["cycle_id"],
            started_at=cycle_context["started_at"],
            order_proposals=sizing_context["order_proposals"],
            current_positions=sizing_context["current_positions"],
            open_sell_reservations=sizing_context["open_sell_reservations"],
            decision_summaries=decision_summaries,
        )

        execution_result = self._build_execution_result(
            cycle_id=cycle_context["cycle_id"],
            as_of=cycle_context["started_at"],
            attempts=[*stale_order_attempts, *execution_attempts],
            order_proposals=sizing_context["order_proposals"],
            risk_adjusted_allocation=sizing_context["risk_adjusted_allocation"],
        )

        reconciliation_result = self._reconcile_portfolio(
            cycle_id=cycle_context["cycle_id"],
            started_at=cycle_context["started_at"],
            execution_result=execution_result,
        )

        submitted_orders = self._execution_result_to_submitted_orders(execution_result)
        blocked_orders = self._execution_result_to_blocked_orders(execution_result)
        order_lifecycle_actions = self._execution_result_to_lifecycle_actions(execution_result)
        portfolio_actions = self._portfolio_actions_to_dict(sizing_context["portfolio_action_artifacts"])
        reconciliation = self._reconciliation_result_to_dict(reconciliation_result)

        # Step 6: Build an operational/monitoring summary for this cycle
        monitoring_decision = self._build_monitoring_decision(
            as_of=cycle_context["started_at"],
            execution_result=execution_result,
            reconciliation_result=reconciliation_result,
            decision_summaries=decision_summaries,
            no_delta_reason=sizing_context["no_delta_reason"],
            scenario_bundle=sizing_context["scenario_bundle"],
            risk_adjusted_allocation=sizing_context["risk_adjusted_allocation"],
            decision_policy_output=sizing_context["decision_policy_output"],
            exit_policy_output=sizing_context["exit_policy_output"],
            portfolio_actions=sizing_context["portfolio_action_artifacts"],
        )

        if get_settings().debug_bot_cycle:
            print_cycle_debug_report(
                cycle_id=cycle_context["cycle_id"],
                as_of=cycle_context["started_at"].isoformat(),
                symbols=cycle_context["symbols"],
                execution_result=execution_result,
                monitoring_decision=monitoring_decision,
                scenario_bundle=sizing_context["scenario_bundle"],
                decision_summaries=decision_summaries,
            )
            write_cycle_dashboard_html_file(
                cycle_id=cycle_context["cycle_id"],
                as_of=cycle_context["started_at"].isoformat(),
                symbols=cycle_context["symbols"],
                execution_result=execution_result,
                monitoring_decision=monitoring_decision,
                scenario_bundle=sizing_context["scenario_bundle"],
                decision_summaries=decision_summaries,
            )

        cycle_report = self._build_cycle_report(
            cycle_id=cycle_context["cycle_id"],
            as_of=cycle_context["started_at"],
            symbols=cycle_context["symbols"],
            execution_result=execution_result,
            monitoring_decision=monitoring_decision,
            decision_policy_output=sizing_context["decision_policy_output"],
            scenario_bundle=sizing_context["scenario_bundle"],
            exit_policy_output=sizing_context["exit_policy_output"],
            risk_adjusted_allocation=sizing_context["risk_adjusted_allocation"],
        )

        # Step 8: Persist a complete audit snapshot for replay/debugging
        snapshot_payload = {
            "cycle_id": cycle_context["cycle_id"],
            "started_at": cycle_context["started_at"].isoformat(),
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "symbols": cycle_context["symbols"],
            "benchmark_symbol": self.benchmark_symbol,
            "features": features,
            "signals": signals,
            "policy_input_signal_bundle": self._serialize_for_snapshot(sizing_context["policy_input_signal_bundle"]),
            "policy_decisions": sizing_context["policy_decisions"],
            "decision_policy_output": self._serialize_for_snapshot(sizing_context["decision_policy_output"]),
            "exit_policy_actions": sizing_context["exit_policy_actions"],
            "exit_policy_state": sizing_context["exit_policy_state"],
            "exit_policy_output": self._serialize_for_snapshot(sizing_context["exit_policy_output"]),
            "target_weights": sizing_context["target_weights"],
            "optimizer_allocation_diagnostics": sizing_context["optimizer_allocation_diagnostics"],
            "adjusted_target_weights": sizing_context["adjusted_target_weights"],
            "sized_targets": sizing_context["sized_targets"],
            "portfolio_actions": portfolio_actions,
            "submitted_orders": submitted_orders,
            "blocked_orders": blocked_orders,
            "order_lifecycle_actions": order_lifecycle_actions,
            "reconciliation": reconciliation,
            "decision_summaries": decision_summaries,
            "no_delta_reason": sizing_context["no_delta_reason"],
            "symbol_lineage": self._build_symbol_lineage(
                signals=signals,
                scenario_bundle=sizing_context["scenario_bundle"],
                decision_policy_output=sizing_context["decision_policy_output"],
                exit_policy_output=sizing_context["exit_policy_output"],
                allocation_proposal=sizing_context["allocation_proposal"],
                risk_adjusted_allocation=sizing_context["risk_adjusted_allocation"],
                portfolio_actions=sizing_context["portfolio_action_artifacts"],
                order_proposals=sizing_context["order_proposals"],
                execution_result=execution_result,
                reconciliation_result=reconciliation_result,
                monitoring_decision=monitoring_decision,
            ),
            "signal_bundle": self._serialize_for_snapshot(sizing_context["signal_bundle"]),
            "scenario_bundle": self._serialize_for_snapshot(sizing_context["scenario_bundle"]),
            "allocation_proposal": self._serialize_for_snapshot(sizing_context["allocation_proposal"]),
            "risk_adjusted_allocation": self._serialize_for_snapshot(sizing_context["risk_adjusted_allocation"]),
            "execution_result": self._serialize_for_snapshot(execution_result),
            "reconciliation_result": self._serialize_for_snapshot(reconciliation_result),
            "order_proposals": self._serialize_for_snapshot(sizing_context["order_proposals"]),
            "monitoring_decision": self._serialize_for_snapshot(monitoring_decision),
            "cycle_report": self._serialize_for_snapshot(cycle_report),
        }
        serialized_snapshot_payload = cast(
            "dict[str, Any]",
            self._serialize_for_snapshot(snapshot_payload),
        )
        create_bot_cycle_snapshot(
            self.db_session,
            cycle_id=cycle_context["cycle_id"],
            payload=serialized_snapshot_payload,
        )

        # Step 9: Return a hybrid payload:
        # - serialized objects for persistence / debugging
        # - typed objects for in-process downstream use
        return {
            **serialized_snapshot_payload,
            "policy_input_signal_bundle": sizing_context["policy_input_signal_bundle"],
            "decision_policy_output": sizing_context["decision_policy_output"],
            "exit_policy_output": sizing_context["exit_policy_output"],
            "signal_bundle": sizing_context["signal_bundle"],
            "scenario_bundle": sizing_context["scenario_bundle"],
            "allocation_proposal": sizing_context["allocation_proposal"],
            "risk_adjusted_allocation": sizing_context["risk_adjusted_allocation"],
            "execution_result": execution_result,
            "reconciliation_result": reconciliation_result,
            "order_proposals": sizing_context["order_proposals"],
            "monitoring_decision": monitoring_decision,
            "cycle_report": cycle_report,
        }
       
    
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
            
    def _build_signal_inputs(self, symbols: list[str]) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
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
        signals: dict[str, dict[str, Any]],
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
        policy_input_signal_bundle = self._build_signal_bundle(
            signals=signals,
            features=features,
            as_of=started_at,
        )
        scenario_bundle = self.scenario_generator.build(
            signal_bundle=policy_input_signal_bundle,
            market_state=market_context,
            as_of=started_at,
        )
        decision_policy_output = self.decision_policy.evaluate(
            signal_bundle=policy_input_signal_bundle,
            portfolio_state={
                "positions": current_positions,
                "cash": cash,
                "equity": equity,
                "concentration": concentration,
            },
            market_context=market_context,
            scenario_bundle=scenario_bundle,
            as_of=started_at,
        )
        policy_decisions = self._decision_policy_output_to_dict(decision_policy_output)

        exit_policy_output = self.exit_policy.evaluate_positions(
            positions=positions,
            latest_prices=latest_prices,
            signal_bundle=decision_policy_output.approved_signal_bundle,
            previous_payload=previous_payload,
            scenario_bundle=scenario_bundle,
            now_utc=started_at,
        )
        exit_policy_actions = self._exit_policy_output_to_dict(exit_policy_output)
        self._apply_policy_decision_annotations(decision_summaries, decision_policy_output)
        self._apply_exit_policy_actions(decision_summaries, exit_policy_output)

        signal_bundle = exit_policy_output.adjusted_signal_bundle
        allocation_proposal = self.optimizer.optimize_allocation(
            signal_bundle=signal_bundle,
            scenario_bundle=scenario_bundle,
            current_positions=current_positions,
            latest_prices=latest_prices,
            equity=equity,
        )
        allocation_proposal = self._attach_proposal_lineage(
            proposal=allocation_proposal,
            decision_policy_output=decision_policy_output,
            exit_policy_output=exit_policy_output,
        )
        target_weights = self._proposal_to_target_weights(allocation_proposal)
        self._apply_exit_actions_to_target_weights(target_weights, exit_policy_output.directives)
        allocation_proposal = self._replace_proposal_weights(
            proposal=allocation_proposal,
            target_weights=target_weights,
            reason="exit_policy_adjustments",
        )
        self._annotate_target_weights(
            decision_summaries,
            signals,
            target_weights,
            allocation_proposal.diagnostics,
            scenario_bundle=scenario_bundle,
        )

        portfolio_action_artifacts, adjusted_target_weights = self._analyze_portfolio_actions(
            cycle_id=cycle_context["cycle_id"],
            as_of=started_at,
            current_positions=current_positions,
            latest_prices=latest_prices,
            base_target_weights=target_weights,
            features=features,
            equity=equity,
            previous_payload=previous_payload,
            exit_policy_actions=exit_policy_actions,
            signal_bundle=signal_bundle,
            scenario_bundle=scenario_bundle,
            decision_policy_output=decision_policy_output,
            exit_policy_output=exit_policy_output,
        )
        self._annotate_portfolio_actions(
            decision_summaries=decision_summaries,
            portfolio_actions=portfolio_action_artifacts,
        )
        adjusted_allocation_proposal = self._replace_proposal_weights(
            proposal=allocation_proposal,
            target_weights=adjusted_target_weights,
            reason="portfolio_actions",
        )
        sized_allocation_proposal = self.position_sizer.size_allocation(
            proposal=adjusted_allocation_proposal,
            signal_bundle=signal_bundle,
            current_positions=current_positions,
            latest_prices=latest_prices,
            feature_rows=features,
            equity=equity,
        )
        sized_targets = self._proposal_to_sized_targets(sized_allocation_proposal)
        for symbol, sizing_payload in sized_targets.items():
            decision_summaries.setdefault(symbol, {"symbol": symbol})
            decision_summaries[symbol]["position_sizing"] = dict(sizing_payload)

        portfolio_state = self._build_portfolio_state(account=account, positions=positions, started_at=started_at)
        risk_adjusted_allocation = self.risk_guardrails.validate_allocation(
            proposal=sized_allocation_proposal,
            execution_router=self.execution_router,
            current_positions=effective_current_positions,
            latest_prices=latest_prices,
            equity=equity,
            portfolio_state=portfolio_state,
            market_states={
                symbol: {
                    "last_price": float(latest_prices.get(symbol, 0.0) or 0.0),
                    "avg_dollar_volume": float(features.get(symbol, {}).get("avg_dollar_volume", 0.0) or 0.0),
                }
                for symbol in {line.symbol for line in sized_allocation_proposal.lines}
            },
        )
        self._annotate_risk_adjustment(
            decision_summaries=decision_summaries,
            risk_adjusted_allocation=risk_adjusted_allocation,
        )
        order_proposals, deltas = self._build_order_proposals(
            cycle_id=cycle_context["cycle_id"],
            started_at=started_at,
            risk_adjusted_allocation=risk_adjusted_allocation,
            effective_current_positions=effective_current_positions,
            open_sell_reservations=open_sell_reservations,
            latest_prices=latest_prices,
            equity=equity,
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
            "policy_input_signal_bundle": policy_input_signal_bundle,
            "decision_policy_output": decision_policy_output,
            "policy_decisions": policy_decisions,
            "exit_policy_actions": exit_policy_actions,
            "exit_policy_state": exit_policy_output.state,
            "exit_policy_output": exit_policy_output,
            "target_weights": target_weights,
            "optimizer_allocation_diagnostics": allocation_proposal.diagnostics,
            "adjusted_target_weights": adjusted_target_weights,
            "sized_targets": sized_targets,
            "portfolio_action_artifacts": portfolio_action_artifacts,
            "signal_bundle": signal_bundle,
            "scenario_bundle": scenario_bundle,
            "allocation_proposal": sized_allocation_proposal,
            "risk_adjusted_allocation": risk_adjusted_allocation,
            "order_proposals": order_proposals,
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
    def _decision_policy_output_to_dict(policy_output: DecisionPolicyOutput) -> dict[str, dict[str, Any]]:
        decisions: dict[str, dict[str, Any]] = {}
        for decision in policy_output.decisions:
            decisions[decision.symbol] = {
                "policy_action": decision.policy_action,
                "policy_reason": decision.reason,
                "portfolio_constraints_triggered": [constraint.code for constraint in decision.constraints],
                "score": decision.requested_intent.score,
                "confidence": decision.requested_intent.confidence,
                "signal_action": decision.requested_intent.action,
                "approved_intent": BotCycleService._serialize_for_snapshot(decision.approved_intent),
                "diagnostics": BotCycleService._serialize_for_snapshot(decision.diagnostics),
            }
        return decisions

    @staticmethod
    def _exit_policy_output_to_dict(exit_policy_output: ExitPolicyOutput) -> dict[str, dict[str, Any]]:
        return {
            directive.symbol: {
                "action": directive.action,
                "trigger": directive.trigger,
                "trigger_type": directive.trigger_type,
                "target_weight_multiplier": directive.target_weight_multiplier,
                "force_target_weight": directive.force_target_weight,
                "diagnostics": BotCycleService._serialize_for_snapshot(directive.diagnostics),
            }
            for directive in exit_policy_output.directives
        }

    @staticmethod
    def _apply_policy_decision_annotations(
        decision_summaries: dict[str, dict],
        policy_output: DecisionPolicyOutput,
    ) -> None:
        for decision in policy_output.decisions:
            decision_summaries.setdefault(decision.symbol, {"symbol": decision.symbol})
            decision_summaries[decision.symbol]["policy_action"] = decision.policy_action
            decision_summaries[decision.symbol]["policy_reason"] = decision.reason
            decision_summaries[decision.symbol]["portfolio_constraints_triggered"] = [
                constraint.code for constraint in decision.constraints
            ]
            decision_summaries[decision.symbol]["policy_constraints_detail"] = [
                BotCycleService._serialize_for_snapshot(constraint) for constraint in decision.constraints
            ]
            if decision.policy_action == "skip":
                decision_summaries[decision.symbol]["decision_status"] = "NO_TRADE"
                decision_summaries[decision.symbol]["decision_reason"] = decision.reason

    @staticmethod
    def _apply_exit_policy_actions(
        decision_summaries: dict[str, dict],
        exit_policy_output: ExitPolicyOutput,
    ) -> None:
        for directive in exit_policy_output.directives:
            symbol = directive.symbol
            action = directive.action
            trigger = directive.trigger
            decision_summaries.setdefault(symbol, {"symbol": symbol})
            decision_summaries[symbol]["position_action"] = action
            decision_summaries[symbol]["position_action_trigger"] = trigger
            decision_summaries[symbol]["position_action_trigger_type"] = directive.trigger_type
            decision_summaries[symbol]["position_action_diagnostics"] = BotCycleService._serialize_for_snapshot(
                directive.diagnostics
            )
            if action == "EXIT":
                decision_summaries[symbol]["decision_status"] = "EXIT_POLICY_TRIGGERED"
                decision_summaries[symbol]["decision_reason"] = f"exit_policy:{trigger}"
            elif action in {"REDUCE", "ADD"}:
                decision_summaries[symbol]["decision_status"] = "EXIT_POLICY_TRIGGERED"
                decision_summaries[symbol]["decision_reason"] = f"exit_policy:{trigger}"

    @staticmethod
    def _apply_exit_actions_to_target_weights(
        target_weights: dict[str, float],
        directives: list[ExitPolicyDirective],
    ) -> None:
        for directive in directives:
            symbol = directive.symbol
            action = directive.action
            if action == "EXIT":
                target_weights[symbol] = float(directive.force_target_weight or 0.0)
            elif action == "REDUCE":
                target_weights[symbol] = float(target_weights.get(symbol, 0.0)) * float(
                    directive.target_weight_multiplier or 1.0
                )
            elif action == "ADD":
                target_weights[symbol] = min(
                    1.0,
                    float(target_weights.get(symbol, 0.0)) * float(directive.target_weight_multiplier or 1.0),
                )

    @staticmethod
    def _annotate_target_weights(
        decision_summaries: dict[str, dict],
        signals: dict[str, dict[str, float | str]],
        target_weights: dict[str, float],
        optimizer_allocation_diagnostics: dict[str, Any] | None = None,
        scenario_bundle: SignalBundle | Any | None = None,
    ) -> None:
        per_symbol_diagnostics = (optimizer_allocation_diagnostics or {}).get("per_symbol", {})
        for symbol in decision_summaries:
            weight = float(target_weights.get(symbol, 0.0))
            decision_summaries[symbol]["target_weight"] = weight
            if scenario_bundle is not None:
                decision_summaries[symbol]["scenario_regime"] = getattr(scenario_bundle, "regime_label", None)

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

    def _build_signal_bundle(
        self,
        signals: dict[str, dict[str, Any]],
        features: dict[str, dict[str, float]],
        as_of: datetime,
    ) -> SignalBundle:
        intents: list[SignalIntent] = []
        for symbol in sorted(signals):
            signal = signals[symbol]
            raw_diagnostics = dict(signal.get("diagnostics", {}) or {})
            direction = str(signal.get("direction", "flat")).lower()
            if direction not in {"long", "short", "flat"}:
                direction = "flat"
            intent_direction: Any = direction
            action = str(signal.get("action", {"long": "buy", "short": "sell", "flat": "hold"}[direction])).lower()
            if action not in {"buy", "sell", "hold"}:
                action = "hold"
            intent_action: Any = action
            score = float(
                signal.get(
                    "score",
                    signal.get(
                        "raw_score",
                        float(signal.get("strength", 0.0) or 0.0) if direction == "long" else 0.0,
                    ),
                )
                or 0.0
            )
            confidence = float(signal.get("confidence", 0.0) or 0.0)
            rank = int(signal.get("rank", 0) or 0)
            rank_bucket = str(signal.get("rank_bucket", "HOLD"))
            intents.append(
                SignalIntent(
                    symbol=symbol,
                    direction=intent_direction,
                    action=intent_action,
                    score=score,
                    confidence=confidence,
                    expected_return=float(signal.get("expected_return", score) or 0.0),
                    normalized_score=float(signal.get("normalized_score", signal.get("z_score", 0.0)) or 0.0),
                    rank=rank,
                    rationale=f"rank_bucket={rank_bucket} confidence={confidence:.2f}",
                    diagnostics={
                        **raw_diagnostics,
                        "strength": float(signal.get("strength", abs(score)) or 0.0),
                        "z_score": float(signal.get("z_score", 0.0) or 0.0),
                        "rank_bucket": rank_bucket,
                        "universe_size": int(signal.get("universe_size", len(signals)) or len(signals) or 1),
                    },
                )
            )
        return SignalBundle(
            as_of=as_of,
            benchmark_symbol=self.benchmark_symbol,
            intents=intents,
            feature_snapshot={symbol: dict(features.get(symbol, {})) for symbol in sorted(features)},
            notes=["Raw signal bundle captures the statistical proposal set before policy vetting."],
            lineage={"stage": "signal_generation"},
        )

    @staticmethod
    def _proposal_to_target_weights(proposal: AllocationProposal) -> dict[str, float]:
        target_weights = {line.symbol: float(line.target_weight) for line in proposal.lines}
        return target_weights

    @staticmethod
    def _replace_proposal_weights(
        proposal: AllocationProposal,
        target_weights: dict[str, float],
        reason: str,
    ) -> AllocationProposal:
        updated_lines = [
            AllocationLine(
                symbol=line.symbol,
                target_weight=float(target_weights.get(line.symbol, line.target_weight)),
                confidence=line.confidence,
                rationale=line.rationale,
                target_notional=line.target_notional,
                target_qty=line.target_qty,
                diagnostics={**line.diagnostics, "weight_adjustment_reason": reason},
            )
            for line in proposal.lines
        ]
        return AllocationProposal(
            as_of=proposal.as_of,
            source_signal_bundle_at=proposal.source_signal_bundle_at,
            source_scenario_bundle_at=proposal.source_scenario_bundle_at,
            source_decision_policy_at=proposal.source_decision_policy_at,
            source_exit_policy_at=proposal.source_exit_policy_at,
            target_gross_exposure=proposal.target_gross_exposure,
            cash_buffer=proposal.cash_buffer,
            lines=updated_lines,
            scenario_regime=proposal.scenario_regime,
            optimizer_name=proposal.optimizer_name,
            constraints_requested=dict(proposal.constraints_requested),
            diagnostics={**proposal.diagnostics, "latest_adjustment_reason": reason},
            lineage=dict(proposal.lineage),
        )

    @staticmethod
    def _attach_proposal_lineage(
        proposal: AllocationProposal,
        decision_policy_output: DecisionPolicyOutput,
        exit_policy_output: ExitPolicyOutput,
    ) -> AllocationProposal:
        return AllocationProposal(
            as_of=proposal.as_of,
            source_signal_bundle_at=proposal.source_signal_bundle_at,
            source_scenario_bundle_at=proposal.source_scenario_bundle_at,
            source_decision_policy_at=decision_policy_output.as_of,
            source_exit_policy_at=exit_policy_output.as_of,
            target_gross_exposure=proposal.target_gross_exposure,
            cash_buffer=proposal.cash_buffer,
            lines=proposal.lines,
            scenario_regime=proposal.scenario_regime,
            optimizer_name=proposal.optimizer_name,
            constraints_requested=dict(proposal.constraints_requested),
            diagnostics=dict(proposal.diagnostics),
            lineage={
                **dict(proposal.lineage),
                "source_decision_policy_at": decision_policy_output.as_of.isoformat(),
                "source_exit_policy_at": exit_policy_output.as_of.isoformat(),
            },
        )

    @staticmethod
    def _proposal_to_sized_targets(proposal: AllocationProposal) -> dict[str, dict[str, float | str]]:
        sized_targets: dict[str, dict[str, float | str]] = {}
        for line in proposal.lines:
            sized_targets[line.symbol] = {
                "target_weight": line.target_weight,
                "target_notional": float(line.target_notional or 0.0),
                "target_qty": float(line.target_qty or 0.0),
                **{
                    key: value
                    for key, value in line.diagnostics.items()
                    if isinstance(value, (int, float, str))
                },
            }
        return sized_targets

    @staticmethod
    def _annotate_risk_adjustment(
        decision_summaries: dict[str, dict[str, Any]],
        risk_adjusted_allocation: Any,
    ) -> None:
        for detail in getattr(risk_adjusted_allocation, "symbol_details", []):
            decision_summaries.setdefault(detail.symbol, {"symbol": detail.symbol})
            decision_summaries[detail.symbol]["risk_adjustment"] = BotCycleService._serialize_for_snapshot(detail)
            if detail.status == "blocked":
                reason = detail.reasons[0].code if detail.reasons else "risk_blocked"
                decision_summaries[detail.symbol]["blocked_reason"] = reason
                decision_summaries[detail.symbol]["decision_status"] = "BLOCKED"
                decision_summaries[detail.symbol]["decision_reason"] = reason

    @staticmethod
    def _annotate_portfolio_actions(
        decision_summaries: dict[str, dict[str, Any]],
        portfolio_actions: list[PortfolioActionAnalysis],
    ) -> None:
        for action in portfolio_actions:
            decision_summaries.setdefault(action.symbol, {"symbol": action.symbol})
            decision_summaries[action.symbol]["portfolio_action"] = BotCycleService._serialize_for_snapshot(action)

    def _build_order_proposals(
        self,
        cycle_id: str,
        started_at: datetime,
        risk_adjusted_allocation: Any,
        effective_current_positions: dict[str, float],
        open_sell_reservations: dict[str, float],
        latest_prices: dict[str, float],
        equity: float,
    ) -> tuple[list[OrderProposal], list[Any]]:
        approved_symbols = {line.symbol for line in risk_adjusted_allocation.approved_lines}
        deltas = self.execution_router.to_rebalance_deltas(
            target_weights={line.symbol: line.target_weight for line in risk_adjusted_allocation.approved_lines},
            current_positions={symbol: float(effective_current_positions.get(symbol, 0.0)) for symbol in approved_symbols},
            latest_prices=latest_prices,
            equity=equity,
            target_notionals={
                line.symbol: line.target_notional
                for line in risk_adjusted_allocation.approved_lines
                if line.target_notional is not None
            },
            target_qtys={
                line.symbol: line.target_qty
                for line in risk_adjusted_allocation.approved_lines
                if line.target_qty is not None
            },
        )
        trade_hour_type = self._get_trade_hour_type(started_at)
        should_use_extended_hours = trade_hour_type != "regular"
        line_lookup = {line.symbol: line for line in risk_adjusted_allocation.approved_lines}
        proposals: list[OrderProposal] = []
        approved_deltas: list[Any] = []
        for delta in deltas:
            if delta.side == "buy" and float(open_sell_reservations.get(delta.symbol, 0.0)) > 0.0:
                continue
            line = line_lookup[delta.symbol]
            approved_deltas.append(delta)
            proposals.append(
                OrderProposal(
                    cycle_id=cycle_id,
                    symbol=delta.symbol,
                    side="buy" if delta.side == "buy" else "sell",
                    qty=delta.qty,
                    order_type="limit" if should_use_extended_hours else "market",
                    rationale=line.rationale or "rebalance_to_risk_approved_target",
                    reference_price=delta.reference_price,
                    time_in_force="day",
                    limit_price=delta.reference_price if should_use_extended_hours else None,
                    extended_hours=should_use_extended_hours,
                    approved_allocation_at=risk_adjusted_allocation.source_allocation_at,
                    source_signal_bundle_at=risk_adjusted_allocation.source_signal_bundle_at,
                    source_scenario_bundle_at=risk_adjusted_allocation.source_scenario_bundle_at,
                    source_decision_policy_at=risk_adjusted_allocation.source_decision_policy_at,
                    source_exit_policy_at=risk_adjusted_allocation.source_exit_policy_at,
                    diagnostics={
                        "trade_hour_type": trade_hour_type,
                        "client_order_id": f"cycle-{cycle_id}-{delta.symbol}",
                        "target_weight": delta.target_weight,
                        "current_weight": delta.current_weight,
                        "current_qty": delta.current_qty,
                        "desired_qty": delta.desired_qty,
                        "notional_delta": delta.notional_delta,
                    },
                )
            )
        return proposals, approved_deltas

    def _submit_order_proposals(
        self,
        cycle_id: str,
        started_at: datetime,
        order_proposals: list[OrderProposal],
        current_positions: dict[str, float],
        open_sell_reservations: dict[str, float],
        decision_summaries: dict[str, dict],
    ) -> list[ExecutionAttempt]:
        attempts: list[ExecutionAttempt] = []
        _ = cycle_id

        for proposal in order_proposals:
            if proposal.side == "sell":
                available_qty = max(
                    0.0,
                    float(current_positions.get(proposal.symbol, 0.0))
                    - float(open_sell_reservations.get(proposal.symbol, 0.0)),
                )
                if available_qty <= 0.0:
                    decision_summaries[proposal.symbol]["blocked_reason"] = "insufficient_qty_after_open_sell_reservations"
                    decision_summaries[proposal.symbol]["decision_status"] = "BLOCKED"
                    decision_summaries[proposal.symbol]["decision_reason"] = "insufficient_qty_after_open_sell_reservations"
                    attempts.append(
                        ExecutionAttempt(
                            cycle_id=proposal.cycle_id,
                            as_of=started_at,
                            symbol=proposal.symbol,
                            status="blocked",
                            stage="primary_execution",
                            reason="insufficient_qty_after_open_sell_reservations",
                            side=proposal.side,
                            qty=proposal.qty,
                            order_type=proposal.order_type,
                            reference_price=proposal.reference_price,
                            limit_price=proposal.limit_price,
                            request_payload=self._proposal_to_order_request(proposal),
                            source_order_proposal=proposal,
                            source_signal_bundle_at=proposal.source_signal_bundle_at,
                            source_scenario_bundle_at=proposal.source_scenario_bundle_at,
                            source_decision_policy_at=proposal.source_decision_policy_at,
                            source_exit_policy_at=proposal.source_exit_policy_at,
                            source_allocation_at=proposal.approved_allocation_at,
                            diagnostics={"available_qty": available_qty},
                        )
                    )
                    continue
                if proposal.qty > available_qty:
                    proposal = OrderProposal(
                        cycle_id=proposal.cycle_id,
                        symbol=proposal.symbol,
                        side=proposal.side,
                        qty=round(available_qty, 4),
                        order_type=proposal.order_type,
                        rationale=proposal.rationale,
                        reference_price=proposal.reference_price,
                        time_in_force=proposal.time_in_force,
                        limit_price=proposal.limit_price,
                        extended_hours=proposal.extended_hours,
                        source_layer=proposal.source_layer,
                        approved_allocation_at=proposal.approved_allocation_at,
                        source_signal_bundle_at=proposal.source_signal_bundle_at,
                        source_scenario_bundle_at=proposal.source_scenario_bundle_at,
                        source_decision_policy_at=proposal.source_decision_policy_at,
                        source_exit_policy_at=proposal.source_exit_policy_at,
                        diagnostics=dict(proposal.diagnostics),
                    )
                    if proposal.qty <= 0.0:
                        decision_summaries[proposal.symbol]["blocked_reason"] = "insufficient_qty_after_open_sell_reservations"
                        decision_summaries[proposal.symbol]["decision_status"] = "BLOCKED"
                        decision_summaries[proposal.symbol]["decision_reason"] = "insufficient_qty_after_open_sell_reservations"
                        attempts.append(
                            ExecutionAttempt(
                                cycle_id=proposal.cycle_id,
                                as_of=started_at,
                                symbol=proposal.symbol,
                                status="blocked",
                                stage="primary_execution",
                                reason="insufficient_qty_after_open_sell_reservations",
                                side=proposal.side,
                                qty=proposal.qty,
                                order_type=proposal.order_type,
                                reference_price=proposal.reference_price,
                                limit_price=proposal.limit_price,
                                request_payload=self._proposal_to_order_request(proposal),
                                source_order_proposal=proposal,
                                source_signal_bundle_at=proposal.source_signal_bundle_at,
                                source_scenario_bundle_at=proposal.source_scenario_bundle_at,
                                source_decision_policy_at=proposal.source_decision_policy_at,
                                source_exit_policy_at=proposal.source_exit_policy_at,
                                source_allocation_at=proposal.approved_allocation_at,
                                diagnostics={"available_qty": available_qty},
                            )
                        )
                        continue

            decision_summaries[proposal.symbol]["candidate_order_side"] = proposal.side
            decision_summaries[proposal.symbol]["candidate_order_qty"] = proposal.qty
            decision_summaries[proposal.symbol]["decision_status"] = "CANDIDATE_ORDER"
            decision_summaries[proposal.symbol]["decision_reason"] = "candidate_order_created"

            try:
                order = self._submit_order_proposal(proposal)
            except Exception as exc:
                decision_summaries[proposal.symbol]["decision_status"] = "BLOCKED"
                decision_summaries[proposal.symbol]["decision_reason"] = "broker_submission_failed"
                decision_summaries[proposal.symbol]["blocked_reason"] = "broker_submission_failed"
                attempts.append(
                    ExecutionAttempt(
                        cycle_id=proposal.cycle_id,
                        as_of=started_at,
                        symbol=proposal.symbol,
                        status="failed",
                        stage="primary_execution",
                        reason="broker_submission_failed",
                        side=proposal.side,
                        qty=proposal.qty,
                        order_type=proposal.order_type,
                        reference_price=proposal.reference_price,
                        limit_price=proposal.limit_price,
                        request_payload=self._proposal_to_order_request(proposal),
                        response_payload={"error": str(exc)},
                        source_order_proposal=proposal,
                        source_signal_bundle_at=proposal.source_signal_bundle_at,
                        source_scenario_bundle_at=proposal.source_scenario_bundle_at,
                        source_decision_policy_at=proposal.source_decision_policy_at,
                        source_exit_policy_at=proposal.source_exit_policy_at,
                        source_allocation_at=proposal.approved_allocation_at,
                    )
                )
                continue

            decision_summaries[proposal.symbol]["decision_status"] = "SUBMITTED"
            decision_summaries[proposal.symbol]["decision_reason"] = "order_submitted"
            attempts.append(
                ExecutionAttempt(
                    cycle_id=proposal.cycle_id,
                    as_of=started_at,
                    symbol=proposal.symbol,
                    status="submitted",
                    stage="primary_execution",
                    reason="order_submitted",
                    side=proposal.side,
                    qty=proposal.qty,
                    order_type=proposal.order_type,
                    reference_price=proposal.reference_price,
                    limit_price=proposal.limit_price,
                    broker_order_id=getattr(order, "id", None),
                    request_payload=self._proposal_to_order_request(proposal),
                    response_payload=self._serialize_submitted_order(order, proposal),
                    source_order_proposal=proposal,
                    source_signal_bundle_at=proposal.source_signal_bundle_at,
                    source_scenario_bundle_at=proposal.source_scenario_bundle_at,
                    source_decision_policy_at=proposal.source_decision_policy_at,
                    source_exit_policy_at=proposal.source_exit_policy_at,
                    source_allocation_at=proposal.approved_allocation_at,
                )
            )
            if proposal.side == "sell":
                open_sell_reservations[proposal.symbol] = (
                    float(open_sell_reservations.get(proposal.symbol, 0.0)) + float(proposal.qty)
                )

        return attempts

    def _submit_order_proposal(self, proposal: OrderProposal) -> Any:
        order_request = self._proposal_to_order_request(proposal)
        return self.alpaca_client.submit_order(**order_request)

    def _proposal_to_order_request(self, proposal: OrderProposal) -> dict[str, Any]:
        order_request: dict[str, Any] = {
            "symbol": proposal.symbol,
            "qty": proposal.qty,
            "side": proposal.side,
            "type": proposal.order_type,
            "time_in_force": proposal.time_in_force,
            "client_order_id": str(
                proposal.diagnostics.get("client_order_id", f"cycle-{proposal.cycle_id}-{proposal.symbol}")
            ),
            "trade_hour_type": str(
                proposal.diagnostics.get("trade_hour_type", self._get_trade_hour_type())
            ),
        }
        if proposal.limit_price is not None:
            order_request["limit_price"] = proposal.limit_price
        if proposal.extended_hours:
            order_request["extended_hours"] = True
        return order_request

    @staticmethod
    def _serialize_submitted_order(order: Any, proposal: OrderProposal) -> dict[str, Any]:
        return {
            "id": getattr(order, "id", None),
            "symbol": getattr(order, "symbol", proposal.symbol),
            "side": getattr(order, "side", proposal.side),
            "qty": getattr(order, "qty", proposal.qty),
            "trade_hour_type": proposal.diagnostics.get("trade_hour_type"),
            "source": proposal.source_layer,
            "approved_allocation_at": proposal.approved_allocation_at.isoformat()
            if proposal.approved_allocation_at
            else None,
            "source_signal_bundle_at": proposal.source_signal_bundle_at.isoformat()
            if proposal.source_signal_bundle_at
            else None,
            "source_scenario_bundle_at": proposal.source_scenario_bundle_at.isoformat()
            if proposal.source_scenario_bundle_at
            else None,
            "source_decision_policy_at": proposal.source_decision_policy_at.isoformat()
            if proposal.source_decision_policy_at
            else None,
            "source_exit_policy_at": proposal.source_exit_policy_at.isoformat()
            if proposal.source_exit_policy_at
            else None,
        }

    @staticmethod
    def _build_execution_result(
        cycle_id: str,
        as_of: datetime,
        attempts: list[ExecutionAttempt],
        order_proposals: list[OrderProposal],
        risk_adjusted_allocation: RiskAdjustedAllocation,
    ) -> ExecutionResult:
        submitted_count = sum(1 for attempt in attempts if attempt.status == "submitted")
        blocked_count = sum(1 for attempt in attempts if attempt.status in {"blocked", "failed"})
        return ExecutionResult(
            cycle_id=cycle_id,
            as_of=as_of,
            attempts=attempts,
            summary=f"attempts={len(attempts)} submitted={submitted_count} blocked={blocked_count}",
            submitted_count=submitted_count,
            blocked_count=blocked_count,
            acted=submitted_count > 0,
            sell_first_order=[
                attempt.symbol
                for attempt in attempts
                if attempt.stage == "primary_execution" and attempt.status == "submitted"
            ],
            diagnostics={
                "proposal_count": len(order_proposals),
                "stage_counts": {
                    "stale_order_lifecycle": sum(1 for attempt in attempts if attempt.stage == "stale_order_lifecycle"),
                    "primary_execution": sum(1 for attempt in attempts if attempt.stage == "primary_execution"),
                },
            },
            source_signal_bundle_at=risk_adjusted_allocation.source_signal_bundle_at,
            source_scenario_bundle_at=risk_adjusted_allocation.source_scenario_bundle_at,
            source_decision_policy_at=risk_adjusted_allocation.source_decision_policy_at,
            source_exit_policy_at=risk_adjusted_allocation.source_exit_policy_at,
            source_allocation_at=risk_adjusted_allocation.source_allocation_at,
            lineage={
                "source_allocation_at": risk_adjusted_allocation.source_allocation_at.isoformat()
                if risk_adjusted_allocation.source_allocation_at
                else None,
            },
        )

    @staticmethod
    def _execution_result_to_submitted_orders(execution_result: ExecutionResult) -> list[dict[str, Any]]:
        submitted_orders: list[dict[str, Any]] = []
        for attempt in execution_result.attempts:
            if attempt.status != "submitted":
                continue
            payload = dict(attempt.response_payload)
            if not payload:
                payload = {
                    "id": attempt.broker_order_id,
                    "symbol": attempt.symbol,
                    "side": attempt.side,
                    "qty": attempt.qty,
                    "source": attempt.stage,
                }
            submitted_orders.append(payload)
        return submitted_orders

    @staticmethod
    def _execution_result_to_blocked_orders(execution_result: ExecutionResult) -> list[dict[str, Any]]:
        return [
            {
                "symbol": attempt.symbol,
                "reason": attempt.reason,
                "stage": attempt.stage,
            }
            for attempt in execution_result.attempts
            if attempt.status in {"blocked", "failed"}
        ]

    @staticmethod
    def _execution_result_to_lifecycle_actions(execution_result: ExecutionResult) -> list[dict[str, Any]]:
        actions: list[dict[str, Any]] = []
        for attempt in execution_result.attempts:
            if attempt.stage != "stale_order_lifecycle":
                continue
            actions.append(
                {
                    "order_id": attempt.lineage.get("replaces_order_id") or attempt.broker_order_id,
                    "symbol": attempt.symbol,
                    "side": attempt.side,
                    "status": attempt.status,
                    "action": attempt.diagnostics.get("lifecycle_action", attempt.status),
                    "reason": attempt.reason,
                    "replacement_order_id": attempt.broker_order_id if attempt.status == "submitted" else None,
                    "replacement_limit_price": attempt.limit_price,
                    "age_seconds": attempt.diagnostics.get("age_seconds"),
                }
            )
        return actions

    @staticmethod
    def _portfolio_actions_to_dict(portfolio_actions: list[PortfolioActionAnalysis]) -> dict[str, dict[str, Any]]:
        return {
            action.symbol: {
                "action": action.action,
                "reason": action.reason,
                "current_weight": action.current_weight,
                "base_target_weight": action.base_target_weight,
                "adjusted_target_weight": action.adjusted_target_weight,
                "current_score": action.current_score,
                "previous_score": action.previous_score,
                "score_delta": action.score_delta,
                "holding_minutes": action.holding_minutes,
                "minimum_hold_satisfied": action.minimum_hold_satisfied,
                "diagnostics": BotCycleService._serialize_for_snapshot(action.diagnostics),
            }
            for action in portfolio_actions
        }

    @staticmethod
    def _reconciliation_result_to_dict(reconciliation_result: ReconciliationResult) -> dict[str, Any]:
        return cast("dict[str, Any]", BotCycleService._serialize_for_snapshot(reconciliation_result))

    @staticmethod
    def _serialize_for_snapshot(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if is_dataclass(value) and not isinstance(value, type):
            return {
                key: BotCycleService._serialize_for_snapshot(raw_value)
                for key, raw_value in asdict(value).items()
            }
        if isinstance(value, dict):
            return {
                key: BotCycleService._serialize_for_snapshot(raw_value)
                for key, raw_value in value.items()
            }
        if isinstance(value, list):
            return [BotCycleService._serialize_for_snapshot(item) for item in value]
        return value

    @staticmethod
    def _build_symbol_lineage(
        *,
        signals: dict[str, dict[str, Any]],
        scenario_bundle: Any,
        decision_policy_output: DecisionPolicyOutput,
        exit_policy_output: ExitPolicyOutput,
        allocation_proposal: AllocationProposal,
        risk_adjusted_allocation: RiskAdjustedAllocation,
        portfolio_actions: list[PortfolioActionAnalysis],
        order_proposals: list[OrderProposal],
        execution_result: ExecutionResult,
        reconciliation_result: ReconciliationResult,
        monitoring_decision: MonitoringDecision,
    ) -> dict[str, dict[str, Any]]:
        lineage: dict[str, dict[str, Any]] = {}
        decision_lookup = {decision.symbol: decision for decision in decision_policy_output.decisions}
        exit_lookup = {directive.symbol: directive for directive in exit_policy_output.directives}
        allocation_lookup = {line.symbol: line for line in allocation_proposal.lines}
        risk_lookup = {detail.symbol: detail for detail in risk_adjusted_allocation.symbol_details}
        portfolio_lookup = {action.symbol: action for action in portfolio_actions}
        order_lookup = {proposal.symbol: proposal for proposal in order_proposals}
        execution_lookup: dict[str, list[ExecutionAttempt]] = {}
        for attempt in execution_result.attempts:
            execution_lookup.setdefault(attempt.symbol, []).append(attempt)
        reconciliation_positions: dict[str, list[dict[str, Any]]] = {}
        for delta in reconciliation_result.position_deltas:
            reconciliation_positions.setdefault(str(delta.get("symbol", "")), []).append(delta)
        reconciliation_fills: dict[str, list[dict[str, Any]]] = {}
        for fill in reconciliation_result.fill_events:
            reconciliation_fills.setdefault(str(fill.get("symbol", "")), []).append(fill)
        anomaly_lookup: dict[str, list[Any]] = {}
        for anomaly in reconciliation_result.anomalies:
            if anomaly.symbol is None:
                continue
            anomaly_lookup.setdefault(anomaly.symbol, []).append(anomaly)
        symbols = (
            set(signals)
            | set(decision_lookup)
            | set(exit_lookup)
            | set(allocation_lookup)
            | set(risk_lookup)
            | set(portfolio_lookup)
            | set(order_lookup)
            | set(execution_lookup)
            | set(reconciliation_positions)
            | set(reconciliation_fills)
            | set(anomaly_lookup)
        )
        for symbol in sorted(symbols):
            lineage[symbol] = {
                "signal": signals.get(symbol),
                "scenario": {
                    "regime_label": getattr(scenario_bundle, "regime_label", None),
                    "scenario_names": [scenario.name for scenario in getattr(scenario_bundle, "scenarios", [])],
                },
                "decision_policy": BotCycleService._serialize_for_snapshot(decision_lookup.get(symbol)),
                "exit_policy": BotCycleService._serialize_for_snapshot(exit_lookup.get(symbol)),
                "allocation_proposal": BotCycleService._serialize_for_snapshot(allocation_lookup.get(symbol)),
                "portfolio_action": BotCycleService._serialize_for_snapshot(portfolio_lookup.get(symbol)),
                "risk_adjustment": BotCycleService._serialize_for_snapshot(risk_lookup.get(symbol)),
                "order_proposal": BotCycleService._serialize_for_snapshot(order_lookup.get(symbol)),
                "execution": BotCycleService._serialize_for_snapshot(execution_lookup.get(symbol, [])),
                "reconciliation": {
                    "position_deltas": BotCycleService._serialize_for_snapshot(reconciliation_positions.get(symbol, [])),
                    "fill_events": BotCycleService._serialize_for_snapshot(reconciliation_fills.get(symbol, [])),
                    "anomalies": BotCycleService._serialize_for_snapshot(anomaly_lookup.get(symbol, [])),
                },
                "monitoring": {
                    "blocked": symbol in monitoring_decision.blocked_symbols,
                    "alerts": [
                        BotCycleService._serialize_for_snapshot(alert)
                        for alert in monitoring_decision.alerts
                        if alert.symbol == symbol
                    ],
                },
            }
        return lineage

    @staticmethod
    def _build_monitoring_decision(
        as_of: datetime,
        execution_result: ExecutionResult,
        reconciliation_result: ReconciliationResult,
        decision_summaries: dict[str, dict[str, Any]],
        no_delta_reason: str,
        scenario_bundle: Any,
        risk_adjusted_allocation: RiskAdjustedAllocation,
        decision_policy_output: DecisionPolicyOutput,
        exit_policy_output: ExitPolicyOutput,
        portfolio_actions: list[PortfolioActionAnalysis],
    ) -> MonitoringDecision:
        alerts: list[MonitoringAlert] = []
        blocked_symbols: list[str] = []
        execution_gate_reasons: list[str] = []
        for attempt in execution_result.attempts:
            if attempt.status in {"blocked", "failed"}:
                blocked_symbols.append(attempt.symbol)
                execution_gate_reasons.append(attempt.reason)
                alerts.append(
                    MonitoringAlert(
                        severity="warning",
                        message=attempt.reason,
                        symbol=attempt.symbol,
                        code="order_blocked",
                    )
                )
            elif attempt.stage == "stale_order_lifecycle":
                alerts.append(
                    MonitoringAlert(
                        severity="info",
                        message=attempt.reason,
                        symbol=attempt.symbol,
                        code="stale_order_lifecycle",
                    )
                )
        for anomaly in reconciliation_result.anomalies:
            alerts.append(
                MonitoringAlert(
                    severity=cast("Any", anomaly.severity),
                    message=anomaly.message,
                    symbol=anomaly.symbol,
                    code=anomaly.code,
                )
            )

        risk_blocked_details = [
            detail
            for detail in getattr(risk_adjusted_allocation, "symbol_details", [])
            if detail.status == "blocked"
        ]
        status = "healthy"
        if (
            execution_result.blocked_count
            or reconciliation_result.anomalies
            or risk_blocked_details
            or any(summary.get("decision_reason") == "quality_issues" for summary in decision_summaries.values())
        ):
            status = "degraded"
        inaction_reasons: list[str] = []
        if not execution_result.acted:
            inaction_reasons.append(no_delta_reason)
        if execution_result.blocked_count:
            inaction_reasons.extend(sorted(set(execution_gate_reasons)))
        for detail in risk_blocked_details:
            blocked_reason = detail.reasons[0].code if detail.reasons else "risk_blocked"
            inaction_reasons.append(f"{detail.symbol}:{blocked_reason}")
        for symbol_summary in decision_summaries.values():
            if symbol_summary.get("decision_reason") == "quality_issues":
                inaction_reasons.append(f"{symbol_summary.get('symbol', 'unknown')}:quality_issues")
            if symbol_summary.get("decision_reason") == "missing_price_fallback_failed":
                inaction_reasons.append(f"{symbol_summary.get('symbol', 'unknown')}:missing_price")
        blocked_symbols.extend(
            detail.symbol for detail in risk_blocked_details
        )
        decision_summary = (
            f"submitted={execution_result.submitted_count} blocked={execution_result.blocked_count} "
            f"reconciliation_anomalies={len(reconciliation_result.anomalies)}"
        )
        next_action = "continue" if execution_result.acted or no_delta_reason != "HAS_DELTAS" else "review"
        return MonitoringDecision(
            as_of=as_of,
            status=cast("Any", status),
            summary=decision_summary,
            next_action=next_action,
            alerts=alerts,
            acted=execution_result.acted,
            blocked_symbols=sorted(set(symbol for symbol in blocked_symbols if symbol)),
            inaction_reasons=sorted(set(reason for reason in inaction_reasons if reason)),
            execution_gate_reasons=sorted(set(reason for reason in execution_gate_reasons if reason)),
            diagnostics={
                "submitted_order_count": execution_result.submitted_count,
                "blocked_order_count": execution_result.blocked_count,
                "no_delta_reason": no_delta_reason,
                "scenario_context": {
                    "regime_label": getattr(scenario_bundle, "regime_label", None),
                    "regime_confidence": getattr(scenario_bundle, "regime_confidence", None),
                    "anomaly_flags": list(getattr(scenario_bundle, "anomaly_flags", [])),
                },
                "risk_blocked_symbols": [
                    detail.symbol for detail in getattr(risk_adjusted_allocation, "symbol_details", []) if detail.status == "blocked"
                ],
                "decision_policy_skips": [
                    decision.symbol for decision in decision_policy_output.decisions if decision.policy_action == "skip"
                ],
                "exit_policy_actions": {
                    directive.symbol: directive.action for directive in exit_policy_output.directives
                },
                "portfolio_actions": {
                    action.symbol: action.action for action in portfolio_actions
                },
                "reconciliation_anomalies": [
                    anomaly.code for anomaly in reconciliation_result.anomalies
                ],
                "symbol_lineage": {
                    symbol: {
                        "decision_status": symbol_summary.get("decision_status"),
                        "decision_reason": symbol_summary.get("decision_reason"),
                        "target_weight": symbol_summary.get("target_weight"),
                        "blocked_reason": symbol_summary.get("blocked_reason"),
                    }
                    for symbol, symbol_summary in decision_summaries.items()
                },
            },
            source_signal_bundle_at=decision_policy_output.source_signal_bundle_at,
            source_scenario_bundle_at=scenario_bundle.as_of,
            source_decision_policy_at=decision_policy_output.as_of,
            source_exit_policy_at=exit_policy_output.as_of,
            source_allocation_at=risk_adjusted_allocation.source_allocation_at,
            lineage={
                "source_signal_bundle_at": decision_policy_output.source_signal_bundle_at.isoformat()
                if decision_policy_output.source_signal_bundle_at
                else None,
                "source_scenario_bundle_at": scenario_bundle.as_of.isoformat(),
                "source_decision_policy_at": decision_policy_output.as_of.isoformat(),
                "source_exit_policy_at": exit_policy_output.as_of.isoformat(),
                "source_allocation_at": risk_adjusted_allocation.source_allocation_at.isoformat()
                if risk_adjusted_allocation.source_allocation_at
                else None,
                "source_reconciliation_at": reconciliation_result.as_of.isoformat(),
            },
        )

    @staticmethod
    def _build_cycle_report(
        cycle_id: str,
        as_of: datetime,
        symbols: list[str],
        execution_result: ExecutionResult,
        monitoring_decision: MonitoringDecision,
        decision_policy_output: DecisionPolicyOutput,
        scenario_bundle: Any,
        exit_policy_output: ExitPolicyOutput,
        risk_adjusted_allocation: RiskAdjustedAllocation,
    ) -> CycleReport:
        return CycleReport(
            cycle_id=cycle_id,
            as_of=as_of,
            status=monitoring_decision.status,
            symbols=list(symbols),
            summary=monitoring_decision.summary,
            submitted_order_count=execution_result.submitted_count,
            blocked_order_count=execution_result.blocked_count,
            acted=execution_result.acted,
            blocked_symbols=list(monitoring_decision.blocked_symbols),
            inaction_reasons=list(monitoring_decision.inaction_reasons),
            next_action=monitoring_decision.next_action,
            diagnostics={
                "execution_summary": execution_result.summary,
                "scenario_regime": getattr(scenario_bundle, "regime_label", None),
                "policy_decision_count": len(decision_policy_output.decisions),
                "exit_directive_count": len(exit_policy_output.directives),
                "risk_blocked_symbols": [
                    detail.symbol
                    for detail in risk_adjusted_allocation.symbol_details
                    if detail.status == "blocked"
                ],
            },
            source_signal_bundle_at=decision_policy_output.source_signal_bundle_at,
            source_scenario_bundle_at=getattr(scenario_bundle, "as_of", None),
            source_decision_policy_at=decision_policy_output.as_of,
            source_exit_policy_at=exit_policy_output.as_of,
            source_allocation_at=risk_adjusted_allocation.source_allocation_at,
            lineage=dict(monitoring_decision.lineage),
        )

    def _build_portfolio_state(self, account: Any, positions: list[Any], started_at: datetime) -> dict[str, Any]:
        return {
            "equity": float(getattr(account, "equity", 0.0) or 0.0),
            "daily_realized_pnl": self._current_daily_realized_pnl(started_at),
            "open_positions": len([position for position in positions if float(getattr(position, "qty", 0.0) or 0.0) != 0.0]),
        }
   
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

    def _reconcile_portfolio(
        self,
        cycle_id: str,
        started_at: datetime,
        execution_result: ExecutionResult,
    ) -> ReconciliationResult:
        refreshed_account = self.alpaca_client.get_account()
        refreshed_positions = self.alpaca_client.get_positions()
        refreshed_orders = self.alpaca_client.get_orders(status="all", limit=200)

        return self.portfolio_engine.sync_account_state(
            account={
                "cash": getattr(refreshed_account, "cash", refreshed_account.buying_power),
                "buying_power": refreshed_account.buying_power,
                "equity": refreshed_account.equity,
            },
            positions=[asdict(p) for p in refreshed_positions],
            orders=[asdict(o) for o in refreshed_orders],
            cycle_id=cycle_id,
            as_of=started_at,
            source_execution_result=execution_result,
        )
   
    #High level reasoning for why no deltas are generated
    def _derive_no_delta_reason(
        self,
        symbols: list[str],
        signals: dict[str, dict[str, Any]],
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

    def _pull_features(self, symbols: list[str]) -> tuple[dict[str, dict[str, float]], dict[str, dict[str, Any]]]:
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
        cycle_id: str,
        as_of: datetime,
        current_positions: dict[str, float],
        latest_prices: dict[str, float],
        base_target_weights: dict[str, float],
        features: dict[str, dict[str, float]],
        equity: float,
        previous_payload: dict[str, Any],
        exit_policy_actions: dict[str, dict[str, Any]] | None = None,
        signal_bundle: SignalBundle | None = None,
        scenario_bundle: Any | None = None,
        decision_policy_output: DecisionPolicyOutput | None = None,
        exit_policy_output: ExitPolicyOutput | None = None,
    ) -> tuple[list[PortfolioActionAnalysis], dict[str, float]]:
        previous_features: dict[str, dict[str, float]] = previous_payload.get("features", {})
        previous_exit_state: dict[str, dict[str, Any]] = previous_payload.get("exit_policy_state", {})
        minimum_hold_minutes = float(getattr(self.exit_policy, "min_holding_minutes", 0.0) or 0.0)
        position_actions = exit_policy_actions or {}

        actions: list[PortfolioActionAnalysis] = []
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
            holding_minutes = float(position_actions.get(symbol, {}).get("holding_minutes", 0.0) or 0.0)
            if holding_minutes <= 0.0:
                holding_minutes = self._holding_minutes_from_state(previous_exit_state.get(symbol, {}))
            minimum_hold_satisfied = holding_minutes >= minimum_hold_minutes
            score_delta = current_score - previous_score

            action = "hold"
            reason = "position_healthy"
            adjusted_target = base_target

            if current_score < -0.05:
                action = "close"
                reason = "hard_negative_score_exit"
                adjusted_target = 0.0
            elif not minimum_hold_satisfied and base_target <= 0.0:
                action = "hold"
                reason = "minimum_hold_enforced"
                adjusted_target = current_weight
            elif minimum_hold_satisfied and (
                (current_score <= -0.02 and previous_score <= -0.01)
                or (previous_score >= 0.03 and current_score <= -0.02 and score_delta <= -0.04)
            ):
                action = "close"
                reason = "score_deterioration_vs_previous_snapshot"
                adjusted_target = 0.0
            elif minimum_hold_satisfied and current_score < 0.0 and current_weight > 0:
                action = "reduce"
                reason = "weak_signal_keep_half_exposure"
                adjusted_target = min(base_target, current_weight * 0.5)

            adjusted[symbol] = adjusted_target
            scenario_bundle_as_of = getattr(scenario_bundle, "as_of", None)
            actions.append(
                PortfolioActionAnalysis(
                    cycle_id=cycle_id,
                    as_of=as_of,
                    symbol=symbol,
                    action=cast("Any", action),
                    reason=reason,
                    current_weight=current_weight,
                    base_target_weight=base_target,
                    adjusted_target_weight=adjusted_target,
                    current_score=current_score,
                    previous_score=previous_score,
                    score_delta=score_delta,
                    holding_minutes=holding_minutes,
                    minimum_hold_satisfied=minimum_hold_satisfied,
                    source_signal_bundle_at=signal_bundle.as_of if signal_bundle else None,
                    source_scenario_bundle_at=getattr(scenario_bundle, "as_of", None),
                    source_decision_policy_at=decision_policy_output.as_of if decision_policy_output else None,
                    source_exit_policy_at=exit_policy_output.as_of if exit_policy_output else None,
                    diagnostics={
                        "current_qty": qty,
                        "current_price": price,
                        "exit_policy_action": position_actions.get(symbol, {}).get("action"),
                    },
                    lineage={
                        "signal_bundle_as_of": signal_bundle.as_of.isoformat() if signal_bundle else None,
                        "scenario_bundle_as_of": scenario_bundle_as_of.isoformat()
                        if scenario_bundle_as_of is not None
                        else None,
                    },
                )
            )
        return actions, adjusted

    @staticmethod
    def _holding_minutes_from_state(previous_row: dict[str, Any]) -> float:
        first_seen_at_raw = previous_row.get("first_seen_at")
        if not isinstance(first_seen_at_raw, str):
            return 0.0
        try:
            first_seen_at = datetime.fromisoformat(first_seen_at_raw)
        except ValueError:
            return 0.0
        if first_seen_at.tzinfo is None:
            first_seen_at = first_seen_at.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return max(0.0, (now - first_seen_at).total_seconds() / 60.0)

    @staticmethod
    def _signal_strength(signal: dict[str, Any]) -> float:
        if "score" in signal:
            return float(signal.get("score", 0.0))
        direction = str(signal.get("direction", "flat")).lower()
        strength = float(signal.get("strength", 0.0))
        if direction == "long":
            return strength
        if direction == "short":
            return -strength
        return 0.0
