from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast, overload

from agent_service.interfaces import ExitPolicyDirective, ExitPolicyOutput, SignalBundle, SignalIntent
from agent_service.interfaces.contracts import Action, Direction, ExitAction


@dataclass(slots=True)
class ExitPolicy:
    max_adverse_excursion_pct: float = 0.03
    stop_loss_pct: float = 0.04
    take_profit_reduce_pct: float = 0.03
    take_profit_exit_pct: float = 0.06
    max_holding_minutes: int = 180
    min_holding_minutes: int = 15
    deterioration_signal_floor: float = 0.002
    deterioration_drop_pct: float = 0.5
    add_signal_threshold: float = 0.03

    def evaluate_positions_typed(
        self,
        positions: list[Any],
        latest_prices: dict[str, float],
        signal_bundle: SignalBundle,
        previous_exit_state: dict[str, dict[str, Any]] | None = None,
        previous_signal_bundle: SignalBundle | None = None,
        now_utc: datetime | None = None,
    ) -> ExitPolicyOutput:
        """Evaluate open positions and emit typed directives plus an adjusted signal bundle."""

        now = now_utc or datetime.now(timezone.utc)
        previous_state = previous_exit_state or {}
        current_signals = {intent.symbol: intent for intent in signal_bundle.intents}
        previous_signals = {
            intent.symbol: intent for intent in (previous_signal_bundle.intents if previous_signal_bundle else [])
        }

        directives: list[ExitPolicyDirective] = []
        state: dict[str, dict[str, Any]] = {}
        adjusted_intents = {intent.symbol: intent for intent in signal_bundle.intents}

        for position in positions:
            symbol = str(getattr(position, "symbol", "")).upper()
            qty = float(getattr(position, "qty", 0.0) or 0.0)
            if not symbol or qty <= 0.0:
                continue

            last_price = float(latest_prices.get(symbol, 0.0) or 0.0)
            previous_row = previous_state.get(symbol, {}) or {}
            requested_intent = current_signals.get(symbol)
            previous_intent = previous_signals.get(symbol)

            if last_price <= 0.0:
                directives.append(
                    ExitPolicyDirective(
                        symbol=symbol,
                        action="HOLD",
                        trigger="missing_price_fallback_failed",
                        trigger_type="guardrail",
                        current_qty=qty,
                        requested_intent=requested_intent,
                        adjusted_intent=requested_intent,
                        rationale="No usable last price was available, so the exit policy failed closed.",
                        diagnostics={
                            "last_price": 0.0,
                            "previous_signal_strength": self._extract_signal_strength(previous_intent),
                            "current_signal_strength": self._extract_signal_strength(requested_intent),
                        },
                    )
                )
                continue

            first_seen_at = self._resolve_first_seen_at(position, previous_row, now)
            holding_minutes = max(0.0, (now - first_seen_at).total_seconds() / 60.0)
            minimum_hold_satisfied = holding_minutes >= self.min_holding_minutes

            avg_entry_price = float(getattr(position, "avg_entry_price", 0.0) or 0.0)
            if avg_entry_price <= 0.0:
                avg_entry_price = last_price
            pnl_pct = (last_price - avg_entry_price) / avg_entry_price if avg_entry_price > 0 else 0.0

            current_adverse_excursion = max(0.0, -pnl_pct)
            historical_mae = float(previous_row.get("max_adverse_excursion_pct", 0.0) or 0.0)
            max_adverse_excursion = max(current_adverse_excursion, historical_mae)

            previous_strength = self._extract_signal_strength(previous_intent)
            current_strength = self._extract_signal_strength(requested_intent)
            deterioration_trigger = self._is_signal_deteriorating(previous_strength, current_strength)

            action: ExitAction = "HOLD"
            trigger = "none"
            trigger_type = "none"
            target_weight_multiplier = 1.0
            force_target_weight: float | None = None
            rationale = "No exit or add condition exceeded policy thresholds."

            if max_adverse_excursion >= self.stop_loss_pct:
                action, trigger, trigger_type = "EXIT", "stop_loss", "risk"
                force_target_weight = 0.0
                rationale = "Hard stop takes precedence once adverse excursion breaches the stop-loss band."
            elif max_adverse_excursion >= self.max_adverse_excursion_pct:
                action, trigger, trigger_type = "REDUCE", "max_adverse_excursion", "risk"
                target_weight_multiplier = 0.5
                rationale = "The position is underwater enough to require deterministic risk reduction."
            elif pnl_pct >= self.take_profit_exit_pct and minimum_hold_satisfied:
                action, trigger, trigger_type = "EXIT", "take_profit_exit_band", "profit"
                force_target_weight = 0.0
                rationale = "Profit exceeded the full exit band after the minimum hold elapsed."
            elif pnl_pct >= self.take_profit_reduce_pct and minimum_hold_satisfied:
                action, trigger, trigger_type = "REDUCE", "take_profit_reduce_band", "profit"
                target_weight_multiplier = 0.5
                rationale = "Profit exceeded the scale-down band after the minimum hold elapsed."
            elif holding_minutes >= self.max_holding_minutes:
                action, trigger, trigger_type = "EXIT", "max_holding_time", "time"
                force_target_weight = 0.0
                rationale = "The position outlived the allowed holding window."
            elif deterioration_trigger and minimum_hold_satisfied:
                action, trigger, trigger_type = "REDUCE", "signal_deterioration", "signal"
                target_weight_multiplier = 0.5
                rationale = "The supporting signal weakened enough to justify reducing exposure."
            elif current_strength >= self.add_signal_threshold and minimum_hold_satisfied:
                action, trigger, trigger_type = "ADD", "strong_signal_continuation", "signal"
                target_weight_multiplier = 1.25
                rationale = "The signal remained strong after the minimum hold, so the policy allows a measured add."

            adjusted_intent = self._adjust_intent(
                symbol=symbol,
                requested_intent=requested_intent,
                action=action,
                target_weight_multiplier=target_weight_multiplier,
            )
            if adjusted_intent is not None:
                adjusted_intents[symbol] = adjusted_intent

            directives.append(
                ExitPolicyDirective(
                    symbol=symbol,
                    action=action,
                    trigger=trigger,
                    trigger_type=trigger_type,
                    current_qty=qty,
                    requested_intent=requested_intent,
                    adjusted_intent=adjusted_intent,
                    target_weight_multiplier=target_weight_multiplier,
                    force_target_weight=force_target_weight,
                    rationale=rationale,
                    diagnostics={
                        "qty": qty,
                        "last_price": last_price,
                        "avg_entry_price": avg_entry_price,
                        "unrealized_pnl_pct": pnl_pct,
                        "max_adverse_excursion_pct": max_adverse_excursion,
                        "holding_minutes": holding_minutes,
                        "minimum_hold_satisfied": minimum_hold_satisfied,
                        "previous_signal_strength": previous_strength,
                        "current_signal_strength": current_strength,
                    },
                )
            )
            state[symbol] = {
                "first_seen_at": first_seen_at.isoformat(),
                "max_adverse_excursion_pct": max_adverse_excursion,
                "last_evaluated_at": now.isoformat(),
            }

        adjusted_signal_bundle = SignalBundle(
            as_of=now,
            benchmark_symbol=signal_bundle.benchmark_symbol,
            intents=list(adjusted_intents.values()),
            feature_snapshot=signal_bundle.feature_snapshot,
            model_name=signal_bundle.model_name,
            notes=[*signal_bundle.notes, "exit_policy_adjusted"],
            lineage={
                **signal_bundle.lineage,
                "source_signal_bundle_at": signal_bundle.as_of,
                "source_scenario_bundle_at": signal_bundle.lineage.get("source_scenario_bundle_at"),
                "source_decision_policy_at": signal_bundle.lineage.get("source_decision_policy_at"),
            },
        )
        return ExitPolicyOutput(
            as_of=now,
            adjusted_signal_bundle=adjusted_signal_bundle,
            directives=directives,
            state=state,
            notes=["Exit policy evaluated live positions and adjusted intents before allocation."],
            diagnostics={
                "positions_evaluated": len(directives),
                "state_rows_written": len(state),
            },
            source_signal_bundle_at=signal_bundle.as_of,
            source_scenario_bundle_at=signal_bundle.lineage.get("source_scenario_bundle_at"),
            source_decision_policy_at=signal_bundle.lineage.get("source_decision_policy_at"),
            lineage={
                "signal_bundle_as_of": signal_bundle.as_of,
                "previous_signal_bundle_as_of": previous_signal_bundle.as_of if previous_signal_bundle else None,
            },
        )

    @overload
    def evaluate_positions(
        self,
        positions: list[Any],
        latest_prices: dict[str, float],
        signals: None = None,
        previous_payload: dict[str, Any] | None = None,
        *,
        signal_bundle: SignalBundle,
        previous_signal_bundle: SignalBundle | None = None,
        scenario_bundle: Any | None = None,
        now_utc: datetime | None = None,
    ) -> ExitPolicyOutput: ...

    @overload
    def evaluate_positions(
        self,
        positions: list[Any],
        latest_prices: dict[str, float],
        signals: dict[str, dict[str, float | str]],
        previous_payload: dict[str, Any],
        *,
        signal_bundle: None = None,
        previous_signal_bundle: SignalBundle | None = None,
        scenario_bundle: Any | None = None,
        now_utc: datetime | None = None,
    ) -> dict[str, Any]: ...

    def evaluate_positions(
        self,
        positions: list[Any],
        latest_prices: dict[str, float],
        signals: dict[str, dict[str, float | str]] | None = None,
        previous_payload: dict[str, Any] | None = None,
        now_utc: datetime | None = None,
        *,
        signal_bundle: SignalBundle | None = None,
        previous_signal_bundle: SignalBundle | None = None,
        scenario_bundle: Any | None = None,
    ) -> ExitPolicyOutput | dict[str, Any]:
        """Typed-first exit-policy API with a legacy dict wrapper for older callers."""

        now = now_utc or datetime.now(timezone.utc)
        del scenario_bundle
        previous_payload = previous_payload or {}
        resolved_signal_bundle = signal_bundle or self._legacy_signals_to_bundle(signals=signals or {}, as_of=now)
        resolved_previous_signal_bundle = previous_signal_bundle or self._legacy_signals_to_bundle(
            signals=previous_payload.get("signals", {}) or {},
            as_of=now,
        )
        output = self.evaluate_positions_typed(
            positions=positions,
            latest_prices=latest_prices,
            signal_bundle=resolved_signal_bundle,
            previous_exit_state=previous_payload.get("exit_policy_state", {}) or {},
            previous_signal_bundle=resolved_previous_signal_bundle,
            now_utc=now,
        )
        if signal_bundle is not None:
            return output
        return {
            "actions": self._output_to_legacy_actions(output),
            "state": output.state,
            "artifact": output,
        }

    @staticmethod
    def _extract_signal_strength(signal: SignalIntent | None) -> float:
        if signal is None:
            return 0.0
        if "strength" in signal.diagnostics:
            raw_strength = float(signal.diagnostics.get("strength", 0.0) or 0.0)
            if signal.direction == "short":
                return -abs(raw_strength)
            if signal.direction == "flat":
                return 0.0
            return abs(raw_strength)
        return float(signal.score or 0.0)

    def _is_signal_deteriorating(self, previous_strength: float, current_strength: float) -> bool:
        if previous_strength <= 0.0:
            return False
        if current_strength <= self.deterioration_signal_floor:
            return True
        drop = previous_strength - current_strength
        if drop <= 0:
            return False
        return (drop / previous_strength) >= self.deterioration_drop_pct

    @staticmethod
    def _resolve_first_seen_at(position: Any, previous_row: dict[str, Any], now: datetime) -> datetime:
        previous_first_seen_at = previous_row.get("first_seen_at")
        if isinstance(previous_first_seen_at, str):
            try:
                return datetime.fromisoformat(previous_first_seen_at)
            except ValueError:
                pass

        for field_name in ("opened_at", "entry_time", "created_at"):
            raw = getattr(position, field_name, None)
            if not raw:
                continue
            if isinstance(raw, datetime):
                return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
            if isinstance(raw, str):
                try:
                    parsed = datetime.fromisoformat(raw)
                    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return now

    @staticmethod
    def _adjust_intent(
        symbol: str,
        requested_intent: SignalIntent | None,
        action: str,
        target_weight_multiplier: float,
    ) -> SignalIntent | None:
        if requested_intent is None:
            if action == "EXIT":
                return SignalIntent(
                    symbol=symbol,
                    direction="flat",
                    action="hold",
                    score=0.0,
                    confidence=1.0,
                    rationale="Exit policy created a flat intent for an existing position.",
                    diagnostics={"strength": 0.0},
                )
            return None

        diagnostics = dict(requested_intent.diagnostics)
        current_strength = float(diagnostics.get("strength", abs(requested_intent.score)) or 0.0)
        if action == "EXIT":
            diagnostics["strength"] = 0.0
            return SignalIntent(
                symbol=requested_intent.symbol,
                direction="flat",
                action="sell",
                score=0.0,
                confidence=max(float(requested_intent.confidence), 1.0),
                expected_return=0.0,
                normalized_score=0.0,
                rank=requested_intent.rank,
                rationale=requested_intent.rationale,
                diagnostics=diagnostics,
            )
        if action == "REDUCE":
            diagnostics["strength"] = current_strength * target_weight_multiplier
            return SignalIntent(
                symbol=requested_intent.symbol,
                direction=requested_intent.direction,
                action=requested_intent.action,
                score=float(requested_intent.score) * target_weight_multiplier,
                confidence=float(requested_intent.confidence),
                expected_return=float(requested_intent.expected_return),
                normalized_score=float(requested_intent.normalized_score),
                rank=requested_intent.rank,
                rationale=requested_intent.rationale,
                diagnostics=diagnostics,
            )
        if action == "ADD":
            diagnostics["strength"] = current_strength * target_weight_multiplier
            return SignalIntent(
                symbol=requested_intent.symbol,
                direction=requested_intent.direction,
                action=requested_intent.action,
                score=float(requested_intent.score) * target_weight_multiplier,
                confidence=float(requested_intent.confidence),
                expected_return=float(requested_intent.expected_return),
                normalized_score=float(requested_intent.normalized_score),
                rank=requested_intent.rank,
                rationale=requested_intent.rationale,
                diagnostics=diagnostics,
            )
        return requested_intent

    @staticmethod
    def _legacy_signals_to_bundle(signals: dict[str, dict[str, float | str]], as_of: datetime) -> SignalBundle:
        intents: list[SignalIntent] = []
        for symbol, signal in signals.items():
            direction_str = str(signal.get("direction", "flat")).lower()
            strength = float(signal.get("strength", 0.0) or 0.0)
            if "strength" not in signal:
                strength = abs(float(signal.get("score", 0.0) or 0.0))
            score = float(signal.get("score", 0.0) or 0.0)
            if "score" not in signal and direction_str == "long":
                score = strength
            elif "score" not in signal and direction_str == "short":
                score = -strength
            direction: Direction = cast(
                Direction,
                direction_str if direction_str in {"long", "short", "flat"} else "flat",
            )
            action: Action = cast(Action, {"long": "buy", "short": "sell", "flat": "hold"}.get(direction_str, "hold"))
            intents.append(
                SignalIntent(
                    symbol=str(symbol).upper(),
                    direction=direction,
                    action=action,
                    score=score,
                    confidence=float(signal.get("confidence", 0.0) or 0.0),
                    normalized_score=float(signal.get("normalized_score", signal.get("z_score", 0.0)) or 0.0),
                    rank=int(signal.get("rank", 0) or 0),
                    rationale=str(signal.get("rationale", "")),
                    diagnostics={
                        "strength": strength,
                        "expected_horizon": str(signal.get("expected_horizon", "30m")),
                    },
                )
            )
        return SignalBundle(as_of=as_of, benchmark_symbol="SPY", intents=intents, model_name="legacy_signal_dict")

    @staticmethod
    def _output_to_legacy_actions(output: ExitPolicyOutput) -> dict[str, dict[str, Any]]:
        return {
            directive.symbol: {
                "symbol": directive.symbol,
                "action": directive.action,
                "trigger": directive.trigger,
                "trigger_type": directive.trigger_type,
                "qty": directive.current_qty,
                "last_price": float(directive.diagnostics.get("last_price", 0.0) or 0.0),
                "avg_entry_price": float(directive.diagnostics.get("avg_entry_price", 0.0) or 0.0),
                "unrealized_pnl_pct": float(directive.diagnostics.get("unrealized_pnl_pct", 0.0) or 0.0),
                "max_adverse_excursion_pct": float(
                    directive.diagnostics.get("max_adverse_excursion_pct", 0.0) or 0.0
                ),
                "holding_minutes": float(directive.diagnostics.get("holding_minutes", 0.0) or 0.0),
                "previous_signal_strength": float(
                    directive.diagnostics.get("previous_signal_strength", 0.0) or 0.0
                ),
                "current_signal_strength": float(directive.diagnostics.get("current_signal_strength", 0.0) or 0.0),
            }
            for directive in output.directives
        }
