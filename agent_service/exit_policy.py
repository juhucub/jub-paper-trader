from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


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

    def evaluate_positions(
        self,
        positions: list[Any],
        latest_prices: dict[str, float],
        signals: dict[str, dict[str, float | str]],
        previous_payload: dict[str, Any],
        now_utc: datetime | None = None,
    ) -> dict[str, Any]:
        now = now_utc or datetime.now(timezone.utc)
        previous_state = previous_payload.get("exit_policy_state", {}) or {}
        previous_signals = previous_payload.get("signals", {}) or {}

        actions: dict[str, dict[str, Any]] = {}
        updated_state: dict[str, dict[str, Any]] = {}

        for position in positions:
            symbol = str(getattr(position, "symbol", "")).upper()
            qty = float(getattr(position, "qty", 0.0) or 0.0)
            if not symbol or qty <= 0.0:
                continue

            last_price = float(latest_prices.get(symbol, 0.0) or 0.0)
            if last_price <= 0.0:
                actions[symbol] = {
                    "action": "HOLD",
                    "trigger": "missing_price_fallback_failed",
                    "trigger_type": "guardrail",
                }
                continue

            previous_row = previous_state.get(symbol, {}) or {}
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

            previous_strength = self._extract_signal_strength(previous_signals.get(symbol, {}))
            current_strength = self._extract_signal_strength(signals.get(symbol, {}))
            deterioration_trigger = self._is_signal_deteriorating(previous_strength, current_strength)

            action = "HOLD"
            trigger = "none"
            trigger_type = "none"

            if max_adverse_excursion >= self.stop_loss_pct:
                action, trigger, trigger_type = "EXIT", "stop_loss", "risk"
            elif max_adverse_excursion >= self.max_adverse_excursion_pct:
                action, trigger, trigger_type = "REDUCE", "max_adverse_excursion", "risk"
            elif pnl_pct >= self.take_profit_exit_pct and minimum_hold_satisfied:
                action, trigger, trigger_type = "EXIT", "take_profit_exit_band", "profit"
            elif pnl_pct >= self.take_profit_reduce_pct and minimum_hold_satisfied:
                action, trigger, trigger_type = "REDUCE", "take_profit_reduce_band", "profit"
            elif holding_minutes >= self.max_holding_minutes:
                action, trigger, trigger_type = "EXIT", "max_holding_time", "time"
            elif deterioration_trigger and minimum_hold_satisfied:
                action, trigger, trigger_type = "REDUCE", "signal_deterioration", "signal"
            elif current_strength >= self.add_signal_threshold and minimum_hold_satisfied:
                action, trigger, trigger_type = "ADD", "strong_signal_continuation", "signal"

            actions[symbol] = {
                "symbol": symbol,
                "action": action,
                "trigger": trigger,
                "trigger_type": trigger_type,
                "qty": qty,
                "last_price": last_price,
                "avg_entry_price": avg_entry_price,
                "unrealized_pnl_pct": pnl_pct,
                "max_adverse_excursion_pct": max_adverse_excursion,
                "holding_minutes": holding_minutes,
                "previous_signal_strength": previous_strength,
                "current_signal_strength": current_strength,
            }

            updated_state[symbol] = {
                "first_seen_at": first_seen_at.isoformat(),
                "max_adverse_excursion_pct": max_adverse_excursion,
                "last_evaluated_at": now.isoformat(),
            }

        return {"actions": actions, "state": updated_state}

    @staticmethod
    def _extract_signal_strength(signal: dict[str, float | str] | None) -> float:
        if not signal:
            return 0.0
        direction = str(signal.get("direction", "flat")).lower()
        if "strength" in signal:
            raw_strength = float(signal.get("strength", 0.0) or 0.0)
            if direction == "short":
                return -raw_strength
            if direction == "flat":
                return 0.0
            return raw_strength
        return float(signal.get("score", 0.0) or 0.0)

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
