"""Snapshot-driven replay and evaluation helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.orm import Session

from agent_service.interfaces import ReplayEvaluation
from db.repositories.snapshots import list_bot_cycle_snapshots


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _max_drawdown(cumulative_returns: list[float]) -> float:
    peak = 1.0
    max_drawdown = 0.0
    for value in cumulative_returns:
        peak = max(peak, value)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - value) / peak)
    return max_drawdown


@dataclass(slots=True)
class SnapshotBacktestHook:
    """Replay cycle snapshots into a walk-forward evaluation."""

    db_session: Session
    transaction_cost_bps: float = 10.0
    slippage_bps: float = 5.0

    def run(self, strategy_name: str, symbols: list[str], benchmark_symbol: str) -> ReplayEvaluation:
        if symbols:
            matching: dict[str, Any] = {}
            for symbol in {symbol.upper() for symbol in symbols}:
                for snapshot in list_bot_cycle_snapshots(self.db_session, symbol=symbol, limit=500):
                    matching[snapshot.cycle_id] = snapshot
            snapshots = sorted(matching.values(), key=lambda snapshot: snapshot.created_at)
        else:
            snapshots = list(reversed(list_bot_cycle_snapshots(self.db_session, limit=500)))

        if len(snapshots) < 2:
            return ReplayEvaluation(
                as_of=datetime.now(timezone.utc),
                strategy_name=strategy_name,
                benchmark_symbol=benchmark_symbol,
                cycle_ids=[snapshot.cycle_id for snapshot in snapshots],
                summary="Insufficient snapshots for walk-forward replay.",
                total_return=0.0,
                benchmark_return=0.0,
                excess_return=0.0,
                max_drawdown=0.0,
                turnover=0.0,
                slippage_drag=0.0,
                spread_drag=0.0,
                diagnostics={"snapshot_count": len(snapshots)},
            )

        cumulative_value = 1.0
        cumulative_values = [cumulative_value]
        benchmark_value = 1.0
        benchmark_values = [benchmark_value]
        total_turnover = 0.0
        total_slippage_drag = 0.0
        total_spread_drag = 0.0
        regime_breakdown: dict[str, dict[str, float]] = {}
        scenario_breakdown: dict[str, dict[str, float]] = {}

        for current_snapshot, next_snapshot in zip(snapshots, snapshots[1:]):
            current_payload = current_snapshot.payload or {}
            next_payload = next_snapshot.payload or {}
            target_weights = self._extract_target_weights(current_payload)
            features = current_payload.get("features", {}) or {}
            next_features = next_payload.get("features", {}) or {}

            realized_return = 0.0
            for symbol, weight in target_weights.items():
                current_price = _coerce_float((features.get(symbol, {}) or {}).get("last_price"), 0.0)
                next_price = _coerce_float((next_features.get(symbol, {}) or {}).get("last_price"), 0.0)
                if current_price <= 0.0 or next_price <= 0.0:
                    continue
                symbol_return = (next_price - current_price) / current_price
                realized_return += max(weight, 0.0) * symbol_return

            turnover = self._estimate_turnover(current_payload, next_payload)
            spread_drag = sum(
                abs(weight) * _coerce_float((features.get(symbol, {}) or {}).get("bid_ask_spread"), 0.0) * 0.5
                for symbol, weight in target_weights.items()
            )
            slippage_drag = turnover * (self.slippage_bps / 10_000.0)
            transaction_drag = turnover * (self.transaction_cost_bps / 10_000.0)
            net_return = realized_return - spread_drag - slippage_drag - transaction_drag

            cumulative_value *= 1.0 + net_return
            cumulative_values.append(cumulative_value)
            total_turnover += turnover
            total_spread_drag += spread_drag
            total_slippage_drag += slippage_drag

            benchmark_return = self._extract_benchmark_return(current_payload, next_payload, benchmark_symbol)
            benchmark_value *= 1.0 + benchmark_return
            benchmark_values.append(benchmark_value)

            regime_label = ((current_payload.get("scenario_bundle", {}) or {}).get("regime_label")) or "unknown"
            regime_row = regime_breakdown.setdefault(regime_label, {"count": 0.0, "net_return": 0.0})
            regime_row["count"] += 1.0
            regime_row["net_return"] += net_return

            for scenario in ((current_payload.get("scenario_bundle", {}) or {}).get("scenarios", []) or []):
                scenario_name = str(scenario.get("name", "unknown"))
                scenario_row = scenario_breakdown.setdefault(
                    scenario_name,
                    {"count": 0.0, "probability_sum": 0.0},
                )
                scenario_row["count"] += 1.0
                scenario_row["probability_sum"] += _coerce_float(scenario.get("probability"), 0.0)

        terminal_payload = snapshots[-1].payload or {}
        terminal_regime = ((terminal_payload.get("scenario_bundle", {}) or {}).get("regime_label")) or "unknown"
        terminal_regime_row = regime_breakdown.setdefault(
            terminal_regime,
            {"count": 0.0, "net_return": 0.0},
        )
        terminal_regime_row["count"] += 1.0
        for scenario in ((terminal_payload.get("scenario_bundle", {}) or {}).get("scenarios", []) or []):
            scenario_name = str(scenario.get("name", "unknown"))
            scenario_row = scenario_breakdown.setdefault(
                scenario_name,
                {"count": 0.0, "probability_sum": 0.0},
            )
            scenario_row["count"] += 1.0
            scenario_row["probability_sum"] += _coerce_float(scenario.get("probability"), 0.0)

        total_return = cumulative_value - 1.0
        benchmark_return = benchmark_value - 1.0
        excess_return = total_return - benchmark_return

        return ReplayEvaluation(
            as_of=datetime.now(timezone.utc),
            strategy_name=strategy_name,
            benchmark_symbol=benchmark_symbol,
            cycle_ids=[snapshot.cycle_id for snapshot in snapshots],
            summary=(
                f"Replay across {len(snapshots)} cycles produced total_return={total_return:.4f} "
                f"and excess_return={excess_return:.4f}."
            ),
            total_return=total_return,
            benchmark_return=benchmark_return,
            excess_return=excess_return,
            max_drawdown=_max_drawdown(cumulative_values),
            turnover=total_turnover,
            slippage_drag=total_slippage_drag,
            spread_drag=total_spread_drag,
            regime_breakdown=regime_breakdown,
            scenario_breakdown=scenario_breakdown,
            diagnostics={
                "snapshot_count": len(snapshots),
                "benchmark_values": benchmark_values,
                "cumulative_values": cumulative_values,
            },
        )

    @staticmethod
    def _extract_target_weights(payload: dict[str, Any]) -> dict[str, float]:
        target_weights = payload.get("adjusted_target_weights") or payload.get("target_weights") or {}
        if target_weights:
            return {str(symbol): _coerce_float(weight, 0.0) for symbol, weight in target_weights.items()}

        allocation_payload = payload.get("allocation_proposal", {}) or {}
        return {
            str(line.get("symbol")): _coerce_float(line.get("target_weight"), 0.0)
            for line in allocation_payload.get("lines", []) or []
            if line.get("symbol")
        }

    @staticmethod
    def _estimate_turnover(current_payload: dict[str, Any], next_payload: dict[str, Any]) -> float:
        current_weights = SnapshotBacktestHook._extract_target_weights(current_payload)
        next_weights = SnapshotBacktestHook._extract_target_weights(next_payload)
        return sum(
            abs(_coerce_float(next_weights.get(symbol), 0.0) - _coerce_float(current_weights.get(symbol), 0.0))
            for symbol in set(current_weights) | set(next_weights)
        )

    @staticmethod
    def _extract_benchmark_return(
        current_payload: dict[str, Any],
        next_payload: dict[str, Any],
        benchmark_symbol: str,
    ) -> float:
        current_features = current_payload.get("features", {}) or {}
        next_features = next_payload.get("features", {}) or {}
        current_price = _coerce_float((current_features.get(benchmark_symbol, {}) or {}).get("last_price"), 0.0)
        next_price = _coerce_float((next_features.get(benchmark_symbol, {}) or {}).get("last_price"), 0.0)
        if current_price > 0.0 and next_price > 0.0:
            return (next_price - current_price) / current_price

        current_market_inputs = (
            (((current_payload.get("scenario_bundle", {}) or {}).get("diagnostics", {}) or {}).get("market_inputs", {}) or {})
        )
        return _coerce_float(current_market_inputs.get("benchmark_return"), 0.0)
