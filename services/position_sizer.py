from __future__ import annotations

from dataclasses import dataclass
from math import floor

from agent_service.interfaces import AllocationLine, AllocationProposal, SignalBundle


@dataclass(slots=True)
class PositionSizer:
    risk_per_trade_pct: float = 0.01
    confidence_floor: float = 0.5
    confidence_ceiling: float = 1.5
    min_notional: float = 20.0
    max_notional: float = 25_000.0
    lot_size: float = 0.0001
    max_position_pct: float = 0.20
    max_leverage: float = 1.0
    max_sector_pct: float = 0.35

    def size_allocation(
        self,
        proposal: AllocationProposal,
        signal_bundle: SignalBundle,
        current_positions: dict[str, float],
        latest_prices: dict[str, float],
        feature_rows: dict[str, dict[str, float]],
        equity: float,
        sector_map: dict[str, str] | None = None,
    ) -> AllocationProposal:
        signal_lookup: dict[str, dict[str, float | str]] = {
            intent.symbol: {
                "confidence": intent.confidence,
                "score": intent.score,
                "normalized_score": intent.normalized_score,
                "rank": intent.rank,
            }
            for intent in signal_bundle.intents
        }
        sized_targets = self.size_targets(
            target_weights={line.symbol: line.target_weight for line in proposal.lines},
            signals=signal_lookup,
            current_positions=current_positions,
            latest_prices=latest_prices,
            feature_rows=feature_rows,
            equity=equity,
            sector_map=sector_map,
        )
        sized_lines = [
            AllocationLine(
                symbol=line.symbol,
                target_weight=line.target_weight,
                confidence=line.confidence,
                rationale=line.rationale,
                target_notional=float(sized_targets.get(line.symbol, {}).get("target_notional", 0.0)),
                target_qty=float(sized_targets.get(line.symbol, {}).get("target_qty", 0.0)),
                diagnostics={
                    **line.diagnostics,
                    **sized_targets.get(line.symbol, {}),
                },
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
            lines=sized_lines,
            scenario_regime=proposal.scenario_regime,
            optimizer_name=proposal.optimizer_name,
            constraints_requested=dict(proposal.constraints_requested),
            diagnostics={**proposal.diagnostics, "sized_targets": sized_targets},
            lineage=dict(proposal.lineage),
        )

    def size_targets(
        self,
        target_weights: dict[str, float],
        signals: dict[str, dict[str, float | str]],
        current_positions: dict[str, float],
        latest_prices: dict[str, float],
        feature_rows: dict[str, dict[str, float]],
        equity: float,
        sector_map: dict[str, str] | None = None,
    ) -> dict[str, dict[str, float | str]]:
        if equity <= 0:
            return {}

        sector_map = sector_map or {}
        gross_notional_cap = max(0.0, equity * self.max_leverage)
        per_symbol_notional_cap = max(0.0, equity * self.max_position_pct)
        risk_budget = max(0.0, equity * self.risk_per_trade_pct)

        current_sector_notionals: dict[str, float] = {}
        for symbol, qty in current_positions.items():
            price = float(latest_prices.get(symbol, 0.0))
            if price <= 0:
                continue
            sector = sector_map.get(symbol, "UNSPECIFIED")
            current_sector_notionals[sector] = current_sector_notionals.get(sector, 0.0) + (float(qty) * price)

        sized: dict[str, dict[str, float | str]] = {}
        gross_target_notional = 0.0

        for symbol in sorted(target_weights):
            price = float(latest_prices.get(symbol, 0.0))
            if price <= 0:
                continue
            target_weight = max(0.0, float(target_weights.get(symbol, 0.0)))
            signal_row = signals.get(symbol, {})
            confidence = float(signal_row.get("confidence", 1.0) or 1.0)
            confidence_mult = min(self.confidence_ceiling, max(self.confidence_floor, confidence))

            feature_row = feature_rows.get(symbol, {})
            volatility_proxy = max(
                1e-6,
                float(feature_row.get("atr") or feature_row.get("stddev") or feature_row.get("volatility") or 0.0),
            )

            desired_notional = equity * target_weight
            volatility_notional_cap = risk_budget / volatility_proxy
            risk_adjusted_notional = min(desired_notional, volatility_notional_cap) * confidence_mult

            clamped_notional = min(risk_adjusted_notional, self.max_notional, per_symbol_notional_cap)
            if 0.0 < clamped_notional < self.min_notional and target_weight > 0.0:
                clamped_notional = self.min_notional

            sector = sector_map.get(symbol, "UNSPECIFIED")
            sector_cap = max(0.0, equity * self.max_sector_pct)
            sector_used = current_sector_notionals.get(sector, 0.0)
            sector_remaining = max(0.0, sector_cap - sector_used)
            post_sector_notional = min(clamped_notional, sector_remaining)

            gross_remaining = max(0.0, gross_notional_cap - gross_target_notional)
            final_notional = min(post_sector_notional, gross_remaining)

            raw_qty = final_notional / price if price > 0 else 0.0
            final_qty = self._round_to_lot(raw_qty)
            final_notional = final_qty * price

            gross_target_notional += final_notional
            current_sector_notionals[sector] = current_sector_notionals.get(sector, 0.0) + final_notional

            sized[symbol] = {
                "target_weight": target_weight,
                "target_notional": final_notional,
                "target_qty": final_qty,
                "confidence_multiplier": confidence_mult,
                "volatility_proxy": volatility_proxy,
                "risk_budget": risk_budget,
                "sector": sector,
                "sector_remaining": max(0.0, sector_cap - current_sector_notionals[sector]),
            }

        return sized

    def _round_to_lot(self, qty: float) -> float:
        if qty <= 0.0:
            return 0.0
        if self.lot_size <= 0:
            return round(qty, 4)
        return round(floor(qty / self.lot_size) * self.lot_size, 6)
