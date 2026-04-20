"""Deterministic risk rules and audit-friendly allocation validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from agent_service.interfaces import (
    AllocationProposal,
    BlockedAllocationLine,
    PolicyConstraint,
    RiskAllocationDetail,
    RiskAdjustedAllocation,
)


@dataclass(slots=True)
class RiskGuardrails:
    max_daily_loss: float = 2_000.0
    # max % of total equity per position
    max_position_pct: float = 0.20
    max_open_positions: int = 12
    # avoid low price, e.g. high volatility, low liquidity, and manipulation prone
    min_price: float = 1.0
    # enter/exit with reasonable liquidity
    min_avg_dollar_volume: float = 250_000.0
    cooldown_after_losses: int = 3
    cooldown_minutes: int = 30
    # in-memory loss timestamps
    _recent_losses: list[datetime] = field(default_factory=list)

    def _in_cooldown(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(timezone.utc)
        active_window = now.timestamp() - (self.cooldown_minutes * 60)
        self._recent_losses = [x for x in self._recent_losses if x.timestamp() >= active_window]
        return len(self._recent_losses) >= self.cooldown_after_losses

    def record_loss(self, when: datetime | None = None) -> None:
        self._recent_losses.append(when or datetime.now(timezone.utc))

    @staticmethod
    def _is_exposure_increasing_order(candidate_order: dict[str, Any]) -> bool:
        side = str(candidate_order.get("side", "buy")).lower()
        return side == "buy"

    @staticmethod
    def _decision_payload(
        *,
        allowed: bool,
        reason: str,
        candidate_order: dict[str, Any],
        market_state: dict[str, Any],
        risk_reducing: bool,
        stale_data_reason: str | None = None,
    ) -> dict[str, Any]:
        price = float(candidate_order.get("price") or market_state.get("last_price") or 0.0)
        qty = float(candidate_order.get("qty", 0.0) or 0.0)
        return {
            "allowed": allowed,
            "reason": reason,
            "symbol": str(candidate_order.get("symbol", "")),
            "requested_qty": qty,
            "reference_price": price,
            "requested_notional": qty * max(price, 0.0),
            "avg_dollar_volume": float(market_state.get("avg_dollar_volume") or 0.0),
            "risk_reducing": risk_reducing,
            "stale_data_reason": stale_data_reason,
        }

    @staticmethod
    def _is_risk_reducing_allocation(
        *,
        target_weight: float,
        current_weight: float | None,
        target_qty: float | None,
        current_qty: float,
    ) -> bool:
        """Fail closed unless the long-only target clearly reduces existing exposure."""

        if target_qty is not None:
            return float(target_qty) <= max(current_qty, 0.0)
        if current_weight is not None:
            return float(target_weight) <= max(current_weight, 0.0)
        return float(target_weight) <= 0.0 and current_qty > 0.0

    def validate_order(
        self,
        candidate_order: dict[str, Any],
        portfolio_state: dict[str, Any],
        market_state: dict[str, Any],
    ) -> dict[str, Any]:
        side = str(candidate_order.get("side", "buy")).lower()
        qty = float(candidate_order.get("qty", 0.0))
        price = float(candidate_order.get("price") or market_state.get("last_price") or 0.0)
        avg_dollar_volume = float(market_state.get("avg_dollar_volume") or 0.0)
        increases_exposure = self._is_exposure_increasing_order(candidate_order)
        risk_reducing = not increases_exposure

        if qty <= 0:
            return self._decision_payload(
                allowed=False,
                reason="invalid_quantity",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=risk_reducing,
            )

        if portfolio_state.get("daily_realized_pnl", 0.0) <= -abs(self.max_daily_loss):
            return self._decision_payload(
                allowed=False,
                reason="max_daily_loss_exceeded",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=risk_reducing,
            )

        # Fail closed on missing market data only for exposure-increasing orders.
        # Risk-reducing exits are allowed to proceed even if the quote payload is incomplete.
        if increases_exposure and price <= 0.0:
            return self._decision_payload(
                allowed=False,
                reason="missing_price_for_exposure_increase",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=False,
                stale_data_reason="missing_price_for_exposure_increase",
            )
        if increases_exposure and avg_dollar_volume <= 0.0:
            return self._decision_payload(
                allowed=False,
                reason="missing_avg_dollar_volume_for_exposure_increase",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=False,
                stale_data_reason="missing_avg_dollar_volume_for_exposure_increase",
            )

        equity = float(portfolio_state.get("equity", 0.0))
        notional = qty * max(price, 0.0)
        # Position-size cap should only prevent opening/increasing exposure.
        # Sell orders in this strategy are used to reduce existing long positions.
        if side == "buy" and equity > 0 and (notional / equity) > self.max_position_pct:
            return self._decision_payload(
                allowed=False,
                reason="max_position_pct_exceeded",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=False,
            )

        current_open_positions = int(portfolio_state.get("open_positions", 0))
        creates_new_position = bool(candidate_order.get("creates_new_position", side == "buy"))
        if creates_new_position and current_open_positions >= self.max_open_positions:
            return self._decision_payload(
                allowed=False,
                reason="max_open_positions_exceeded",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=False,
            )

        if price and price < self.min_price:
            return self._decision_payload(
                allowed=False,
                reason="penny_stock_blocked",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=risk_reducing,
            )

        if avg_dollar_volume and avg_dollar_volume < self.min_avg_dollar_volume:
            return self._decision_payload(
                allowed=False,
                reason="illiquid_asset_blocked",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=risk_reducing,
            )

        if self._in_cooldown():
            return self._decision_payload(
                allowed=False,
                reason="cooldown_after_losses_active",
                candidate_order=candidate_order,
                market_state=market_state,
                risk_reducing=risk_reducing,
            )

        return self._decision_payload(
            allowed=True,
            reason="ok",
            candidate_order=candidate_order,
            market_state=market_state,
            risk_reducing=risk_reducing,
        )

    def validate_allocation(
        self,
        proposal: AllocationProposal,
        *,
        execution_router: Any,
        current_positions: dict[str, float],
        latest_prices: dict[str, float],
        equity: float,
        portfolio_state: dict[str, Any],
        market_states: dict[str, dict[str, float]],
    ) -> RiskAdjustedAllocation:
        symbols = {line.symbol for line in proposal.lines}
        filtered_current_positions = {symbol: float(current_positions.get(symbol, 0.0)) for symbol in symbols}
        deltas = execution_router.to_rebalance_deltas(
            target_weights={line.symbol: line.target_weight for line in proposal.lines},
            current_positions=filtered_current_positions,
            latest_prices=latest_prices,
            equity=equity,
            target_notionals={
                line.symbol: line.target_notional
                for line in proposal.lines
                if line.target_notional is not None
            },
            target_qtys={
                line.symbol: line.target_qty
                for line in proposal.lines
                if line.target_qty is not None
            },
        )
        deltas_by_symbol = {delta.symbol: delta for delta in deltas}
        approved_lines = []
        blocked_lines: list[BlockedAllocationLine] = []
        symbol_details: list[RiskAllocationDetail] = []
        guardrail_notes: list[str] = []

        for line in proposal.lines:
            delta = deltas_by_symbol.get(line.symbol)
            latest_price = float(latest_prices.get(line.symbol, 0.0) or 0.0)
            current_qty = filtered_current_positions.get(line.symbol, 0.0)
            current_weight = None
            if equity > 0.0 and latest_price > 0.0:
                current_weight = (current_qty * latest_price) / equity

            lineage = {
                "allocation_as_of": proposal.as_of,
                "signal_bundle_as_of": proposal.source_signal_bundle_at,
                "scenario_bundle_as_of": proposal.source_scenario_bundle_at,
            }
            base_diagnostics = {
                "requested_confidence": line.confidence,
                "line_diagnostics": dict(line.diagnostics),
                "market_state": dict(market_states.get(line.symbol, {})),
            }

            if delta is None:
                risk_reducing_without_delta = self._is_risk_reducing_allocation(
                    target_weight=line.target_weight,
                    current_weight=current_weight,
                    target_qty=line.target_qty,
                    current_qty=current_qty,
                )
                if latest_price <= 0.0 and not risk_reducing_without_delta:
                    stale_reason = "missing_price_for_allocation_validation"
                    blocked_lines.append(
                        BlockedAllocationLine(
                            symbol=line.symbol,
                            requested_weight=line.target_weight,
                            requested_notional=line.target_notional,
                            reason=stale_reason,
                            diagnostics={
                                **base_diagnostics,
                                "approved_weight": 0.0,
                                "clip_amount": line.target_weight,
                                "stale_data_reason": stale_reason,
                                "risk_reducing": False,
                            },
                        )
                    )
                    symbol_details.append(
                        RiskAllocationDetail(
                            symbol=line.symbol,
                            status="blocked",
                            requested_weight=line.target_weight,
                            approved_weight=0.0,
                            clip_amount=line.target_weight,
                            requested_notional=line.target_notional,
                            approved_notional=0.0,
                            requested_qty=line.target_qty,
                            approved_qty=0.0,
                            risk_reducing=False,
                            reasons=[
                                PolicyConstraint(
                                    code=stale_reason,
                                    message="Blocked because price data was missing for an exposure-increasing target.",
                                    metadata={"stale_data_reason": stale_reason},
                                )
                            ],
                            diagnostics={**base_diagnostics, "current_weight": current_weight, "lineage": lineage},
                        )
                    )
                    guardrail_notes.append(f"{line.symbol}:{stale_reason}")
                    continue

                approved_lines.append(line)
                symbol_details.append(
                    RiskAllocationDetail(
                        symbol=line.symbol,
                        status="unchanged",
                        requested_weight=line.target_weight,
                        approved_weight=line.target_weight,
                        clip_amount=0.0,
                        requested_notional=line.target_notional,
                        approved_notional=line.target_notional,
                        requested_qty=line.target_qty,
                        approved_qty=line.target_qty,
                        risk_reducing=risk_reducing_without_delta,
                        reasons=[
                            PolicyConstraint(
                                code="no_rebalance_delta",
                                message="Target stayed unchanged because no rebalance delta cleared routing thresholds.",
                            )
                        ],
                        diagnostics={**base_diagnostics, "current_weight": current_weight, "lineage": lineage},
                    )
                )
                continue

            rebalance_delta = {
                "side": delta.side,
                "qty": delta.qty,
                "reference_price": delta.reference_price,
                "target_weight": delta.target_weight,
                "current_weight": delta.current_weight,
            }
            decision = self.validate_order(
                candidate_order={
                    "symbol": delta.symbol,
                    "qty": delta.qty,
                    "side": delta.side,
                    "price": delta.reference_price,
                    "creates_new_position": delta.side == "buy" and current_qty == 0.0,
                },
                portfolio_state=portfolio_state,
                market_state=market_states.get(delta.symbol, {}),
            )
            line_diagnostics = {**base_diagnostics, "rebalance_delta": rebalance_delta}
            if decision["allowed"]:
                approved_lines.append(line)
                symbol_details.append(
                    RiskAllocationDetail(
                        symbol=line.symbol,
                        status="approved",
                        requested_weight=line.target_weight,
                        approved_weight=line.target_weight,
                        clip_amount=0.0,
                        requested_notional=line.target_notional,
                        approved_notional=line.target_notional,
                        requested_qty=line.target_qty if line.target_qty is not None else delta.qty,
                        approved_qty=line.target_qty if line.target_qty is not None else delta.qty,
                        risk_reducing=bool(decision["risk_reducing"]),
                        reasons=[
                            PolicyConstraint(
                                code=str(decision["reason"]),
                                message="Proposal passed deterministic validation.",
                            )
                        ],
                        diagnostics={**line_diagnostics, "current_weight": delta.current_weight, "lineage": lineage},
                    )
                )
                continue

            approved_weight = line.target_weight if decision["risk_reducing"] else 0.0
            clip_amount = max(line.target_weight - approved_weight, 0.0)
            approved_notional = line.target_notional if decision["risk_reducing"] else 0.0
            approved_qty = line.target_qty if decision["risk_reducing"] else 0.0
            blocked_lines.append(
                BlockedAllocationLine(
                    symbol=line.symbol,
                    requested_weight=line.target_weight,
                    requested_notional=line.target_notional,
                    reason=str(decision["reason"]),
                    diagnostics={
                        **line_diagnostics,
                        "approved_weight": approved_weight,
                        "clip_amount": clip_amount,
                        "stale_data_reason": decision.get("stale_data_reason"),
                        "risk_reducing": bool(decision["risk_reducing"]),
                    },
                )
            )
            guardrail_notes.append(f"{line.symbol}:{decision['reason']}")
            symbol_details.append(
                RiskAllocationDetail(
                    symbol=line.symbol,
                    status="blocked",
                    requested_weight=line.target_weight,
                    approved_weight=approved_weight,
                    clip_amount=clip_amount,
                    requested_notional=line.target_notional,
                    approved_notional=approved_notional,
                    requested_qty=line.target_qty if line.target_qty is not None else delta.qty,
                    approved_qty=approved_qty,
                    risk_reducing=bool(decision["risk_reducing"]),
                    reasons=[
                        PolicyConstraint(
                            code=str(decision["reason"]),
                            message="Deterministic guardrail vetoed this requested allocation.",
                            metadata={"stale_data_reason": decision.get("stale_data_reason")},
                        )
                    ],
                    diagnostics={**line_diagnostics, "current_weight": delta.current_weight, "lineage": lineage},
                )
            )

        return RiskAdjustedAllocation(
            as_of=proposal.as_of,
            source_allocation_at=proposal.as_of,
            approved_lines=approved_lines,
            blocked_lines=blocked_lines,
            symbol_details=symbol_details,
            gross_exposure_cap=proposal.target_gross_exposure,
            cash_buffer_applied=proposal.cash_buffer,
            guardrail_notes=guardrail_notes,
            source_signal_bundle_at=proposal.source_signal_bundle_at,
            source_scenario_bundle_at=proposal.source_scenario_bundle_at,
            source_decision_policy_at=proposal.source_decision_policy_at,
            source_exit_policy_at=proposal.source_exit_policy_at,
            lineage={
                "allocation_as_of": proposal.as_of,
                "signal_bundle_as_of": proposal.source_signal_bundle_at,
                "scenario_bundle_as_of": proposal.source_scenario_bundle_at,
                "decision_policy_as_of": proposal.source_decision_policy_at,
                "exit_policy_as_of": proposal.source_exit_policy_at,
            },
            diagnostics={
                "validated_symbols": sorted(symbols),
                "approved_symbols": [line.symbol for line in approved_lines],
                "blocked_symbols": [line.symbol for line in blocked_lines],
                "symbol_outcomes": {
                    item.symbol: {
                        "status": item.status,
                        "reasons": [reason.code for reason in item.reasons],
                        "clip_amount": item.clip_amount,
                        "risk_reducing": item.risk_reducing,
                        "stale_data_reason": next(
                            (reason.metadata.get("stale_data_reason") for reason in item.reasons if reason.metadata.get("stale_data_reason")),
                            None,
                        ),
                    }
                    for item in symbol_details
                },
            },
        )

    def allow_order(self, symbol: str, qty: float, side: str) -> bool:
        decision = self.validate_order(
            candidate_order={"symbol": symbol, "qty": qty, "side": side},
            portfolio_state={},
            market_state={},
        )
        return bool(decision["allowed"])
