#Execution routing helpers for converting target allocations into rebalance deltas.

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RebalanceDelta:
    symbol: str
    side: str
    qty: float
    target_weight: float
    current_weight: float
    reference_price: float
    current_qty: float = 0.0
    desired_qty: float = 0.0
    notional_delta: float = 0.0


@dataclass(slots=True)
class ExecutionRouter:
    min_trade_notional: float = 20.0
    rebalance_tolerance_pct: float = 0.02

    @staticmethod
    def build_execution_plan(deltas: list[RebalanceDelta]) -> list[RebalanceDelta]:
        """Sell first so risk-reducing orders free capital before new buys."""

        return sorted(deltas, key=lambda delta: (0 if delta.side == "sell" else 1, delta.symbol))

    def to_rebalance_deltas(
        self,
        target_weights: dict[str, float],
        current_positions: dict[str, float],
        latest_prices: dict[str, float],
        equity: float,
        target_notionals: dict[str, float] | None = None,
        target_qtys: dict[str, float] | None = None,
    ) -> list[RebalanceDelta]:
        if equity <= 0:
            return []

        deltas: list[RebalanceDelta] = []
        target_notionals = target_notionals or {}
        target_qtys = target_qtys or {}

        symbols = set(target_weights) | set(current_positions) | set(target_notionals) | set(target_qtys)
        for symbol in symbols:
            price = float(latest_prices.get(symbol, 0.0))
            if price <= 0:
                continue

            current_qty = float(current_positions.get(symbol, 0.0))
            current_weight = (current_qty * price) / equity
            target_weight = float(target_weights.get(symbol, 0.0))
            weight_delta = target_weight - current_weight

            if symbol in target_qtys:
                desired_qty = float(target_qtys[symbol])
                notional_delta = (desired_qty - current_qty) * price
            elif symbol in target_notionals:
                desired_notional = float(target_notionals[symbol])
                notional_delta = desired_notional - (current_qty * price)
            else:
                notional_delta = weight_delta * equity

            if abs(weight_delta) < self.rebalance_tolerance_pct:
                continue

            if abs(notional_delta) < self.min_trade_notional:
                continue

            qty_delta = abs(notional_delta) / price
            side = "buy" if notional_delta > 0 else "sell"
            deltas.append(
                RebalanceDelta(
                    symbol=symbol,
                    side=side,
                    qty=round(qty_delta, 4),
                    target_weight=target_weight,
                    current_weight=current_weight,
                    reference_price=price,
                    current_qty=current_qty,
                    desired_qty=float(target_qtys.get(symbol, current_qty + qty_delta if side == "buy" else max(current_qty - qty_delta, 0.0))),
                    notional_delta=notional_delta,
                )
            )

        return self.build_execution_plan(deltas)
