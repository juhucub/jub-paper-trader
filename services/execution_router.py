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


@dataclass(slots=True)
class ExecutionRouter:
    min_trade_notional: float = 20.0
    rebalance_tolerance_pct: float = 0.02

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

            is_full_exit = current_qty > 0.0 and target_weight <= 0.0
            if not is_full_exit and abs(weight_delta) < self.rebalance_tolerance_pct:
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
                )
            )

        return sorted(deltas, key=lambda x: (x.symbol, x.side))
