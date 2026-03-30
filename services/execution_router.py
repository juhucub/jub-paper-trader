#Execution routing helpers for converting target weights into rebalance deltas.

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

    def to_rebalance_deltas(
        self,
        target_weights: dict[str, float],
        current_positions: dict[str, float],
        latest_prices: dict[str, float],
        equity: float,
    ) -> list[RebalanceDelta]:
        if equity <= 0:
            return []

        deltas: list[RebalanceDelta] = []
        symbols = set(target_weights) | set(current_positions)
        for symbol in symbols:
            price = float(latest_prices.get(symbol, 0.0))
            if price <= 0:
                continue

            current_qty = float(current_positions.get(symbol, 0.0))
            current_weight = (current_qty * price) / equity
            target_weight = float(target_weights.get(symbol, 0.0))

            notional_delta = (target_weight - current_weight) * equity
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
