#Risk rules and pre trade validation
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

@dataclass(slots=True)
class RiskGuardrails:
    max_daily_loss: float = 2_000.0
    #max % of total equity per position
    max_position_pct: float = 0.20
    max_open_positions: int = 12
    #avoid low price, e.g. high volatility, low liquidity, and manipulation prone
    min_price: float = 1.0
    #Enter/exit no major slippage
    min_avg_dollar_volume: float = 250_000.0
    cooldown_after_losses: int = 3
    cooldown_minutes: int = 30
    #in memory loss timestamps
    _recent_losses: list[datetime] = field(default_factory=list)

    def _in_cooldown(self, now: datetime | None = None) -> bool:
        now = now or datetime.now(timezone.utc)
        active_window = now.timestamp() - (self.cooldown_minutes * 60)
        self._recent_losses = [x for x in self._recent_losses if x.timestamp() >= active_window]
        return len(self._recent_losses) >= self.cooldown_after_losses

    def record_loss(self, when: datetime | None = None) -> None:
        self._recent_losses.append(when or datetime.now(timezone.utc))

    def validate_order(
        self,
        candidate_order: dict[str, Any],
        portfolio_state: dict[str, Any],
        market_state: dict[str, Any],
    ) -> dict[str, Any]:
        side = str(candidate_order.get("side", "buy")).lower()
        qty = float(candidate_order.get("qty", 0.0))
        price = float(candidate_order.get("price") or market_state.get("last_price") or 0.0)

        if qty <= 0:
            return {"allowed": False, "reason": "invalid_quantity"}

        if portfolio_state.get("daily_realized_pnl", 0.0) <= -abs(self.max_daily_loss):
            return {"allowed": False, "reason": "max_daily_loss_exceeded"}

        equity = float(portfolio_state.get("equity", 0.0))
        notional = qty * max(price, 0.0)
        if equity > 0 and (notional / equity) > self.max_position_pct:
            return {"allowed": False, "reason": "max_position_pct_exceeded"}

        current_open_positions = int(portfolio_state.get("open_positions", 0))
        creates_new_position = bool(candidate_order.get("creates_new_position", side == "buy"))
        if creates_new_position and current_open_positions >= self.max_open_positions:
            return {"allowed": False, "reason": "max_open_positions_exceeded"}

        if price and price < self.min_price:
            return {"allowed": False, "reason": "penny_stock_blocked"}

        avg_dollar_volume = float(market_state.get("avg_dollar_volume", 0.0))
        if avg_dollar_volume and avg_dollar_volume < self.min_avg_dollar_volume:
            return {"allowed": False, "reason": "illiquid_asset_blocked"}

        # v1: no after-hours check by design.

        if self._in_cooldown():
            return {"allowed": False, "reason": "cooldown_after_losses_active"}

        return {"allowed": True, "reason": "ok"} 

    def allow_order(self, symbol: str, qty: float, side: str) -> bool:
        _ = symbol
        decision = self.validate_order(
            candidate_order={"symbol": symbol, "qty": qty, "side": side},
            portfolio_state={},
            market_state={},
        )
        return bool(decision["allowed"])