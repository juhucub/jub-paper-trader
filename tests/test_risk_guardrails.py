from __future__ import annotations

from datetime import datetime, timezone

from services.risk_guardrails import RiskGuardrails


def test_validate_order_vetoes_daily_loss_position_pct_and_open_positions():
    rg = RiskGuardrails(max_daily_loss=500, max_position_pct=0.10, max_open_positions=2)

    blocked_loss = rg.validate_order(
        candidate_order={"symbol": "AAPL", "qty": 1, "side": "buy", "price": 100},
        portfolio_state={"equity": 10_000, "daily_realized_pnl": -700, "open_positions": 1},
        market_state={},
    )
    assert blocked_loss["allowed"] is False
    assert blocked_loss["reason"] == "max_daily_loss_exceeded"

    blocked_size = rg.validate_order(
        candidate_order={"symbol": "AAPL", "qty": 20, "side": "buy", "price": 100},
        portfolio_state={"equity": 10_000, "daily_realized_pnl": 0, "open_positions": 1},
        market_state={},
    )
    assert blocked_size["reason"] == "max_position_pct_exceeded"

    blocked_positions = rg.validate_order(
        candidate_order={"symbol": "AAPL", "qty": 1, "side": "buy", "price": 100},
        portfolio_state={"equity": 10_000, "daily_realized_pnl": 0, "open_positions": 2},
        market_state={},
    )
    assert blocked_positions["reason"] == "max_open_positions_exceeded"


def test_validate_order_vetoes_illiquidity_penny_stock_and_cooldown():
    rg = RiskGuardrails(min_price=1.0, min_avg_dollar_volume=1_000_000, cooldown_after_losses=2)

    penny = rg.validate_order(
        candidate_order={"symbol": "XYZ", "qty": 100, "side": "buy", "price": 0.5},
        portfolio_state={"equity": 50_000, "open_positions": 0},
        market_state={},
    )
    assert penny["reason"] == "penny_stock_blocked"

    illiquid = rg.validate_order(
        candidate_order={"symbol": "XYZ", "qty": 10, "side": "buy", "price": 10},
        portfolio_state={"equity": 50_000, "open_positions": 0},
        market_state={"avg_dollar_volume": 100_000},
    )
    assert illiquid["reason"] == "illiquid_asset_blocked"

    rg.record_loss(datetime.now(timezone.utc))
    rg.record_loss(datetime.now(timezone.utc))
    cooldown = rg.validate_order(
        candidate_order={"symbol": "XYZ", "qty": 10, "side": "buy", "price": 10},
        portfolio_state={"equity": 50_000, "open_positions": 0},
        market_state={"avg_dollar_volume": 10_000_000},
    )
    assert cooldown["reason"] == "cooldown_after_losses_active"
