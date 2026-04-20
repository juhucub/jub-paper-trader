from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone

from agent_service.interfaces import AllocationLine, AllocationProposal
from services.execution_router import ExecutionRouter
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
        market_state={"avg_dollar_volume": 10_000_000},
    )
    assert blocked_size["reason"] == "max_position_pct_exceeded"

    blocked_positions = rg.validate_order(
        candidate_order={"symbol": "AAPL", "qty": 1, "side": "buy", "price": 100},
        portfolio_state={"equity": 10_000, "daily_realized_pnl": 0, "open_positions": 2},
        market_state={"avg_dollar_volume": 10_000_000},
    )
    assert blocked_positions["reason"] == "max_open_positions_exceeded"


def test_validate_order_vetoes_illiquidity_penny_stock_and_cooldown():
    rg = RiskGuardrails(min_price=1.0, min_avg_dollar_volume=1_000_000, cooldown_after_losses=2)

    penny = rg.validate_order(
        candidate_order={"symbol": "XYZ", "qty": 100, "side": "buy", "price": 0.5},
        portfolio_state={"equity": 50_000, "open_positions": 0},
        market_state={"avg_dollar_volume": 10_000_000},
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


def test_validate_order_fails_closed_on_missing_market_data_for_exposure_increase():
    rg = RiskGuardrails(min_avg_dollar_volume=1_000_000)

    missing_price = rg.validate_order(
        candidate_order={"symbol": "AAPL", "qty": 10, "side": "buy"},
        portfolio_state={"equity": 50_000, "open_positions": 0},
        market_state={"avg_dollar_volume": 5_000_000},
    )
    assert missing_price["allowed"] is False
    assert missing_price["reason"] == "missing_price_for_exposure_increase"
    assert missing_price["stale_data_reason"] == "missing_price_for_exposure_increase"
    assert missing_price["risk_reducing"] is False

    missing_adv = rg.validate_order(
        candidate_order={"symbol": "AAPL", "qty": 10, "side": "buy", "price": 100},
        portfolio_state={"equity": 50_000, "open_positions": 0},
        market_state={},
    )
    assert missing_adv["allowed"] is False
    assert missing_adv["reason"] == "missing_avg_dollar_volume_for_exposure_increase"
    assert missing_adv["stale_data_reason"] == "missing_avg_dollar_volume_for_exposure_increase"


def test_validate_order_allows_risk_reducing_exit_when_market_data_is_missing():
    rg = RiskGuardrails(min_avg_dollar_volume=1_000_000)

    exit_decision = rg.validate_order(
        candidate_order={"symbol": "AAPL", "qty": 5, "side": "sell"},
        portfolio_state={"equity": 50_000, "open_positions": 1},
        market_state={},
    )
    assert exit_decision["allowed"] is True
    assert exit_decision["reason"] == "ok"
    assert exit_decision["risk_reducing"] is True
    assert exit_decision["stale_data_reason"] is None


def test_validate_allocation_emits_symbol_level_risk_explainability():
    rg = RiskGuardrails(max_position_pct=0.10, min_avg_dollar_volume=1.0)
    router = ExecutionRouter(min_trade_notional=10.0, rebalance_tolerance_pct=0.0)
    now = datetime.now(timezone.utc)
    proposal = AllocationProposal(
        as_of=now,
        source_signal_bundle_at=now,
        source_scenario_bundle_at=now,
        target_gross_exposure=0.95,
        cash_buffer=0.05,
        lines=[
            AllocationLine(
                symbol="AAPL",
                target_weight=0.20,
                confidence=0.9,
                rationale="increase leader",
                target_notional=2_000.0,
                target_qty=20.0,
                diagnostics={"signal_rank": 1},
            ),
            AllocationLine(
                symbol="MSFT",
                target_weight=0.05,
                confidence=0.7,
                rationale="small rebalance",
                target_notional=500.0,
                target_qty=5.0,
            ),
        ],
    )

    adjusted = rg.validate_allocation(
        proposal,
        execution_router=router,
        current_positions={"AAPL": 0.0, "MSFT": 5.0},
        latest_prices={"AAPL": 100.0, "MSFT": 100.0},
        equity=10_000.0,
        portfolio_state={"equity": 10_000.0, "daily_realized_pnl": 0.0, "open_positions": 1},
        market_states={
            "AAPL": {"avg_dollar_volume": 5_000_000.0, "last_price": 100.0},
            "MSFT": {"avg_dollar_volume": 5_000_000.0, "last_price": 100.0},
        },
    )

    assert [line.symbol for line in adjusted.approved_lines] == ["MSFT"]
    assert adjusted.blocked_lines[0].symbol == "AAPL"
    assert adjusted.blocked_lines[0].reason == "max_position_pct_exceeded"
    assert adjusted.blocked_lines[0].diagnostics["clip_amount"] == 0.20

    symbol_outcomes = {item.symbol: item for item in adjusted.symbol_details}
    assert symbol_outcomes["AAPL"].status == "blocked"
    assert symbol_outcomes["AAPL"].requested_weight == 0.20
    assert symbol_outcomes["AAPL"].approved_weight == 0.0
    assert symbol_outcomes["AAPL"].clip_amount == 0.20
    assert symbol_outcomes["AAPL"].risk_reducing is False
    assert symbol_outcomes["AAPL"].diagnostics["lineage"]["allocation_as_of"] == now
    assert symbol_outcomes["AAPL"].diagnostics["line_diagnostics"]["signal_rank"] == 1
    assert symbol_outcomes["AAPL"].reasons[0].code == "max_position_pct_exceeded"
    assert symbol_outcomes["MSFT"].status == "unchanged"
    assert symbol_outcomes["MSFT"].reasons[0].code == "no_rebalance_delta"


def test_validate_allocation_fails_closed_on_missing_price_for_exposure_increase():
    rg = RiskGuardrails(min_avg_dollar_volume=1.0)
    router = ExecutionRouter(min_trade_notional=10.0, rebalance_tolerance_pct=0.0)
    now = datetime.now(timezone.utc)
    proposal = AllocationProposal(
        as_of=now,
        source_signal_bundle_at=now,
        source_scenario_bundle_at=now,
        target_gross_exposure=0.90,
        cash_buffer=0.05,
        lines=[
            AllocationLine(
                symbol="AAPL",
                target_weight=0.10,
                confidence=0.8,
                rationale="new long",
            )
        ],
    )

    adjusted = rg.validate_allocation(
        proposal,
        execution_router=router,
        current_positions={"AAPL": 0.0},
        latest_prices={"AAPL": 0.0},
        equity=10_000.0,
        portfolio_state={"equity": 10_000.0, "daily_realized_pnl": 0.0, "open_positions": 0},
        market_states={"AAPL": {"avg_dollar_volume": 5_000_000.0}},
    )

    assert adjusted.approved_lines == []
    assert adjusted.blocked_lines[0].reason == "missing_price_for_allocation_validation"
    assert adjusted.symbol_details[0].reasons[0].metadata["stale_data_reason"] == "missing_price_for_allocation_validation"
    assert adjusted.diagnostics["symbol_outcomes"]["AAPL"]["stale_data_reason"] == "missing_price_for_allocation_validation"

    payload = asdict(adjusted)
    assert payload["lineage"]["signal_bundle_as_of"] == now
    assert payload["symbol_details"][0]["reasons"][0]["code"] == "missing_price_for_allocation_validation"
