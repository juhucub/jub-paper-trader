from services.execution_router import ExecutionRouter
from services.position_sizer import PositionSizer


def test_position_sizer_applies_risk_and_sector_caps():
    sizer = PositionSizer(
        risk_per_trade_pct=0.01,
        min_notional=10.0,
        max_notional=5_000.0,
        max_position_pct=0.25,
        max_sector_pct=0.20,
        max_leverage=1.0,
    )

    sized = sizer.size_targets(
        target_weights={"AAPL": 0.30, "MSFT": 0.30},
        signals={
            "AAPL": {"confidence": 1.2},
            "MSFT": {"confidence": 1.0},
        },
        current_positions={},
        latest_prices={"AAPL": 100.0, "MSFT": 100.0},
        feature_rows={
            "AAPL": {"volatility": 0.02},
            "MSFT": {"volatility": 0.02},
        },
        equity=10_000.0,
        sector_map={"AAPL": "TECH", "MSFT": "TECH"},
    )

    assert sized["AAPL"]["target_notional"] == 2000.0
    assert sized["MSFT"]["target_notional"] == 0.0


def test_execution_router_prefers_precomputed_target_qtys():
    router = ExecutionRouter(min_trade_notional=10.0)
    deltas = router.to_rebalance_deltas(
        target_weights={"AAPL": 0.10},
        current_positions={"AAPL": 1.0},
        latest_prices={"AAPL": 100.0},
        equity=10_000.0,
        target_qtys={"AAPL": 5.0},
    )

    assert len(deltas) == 1
    assert deltas[0].symbol == "AAPL"
    assert deltas[0].side == "buy"
    assert deltas[0].qty == 4.0
