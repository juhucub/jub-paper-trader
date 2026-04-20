from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from agent_service.replay import SnapshotBacktestHook
from db.base import Base
from db.repositories.snapshots import (
    create_bot_cycle_snapshot,
    list_bot_cycle_snapshots,
    query_blocked_cycle_symbols,
    reconstruct_symbol_cycle_history,
)


def _build_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, future=True)()


def _persist_snapshot(session, cycle_id: str, created_at: datetime, payload: dict) -> None:
    snapshot = create_bot_cycle_snapshot(session, cycle_id=cycle_id, payload=payload)
    snapshot.created_at = created_at
    session.commit()


def test_snapshot_query_helpers_reconstruct_symbol_history_beyond_latest_limit() -> None:
    session = _build_session()
    now = datetime(2026, 4, 17, 15, 30, tzinfo=timezone.utc)

    _persist_snapshot(
        session,
        "cycle-aapl",
        now,
        {
            "symbols": ["AAPL"],
            "monitoring_decision": {"blocked_symbols": ["AAPL"], "inaction_reasons": ["AAPL:max_position_pct_exceeded"]},
            "symbol_lineage": {"AAPL": {"signal": {"score": 0.4}}},
            "scenario_bundle": {"regime_label": "risk_on"},
        },
    )
    _persist_snapshot(
        session,
        "cycle-msft-1",
        now + timedelta(minutes=1),
        {"symbols": ["MSFT"], "monitoring_decision": {"blocked_symbols": []}, "symbol_lineage": {"MSFT": {}}},
    )
    _persist_snapshot(
        session,
        "cycle-msft-2",
        now + timedelta(minutes=2),
        {"symbols": ["MSFT"], "monitoring_decision": {"blocked_symbols": []}, "symbol_lineage": {"MSFT": {}}},
    )

    snapshots = list_bot_cycle_snapshots(session, symbol="AAPL", limit=1)
    blocked = query_blocked_cycle_symbols(session, symbol="AAPL", limit=1)
    history = reconstruct_symbol_cycle_history(session, symbol="AAPL", limit=5)

    assert [snapshot.cycle_id for snapshot in snapshots] == ["cycle-aapl"]
    assert blocked[0]["symbol"] == "AAPL"
    assert blocked[0]["inaction_reasons"] == ["AAPL:max_position_pct_exceeded"]
    assert history[0]["cycle_id"] == "cycle-aapl"
    assert history[0]["scenario_regime"] == "risk_on"


def test_snapshot_backtest_hook_reports_walk_forward_metrics() -> None:
    session = _build_session()
    now = datetime(2026, 4, 17, 15, 30, tzinfo=timezone.utc)

    _persist_snapshot(
        session,
        "cycle-1",
        now,
        {
            "symbols": ["AAPL"],
            "adjusted_target_weights": {"AAPL": 0.50},
            "features": {
                "AAPL": {"last_price": 100.0, "bid_ask_spread": 0.002},
                "SPY": {"last_price": 500.0},
            },
            "scenario_bundle": {
                "regime_label": "risk_on",
                "scenarios": [{"name": "base_case", "probability": 0.6}],
            },
        },
    )
    _persist_snapshot(
        session,
        "cycle-2",
        now + timedelta(minutes=1),
        {
            "symbols": ["AAPL"],
            "adjusted_target_weights": {"AAPL": 0.30},
            "features": {
                "AAPL": {"last_price": 105.0, "bid_ask_spread": 0.003},
                "SPY": {"last_price": 505.0},
            },
            "scenario_bundle": {
                "regime_label": "neutral",
                "scenarios": [{"name": "downside_stress", "probability": 0.4}],
            },
        },
    )
    _persist_snapshot(
        session,
        "cycle-3",
        now + timedelta(minutes=2),
        {
            "symbols": ["AAPL"],
            "adjusted_target_weights": {"AAPL": 0.10},
            "features": {
                "AAPL": {"last_price": 103.0, "bid_ask_spread": 0.004},
                "SPY": {"last_price": 507.0},
            },
            "scenario_bundle": {
                "regime_label": "risk_off",
                "scenarios": [{"name": "liquidity_stress", "probability": 0.5}],
            },
        },
    )

    evaluation = SnapshotBacktestHook(session).run("default", ["AAPL"], "SPY")

    assert evaluation.strategy_name == "default"
    assert evaluation.cycle_ids == ["cycle-1", "cycle-2", "cycle-3"]
    assert evaluation.turnover > 0.0
    assert evaluation.spread_drag > 0.0
    assert evaluation.slippage_drag > 0.0
    assert "risk_on" in evaluation.regime_breakdown
    assert "liquidity_stress" in evaluation.scenario_breakdown
    assert evaluation.diagnostics["snapshot_count"] == 3
