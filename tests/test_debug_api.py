from __future__ import annotations

from types import SimpleNamespace

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from backend.dependencies.wiring import get_container
from backend.main import create_app
from db.base import Base
from db.repositories.snapshots import create_bot_cycle_snapshot


def test_latest_cycle_debug_route_renders_latest_persisted_snapshot() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, future=True)()

    create_bot_cycle_snapshot(
        session,
        cycle_id="cycle-123",
        payload={
            "cycle_id": "cycle-123",
            "started_at": "2026-04-17T14:31:00+00:00",
            "symbols": ["AAPL"],
            "scenario_bundle": {"regime_label": "neutral"},
            "decision_summaries": {
                "AAPL": {
                    "symbol": "AAPL",
                    "decision_status": "SUBMITTED",
                    "decision_reason": "order_submitted",
                    "candidate_order_side": "buy",
                    "candidate_order_qty": 2.0,
                    "target_weight": 0.1,
                    "quote_time": "2026-04-17T14:31:00+00:00",
                    "reject_reasons": [],
                }
            },
            "monitoring_decision": {
                "status": "healthy",
                "next_action": "continue",
                "alerts": [],
                "diagnostics": {"submitted_order_count": 1, "blocked_order_count": 0},
            },
            "cycle_report": {
                "status": "healthy",
                "submitted_order_count": 1,
                "blocked_order_count": 0,
                "next_action": "continue",
            },
        },
    )

    app = create_app()
    app.dependency_overrides[get_container] = lambda: SimpleNamespace(
        bot_cycle_service=SimpleNamespace(db_session=session)
    )
    client = TestClient(app)

    response = client.get("/api/debug/cycle/latest")

    assert response.status_code == 200
    assert "Latest Persisted Bot Cycle" in response.text
    assert "AAPL" in response.text

    app.dependency_overrides.clear()
