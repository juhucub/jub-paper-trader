from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from agent_service.exit_policy import ExitPolicy


@dataclass(slots=True)
class _Position:
    symbol: str
    qty: float
    avg_entry_price: float


def test_exit_policy_respects_min_holding_minutes_for_take_profit() -> None:
    policy = ExitPolicy(
        stop_loss_pct=0.04,
        take_profit_exit_pct=0.02,
        min_holding_minutes=15,
    )
    now = datetime(2026, 4, 6, 13, 0, tzinfo=timezone.utc)
    position = _Position(symbol="AAPL", qty=1.0, avg_entry_price=100.0)

    result = policy.evaluate_positions(
        positions=[position],
        latest_prices={"AAPL": 103.0},
        signals={"AAPL": {"direction": "long", "strength": 0.05}},
        previous_payload={"exit_policy_state": {}, "signals": {}},
        now_utc=now,
    )

    assert result["actions"]["AAPL"]["action"] == "HOLD"
    assert result["actions"]["AAPL"]["trigger"] == "none"
