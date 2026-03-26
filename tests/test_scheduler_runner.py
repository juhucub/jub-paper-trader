from __future__ import annotations

from dataclasses import dataclass

import pytest

from scheduler.runner import parse_symbols, run_once


@dataclass(slots=True)
class FakeScheduler:
    def run_minute(self, symbols: list[str]) -> dict:
        return {
            "cycle_id": "cycle-1",
            "submitted_order_count": 1,
            "blocked_order_count": 0,
            "symbols": symbols,
        }


@dataclass(slots=True)
class FakeContainer:
    bot_scheduler: FakeScheduler


def test_parse_symbols_normalizes_and_validates():
    assert parse_symbols(" aapl, msft ,, nvda ") == ["AAPL", "MSFT", "NVDA"]

    with pytest.raises(ValueError):
        parse_symbols(" , , ")


def test_run_once_delegates_to_scheduler():
    container = FakeContainer(bot_scheduler=FakeScheduler())
    result = run_once(container=container, symbols=["AAPL"])

    assert result["cycle_id"] == "cycle-1"
    assert result["symbols"] == ["AAPL"]
