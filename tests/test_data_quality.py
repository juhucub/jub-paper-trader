from __future__ import annotations

from datetime import datetime, timezone

from agent_service.data_quality import MarketDataValidator


def test_data_quality_handles_invalid_timestamps_without_crashing():
    validator = MarketDataValidator()
    bars = [
        {"t": "2026-01-01T10:00:00Z", "c": 100.0, "v": 1000},
        {"t": "not-a-timestamp", "c": 101.0, "v": 1100},
    ]
    quote = {"t": "also-not-a-timestamp", "ap": 101.2, "bp": 101.0}

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote)
    codes = {issue.code for issue in issues}

    assert "invalid_timestamp" in codes


def test_data_quality_does_not_enforce_stale_quote_in_after_hours():
    validator = MarketDataValidator()
    bars = [{"t": "2026-04-02T20:39:00Z", "c": 100.0, "v": 1000} for _ in range(30)]
    quote = {"t": "2026-04-02T20:00:00Z", "ap": 101.2, "bp": 101.0}

    # 2026-04-02T20:42:00Z == 4:42pm ET (after-hours).
    issues = validator.validate(
        symbol="AAPL",
        bars=bars,
        quote=quote,
        now_utc=datetime(2026, 4, 2, 20, 42, tzinfo=timezone.utc),
    )
    codes = {issue.code for issue in issues}

    assert "stale_quote" not in codes
