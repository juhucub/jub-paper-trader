from datetime import datetime, timezone

from agent_service.data_quality import DataQualityConfig, MarketDataValidator


def test_validate_allows_delayed_quote_when_threshold_increased() -> None:
    validator = MarketDataValidator(DataQualityConfig(max_quote_age_seconds=1200))
    now = datetime(2026, 4, 2, 20, 15, 0, tzinfo=timezone.utc)
    quote = {"bp": 100.0, "ap": 100.1, "t": "2026-04-02T20:00:00Z"}
    bars = [
        {"t": "2026-04-02T19:46:00Z", "c": 100.0},
        {"t": "2026-04-02T19:47:00Z", "c": 100.1},
        {"t": "2026-04-02T19:48:00Z", "c": 100.2},
        {"t": "2026-04-02T19:49:00Z", "c": 100.2},
        {"t": "2026-04-02T19:50:00Z", "c": 100.3},
        {"t": "2026-04-02T19:51:00Z", "c": 100.2},
        {"t": "2026-04-02T19:52:00Z", "c": 100.3},
        {"t": "2026-04-02T19:53:00Z", "c": 100.4},
        {"t": "2026-04-02T19:54:00Z", "c": 100.4},
        {"t": "2026-04-02T19:55:00Z", "c": 100.5},
        {"t": "2026-04-02T19:56:00Z", "c": 100.6},
        {"t": "2026-04-02T19:57:00Z", "c": 100.7},
        {"t": "2026-04-02T19:58:00Z", "c": 100.6},
        {"t": "2026-04-02T19:59:00Z", "c": 100.7},
        {"t": "2026-04-02T20:00:00Z", "c": 100.8},
        {"t": "2026-04-02T20:01:00Z", "c": 100.8},
        {"t": "2026-04-02T20:02:00Z", "c": 100.9},
        {"t": "2026-04-02T20:03:00Z", "c": 101.0},
        {"t": "2026-04-02T20:04:00Z", "c": 101.0},
        {"t": "2026-04-02T20:05:00Z", "c": 101.1},
    ]

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote, now_utc=now)

    assert all(issue.code != "stale_quote" for issue in issues)


def test_validate_allows_stale_quote_when_aligned_with_latest_bar() -> None:
    validator = MarketDataValidator(DataQualityConfig(max_quote_age_seconds=180))
    now = datetime(2026, 4, 2, 20, 22, 0, tzinfo=timezone.utc)
    quote = {"bp": 100.0, "ap": 100.1, "t": "2026-04-02T20:00:00Z"}
    bars = [{"t": f"2026-04-02T19:{minute:02d}:00Z", "c": 100.0 + (minute * 0.01)} for minute in range(31, 60)]
    bars.append({"t": "2026-04-02T20:00:00Z", "c": 100.8})

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote, now_utc=now)

    assert all(issue.code != "stale_quote" for issue in issues)


def test_validate_skips_bar_gap_in_after_hours_by_default() -> None:
    validator = MarketDataValidator(DataQualityConfig(min_bar_count=2))
    now = datetime(2026, 4, 2, 20, 22, 0, tzinfo=timezone.utc)  # 4:22pm ET (after-hours)
    quote = {"bp": 100.0, "ap": 100.1, "t": "2026-04-02T20:22:00Z"}
    bars = [
        {"t": "2026-04-02T20:00:00Z", "c": 100.0},
        {"t": "2026-04-02T20:10:00Z", "c": 100.1},
    ]

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote, now_utc=now)

    assert all(issue.code != "bar_gap" for issue in issues)
