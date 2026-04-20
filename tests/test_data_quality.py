from datetime import datetime, timezone

from agent_service.data_quality import DataQualityConfig, MarketDataValidator


def _bars(start_minute: int, end_minute: int, hour: int = 15) -> list[dict[str, float | str]]:
    return [{"t": f"2026-04-02T{hour:02d}:{minute:02d}:00Z", "c": 100.0 + (minute * 0.01)} for minute in range(start_minute, end_minute + 1)]


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
    validator = MarketDataValidator(DataQualityConfig(max_bar_age_seconds=300, max_quote_age_seconds=180))
    now = datetime(2026, 4, 2, 15, 22, 0, tzinfo=timezone.utc)
    quote = {"bp": 100.0, "ap": 100.1, "t": "2026-04-02T15:18:00Z"}
    bars = _bars(49, 59, hour=14) + _bars(0, 18, hour=15)

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote, now_utc=now)

    assert all(issue.code != "stale_quote" for issue in issues)
    assert all(issue.code != "quote_bar_misalignment" for issue in issues)
    assert all(issue.code != "stale_bar" for issue in issues)


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


def test_validate_flags_stale_quote_when_not_aligned_with_latest_bar() -> None:
    validator = MarketDataValidator(DataQualityConfig(max_quote_age_seconds=180))
    now = datetime(2026, 4, 2, 15, 22, 0, tzinfo=timezone.utc)
    quote = {"bp": 100.0, "ap": 100.1, "t": "2026-04-02T15:17:00Z"}
    bars = _bars(49, 59, hour=14) + _bars(0, 18, hour=15)

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote, now_utc=now)

    assert any(issue.code == "stale_quote" for issue in issues)
    assert any(issue.code == "quote_bar_misalignment" for issue in issues)


def test_validate_flags_stale_bar_during_regular_session() -> None:
    validator = MarketDataValidator(DataQualityConfig(min_bar_count=2, max_bar_age_seconds=180))
    now = datetime(2026, 4, 2, 15, 22, 0, tzinfo=timezone.utc)
    quote = {"bp": 100.0, "ap": 100.1, "t": "2026-04-02T15:16:00Z"}
    bars = [
        {"t": "2026-04-02T15:15:00Z", "c": 100.0},
        {"t": "2026-04-02T15:16:00Z", "c": 100.1},
    ]

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote, now_utc=now)

    assert any(issue.code == "stale_bar" for issue in issues)


def test_validate_flags_invalid_quote_timestamp_in_future() -> None:
    validator = MarketDataValidator()
    now = datetime(2026, 4, 2, 15, 22, 0, tzinfo=timezone.utc)
    quote = {"bp": 100.0, "ap": 100.1, "t": "2026-04-02T15:23:00Z"}
    bars = _bars(49, 59, hour=14) + _bars(0, 18, hour=15)

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote, now_utc=now)

    assert any(issue.code == "invalid_timestamp" and issue.metadata.get("field") == "quote.t" for issue in issues)


def test_validate_flags_missing_bar_timestamp_during_regular_session() -> None:
    validator = MarketDataValidator(DataQualityConfig(min_bar_count=2))
    now = datetime(2026, 4, 2, 15, 22, 0, tzinfo=timezone.utc)
    quote = {"bp": 100.0, "ap": 100.1, "t": "2026-04-02T15:22:00Z"}
    bars = [
        {"c": 100.0},
        {"c": 100.1},
    ]

    issues = validator.validate(symbol="AAPL", bars=bars, quote=quote, now_utc=now)

    invalid_timestamp_fields = [issue.metadata.get("field") for issue in issues if issue.code == "invalid_timestamp"]
    assert invalid_timestamp_fields.count("bar.t") >= 2
