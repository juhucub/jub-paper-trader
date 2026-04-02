from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo


@dataclass(slots=True)
class DataQualityIssue:
    code: str
    message: str
    metadata: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "metadata": dict(self.metadata),
        }


@dataclass(slots=True)
class DataQualityConfig:
    min_bar_count: int = 20
    expected_bar_interval_seconds: int = 60
    max_quote_age_seconds: int = 1200
    max_price_jump_pct: float = 0.2
    enforce_quote_freshness_only_during_trading_session: bool = True
    enforce_bar_continuity_only_during_regular_session: bool = True


class MarketDataValidator:
    def __init__(self, config: DataQualityConfig | None = None):
        self.config = config or DataQualityConfig()

    def validate(
        self,
        symbol: str,
        bars: list[dict[str, Any]],
        quote: dict[str, Any],
        now_utc: datetime | None = None,
    ) -> list[DataQualityIssue]:
        now = now_utc or datetime.now(timezone.utc)
        issues: list[DataQualityIssue] = []

        if len(bars) < self.config.min_bar_count:
            issues.append(
                DataQualityIssue(
                    code="insufficient_bars",
                    message="Insufficient bars to compute stable features.",
                    metadata={"symbol": symbol, "bar_count": len(bars), "min_bar_count": self.config.min_bar_count},
                )
            )

        if not bars:
            issues.append(
                DataQualityIssue(
                    code="bar_completeness",
                    message="No bars returned.",
                    metadata={"symbol": symbol},
                )
            )
            return issues

        missing_close_count = sum(1 for bar in bars if bar.get("c") is None)
        if missing_close_count > 0:
            issues.append(
                DataQualityIssue(
                    code="bar_completeness",
                    message="Some bars are missing close values.",
                    metadata={"symbol": symbol, "missing_close_count": missing_close_count},
                )
            )

        bar_timestamps: list[datetime] = []
        for bar in bars:
            raw_timestamp = bar.get("t")
            if raw_timestamp is None:
                continue
            parsed_timestamp = self._parse_timestamp(raw_timestamp)
            if parsed_timestamp is None:
                issues.append(
                    DataQualityIssue(
                        code="invalid_timestamp",
                        message="Bar timestamp could not be parsed.",
                        metadata={"symbol": symbol, "field": "bar.t", "value": str(raw_timestamp)},
                    )
                )
                continue
            bar_timestamps.append(parsed_timestamp)

        if bar_timestamps:
            if any(left >= right for left, right in zip(bar_timestamps, bar_timestamps[1:])):
                issues.append(
                    DataQualityIssue(
                        code="non_monotonic_timestamps",
                        message="Bar timestamps are not strictly increasing.",
                        metadata={"symbol": symbol},
                    )
                )

            should_enforce_bar_continuity = True
            if self.config.enforce_bar_continuity_only_during_regular_session:
                should_enforce_bar_continuity = self._is_regular_trading_session(now)
            if should_enforce_bar_continuity:
                max_allowed_gap = int(self.config.expected_bar_interval_seconds * 1.5)
                for left, right in zip(bar_timestamps, bar_timestamps[1:]):
                    gap_seconds = int((right - left).total_seconds())
                    if gap_seconds > max_allowed_gap:
                        issues.append(
                            DataQualityIssue(
                                code="bar_gap",
                                message="Detected a large gap between consecutive bars.",
                                metadata={
                                    "symbol": symbol,
                                    "gap_seconds": gap_seconds,
                                    "max_allowed_gap_seconds": max_allowed_gap,
                                },
                            )
                        )
                        break

        closes = [float(bar.get("c", 0.0)) for bar in bars if bar.get("c") is not None]
        if any(price <= 0.0 for price in closes):
            issues.append(
                DataQualityIssue(
                    code="non_positive_price",
                    message="Detected non-positive close price.",
                    metadata={"symbol": symbol},
                )
            )

        for prev_close, next_close in zip(closes, closes[1:]):
            if prev_close <= 0:
                continue
            jump_pct = abs((next_close - prev_close) / prev_close)
            if jump_pct > self.config.max_price_jump_pct:
                issues.append(
                    DataQualityIssue(
                        code="price_outlier",
                        message="Detected abnormal jump in close prices.",
                        metadata={
                            "symbol": symbol,
                            "jump_pct": jump_pct,
                            "max_jump_pct": self.config.max_price_jump_pct,
                        },
                    )
                )
                break

        quote_time_raw = quote.get("t")
        quote_time = self._parse_timestamp(quote_time_raw)
        if quote_time_raw is not None and quote_time is None:
            issues.append(
                DataQualityIssue(
                    code="invalid_timestamp",
                    message="Quote timestamp could not be parsed.",
                    metadata={"symbol": symbol, "field": "quote.t", "value": str(quote_time_raw)},
                )
            )
        #FIXME: ENFORCE DATA FRESHNESS
        should_enforce_quote_freshness = True
        if self.config.enforce_quote_freshness_only_during_regular_session:
            should_enforce_quote_freshness = self._is_regular_trading_session(now)
        if quote_time is not None and should_enforce_quote_freshness:
            quote_age_seconds = int((now - quote_time).total_seconds())
            if quote_age_seconds > self.config.max_quote_age_seconds:
                issues.append(
                    DataQualityIssue(
                        code="stale_quote",
                        message="Latest quote is stale.",
                        metadata={
                            "symbol": symbol,
                            "quote_age_seconds": quote_age_seconds,
                            "max_quote_age_seconds": self.config.max_quote_age_seconds,
                        },
                    )
                )

        ask = quote.get("ap")
        bid = quote.get("bp")
        if ask is not None and float(ask) <= 0.0:
            issues.append(
                DataQualityIssue(
                    code="non_positive_price",
                    message="Ask price is non-positive.",
                    metadata={"symbol": symbol, "field": "ap"},
                )
            )
        if bid is not None and float(bid) <= 0.0:
            issues.append(
                DataQualityIssue(
                    code="non_positive_price",
                    message="Bid price is non-positive.",
                    metadata={"symbol": symbol, "field": "bp"},
                )
            )

        return issues

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        if isinstance(value, str):
            try:
                normalized = value.replace("Z", "+00:00")
                parsed = datetime.fromisoformat(normalized)
            except ValueError:
                return None
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        return None

    @staticmethod
    def _is_regular_trading_session(now_utc: datetime) -> bool:
        now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
        if now_et.weekday() >= 5:
            return False
        minutes_since_midnight = (now_et.hour * 60) + now_et.minute
        return (4 * 60) <= minutes_since_midnight < (20 * 60)

    @staticmethod
    def _is_regular_trading_session(now_utc: datetime) -> bool:
        now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
        if now_et.weekday() >= 5:
            return False
        minutes_since_midnight = (now_et.hour * 60) + now_et.minute
        return (9 * 60 + 30) <= minutes_since_midnight < (16 * 60)
