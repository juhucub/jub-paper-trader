from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.base import Base
from db.models.snapshots import MarketDataSnapshot
from db.repositories.snapshots import upsert_market_data_snapshot
from services.alpaca_data import AlpacaDataClient


class FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class FakeHttpClient:
    def get(self, url: str, headers=None, params=None):
        if url.endswith("/v2/stocks/AAPL/bars"):
            assert params["limit"] == 50
            assert params["timeframe"] == "1Min"
            return FakeResponse(
                {
                    "bars": [{"t": f"2026-03-24T14:{i:02d}:00Z", "c": 100 + i} for i in range(50)]
                }
            )

        if url.endswith("/v2/stocks/NVDA/quotes/latest"):
            return FakeResponse({"quote": {"bp": 900.10, "ap": 900.20, "t": "2026-03-24T14:30:00Z"}})

        if url.endswith("/v2/stocks/NVDA/trades/latest"):
            return FakeResponse({"trade": {"p": 900.15, "s": 10, "t": "2026-03-24T14:30:01Z"}})

        raise AssertionError(f"Unexpected URL called: {url}")

    def close(self) -> None:
        return None


def _build_client() -> AlpacaDataClient:
    client = AlpacaDataClient(api_key="key", api_secret="secret")
    client._client = FakeHttpClient()  # type: ignore[assignment]
    return client


def test_get_historical_bars_fetches_last_50_for_aapl():
    client = _build_client()

    bars = client.get_historical_bars(symbol="AAPL", timeframe="1Min", limit=50)

    assert len(bars) == 50
    assert bars[0]["c"] == 100
    assert bars[-1]["c"] == 149


def test_get_latest_quote_and_trade_for_nvda():
    client = _build_client()

    quote = client.get_latest_quote("NVDA")
    trade = client.get_latest_trade("NVDA")

    assert quote["bp"] == 900.10
    assert quote["ap"] == 900.20
    assert trade["p"] == 900.15
    assert trade["s"] == 10


def test_store_snapshot_persists_into_model_repo_interface():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, future=True)()

    client = _build_client()
    quote_payload = {"quote": {"bp": 900.10, "ap": 900.20, "t": "2026-03-24T14:30:00Z"}}

    first = client.store_snapshot("NVDA", quote_payload, session)
    assert first.id is not None

    updated_payload = {"quote": {"bp": 901.10, "ap": 901.20, "t": "2026-03-24T14:30:00Z"}}
    second = client.store_snapshot("NVDA", updated_payload, session)

    rows = session.query(MarketDataSnapshot).all()
    assert len(rows) == 1
    assert rows[0].payload["bp"] == 901.10
    assert second.id == first.id


def test_repository_uses_postgres_on_conflict_for_upsert_path():
    mock_session = MagicMock()
    mock_session.bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    mock_session.execute.return_value.scalar_one.return_value = MarketDataSnapshot(
        symbol="NVDA",
        snapshot_type="trade",
        source_timestamp=datetime(2026, 3, 24, 14, 31, tzinfo=timezone.utc),
        payload={"p": 900.15},
    )

    _ = upsert_market_data_snapshot(
        db_session=mock_session,
        symbol="NVDA",
        snapshot_type="trade",
        payload={"p": 900.15},
        source_timestamp=datetime(2026, 3, 24, 14, 31, tzinfo=timezone.utc),
    )

    executed_stmt = mock_session.execute.call_args[0][0]
    assert "ON CONFLICT" in str(executed_stmt)
    mock_session.commit.assert_called_once()
