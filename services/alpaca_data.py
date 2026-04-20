#Market data wrapper for alpaca historical / real time feeds

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy.orm import Session
from db.models.snapshots import utc_now

from db.repositories.snapshots import upsert_market_data_snapshot

""" DATA API
ap - ask price
bp - bid price
as - ask size
bs - bid size
ax - ask exchange
bx - bid exchange
c - conditions
t - timestamp
z - tape (venue code)
"""

"""
For each stock i:
    signal_i = f(features_i)
"""
@dataclass(slots=True)
class AlpacaDataClient:
    api_key: str
    api_secret: str
    data_url: str = "https://data.alpaca.markets"
    trading_url: str = "https://api.alpaca.markets"
    timeout: float = 10.0
    _client: httpx.Client = field(init=False, repr=False)
    _headers: dict[str, str] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._client = httpx.Client(timeout=self.timeout)
        self._headers = {
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.api_secret,
        }

    def close(self) -> None:
        self._client.close()
        
    def get_latest_quote(self, symbol: str) -> dict[str, Any]:
        response = self._client.get(
            f"{self.data_url}/v2/stocks/{symbol}/quotes/latest",
            headers=self._headers,
        )
        response.raise_for_status()
        quote = dict(response.json()["quote"])
        bid = float(quote.get("bp") or 0.0)
        ask = float(quote.get("ap") or 0.0)
        if bid > 0.0 and ask > 0.0:
            return quote

        trade = self.get_latest_trade(symbol)
        trade_price = float(trade.get("p") or 0.0)
        if trade_price <= 0.0:
            return quote

        quote.setdefault("bp", trade_price)
        quote.setdefault("ap", trade_price)
        if float(quote.get("bp") or 0.0) <= 0.0:
            quote["bp"] = trade_price
        if float(quote.get("ap") or 0.0) <= 0.0:
            quote["ap"] = trade_price
        quote.setdefault("t", trade.get("t"))
        return quote

    def get_latest_trade(self, symbol: str) -> dict[str, Any]:
        response = self._client.get(
            f"{self.data_url}/v2/stocks/{symbol}/trades/latest",
            headers=self._headers,
        )
        response.raise_for_status()
        return response.json()["trade"]

    def get_historical_bars(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "timeframe": timeframe,
            "limit": limit,
        }
        if start:
            params["start"] = start
        if end:
            params["end"] = end

        response = self._client.get(
            f"{self.data_url}/v2/stocks/{symbol}/bars",
            headers=self._headers,
            params=params,
        )
        response.raise_for_status()

        data = response.json()
        return data.get("bars") or []

    def get_market_clock(self) -> dict[str, Any]:
        response = self._client.get(f"{self.trading_url}/v2/clock", headers=self._headers)
        response.raise_for_status()
        return response.json()

    def store_snapshot(
        self,
        symbol: str,
        quote_or_trade_payload: dict[str, Any],
        db_session: Session,
    ):
        snapshot_type, payload = self._normalize_snapshot_payload(quote_or_trade_payload)
        source_timestamp = self._extract_timestamp(payload)

        return upsert_market_data_snapshot(
            db_session=db_session,
            symbol=symbol,
            snapshot_type=snapshot_type,
            payload=payload,
            source_timestamp=source_timestamp,
        )

    @staticmethod
    def _normalize_snapshot_payload(quote_or_trade_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        if "quote" in quote_or_trade_payload:
            return "quote", quote_or_trade_payload["quote"]
        if "trade" in quote_or_trade_payload:
            return "trade", quote_or_trade_payload["trade"]

        payload_keys = set(quote_or_trade_payload.keys())
        if {"bp", "ap"}.issubset(payload_keys):
            return "quote", quote_or_trade_payload
        if "p" in payload_keys:
            return "trade", quote_or_trade_payload

        raise ValueError("Unable to infer snapshot type from payload. Expected quote or trade payload.")

    @staticmethod
    def _extract_timestamp(payload: dict[str, Any]) -> datetime:
        raw_timestamp = payload.get("t")
        if raw_timestamp is None:
            return utc_now

        normalized = raw_timestamp.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
