from __future__ import annotations

import httpx
import pytest

from services.alpaca_client import AlpacaAPIError, AlpacaClient


class FakeResponse:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise ValueError("No payload")
        return self._payload


class FakeHttpClient:
    def __init__(self, responses=None, errors=None):
        self.responses = list(responses or [])
        self.errors = list(errors or [])
        self.requests = []

    def request(self, method, url, headers=None, params=None, json=None):
        self.requests.append(
            {
                "method": method,
                "url": url,
                "headers": headers,
                "params": params,
                "json": json,
            }
        )
        if self.errors:
            raise self.errors.pop(0)
        return self.responses.pop(0)

    def close(self):
        return None


def _build_client(fake_http: FakeHttpClient) -> AlpacaClient:
    client = AlpacaClient(api_key="key", api_secret="secret", max_retries=2, retry_delay_seconds=0)
    client._client = fake_http  # type: ignore[assignment]
    return client


def test_get_orders_builds_query_params_and_maps_response():
    fake_http = FakeHttpClient(
        responses=[
            FakeResponse(
                200,
                [
                    {
                        "id": "ord-1",
                        "symbol": "AAPL",
                        "qty": "2",
                        "side": "buy",
                        "type": "market",
                        "time_in_force": "day",
                        "status": "filled",
                    }
                ],
            )
        ]
    )
    client = _build_client(fake_http)

    orders = client.get_orders(status="open", limit=25)

    assert len(orders) == 1
    assert orders[0].qty == 2.0
    assert fake_http.requests[0]["params"] == {"status": "open", "limit": 25}


def test_submit_order_builds_body_and_maps_response():
    fake_http = FakeHttpClient(
        responses=[
            FakeResponse(
                200,
                {
                    "id": "ord-2",
                    "symbol": "NVDA",
                    "qty": "3",
                    "side": "buy",
                    "type": "limit",
                    "time_in_force": "gtc",
                    "status": "new",
                },
            )
        ]
    )
    client = _build_client(fake_http)

    order = client.submit_order(
        symbol="NVDA",
        qty=3,
        side="buy",
        type="limit",
        time_in_force="gtc",
        limit_price=900.5,
    )

    assert order.id == "ord-2"
    assert order.qty == 3.0
    assert fake_http.requests[0]["json"] == {
        "symbol": "NVDA",
        "qty": 3,
        "side": "buy",
        "type": "limit",
        "time_in_force": "gtc",
        "limit_price": 900.5,
    }


def test_get_buying_power_and_equity_maps_account_fields():
    fake_http = FakeHttpClient(
        responses=[
            FakeResponse(
                200,
                {
                    "id": "acct-1",
                    "status": "ACTIVE",
                    "currency": "USD",
                    "buying_power": "25000.50",
                    "equity": "26000.75",
                },
            )
        ]
    )
    client = _build_client(fake_http)

    summary = client.get_buying_power_and_equity()

    assert summary.buying_power == 25000.5
    assert summary.equity == 26000.75


def test_retries_transient_http_errors_then_succeeds():
    fake_http = FakeHttpClient(
        responses=[
            FakeResponse(
                200,
                {
                    "id": "acct-1",
                    "status": "ACTIVE",
                    "currency": "USD",
                    "buying_power": "1000",
                    "equity": "1200",
                },
            )
        ],
        errors=[httpx.ReadTimeout("boom")],
    )
    client = _build_client(fake_http)

    account = client.get_account()

    assert account.id == "acct-1"
    assert len(fake_http.requests) == 2


def test_raises_api_error_on_non_retryable_error():
    fake_http = FakeHttpClient(responses=[FakeResponse(403, {"message": "forbidden"})])
    client = _build_client(fake_http)

    with pytest.raises(AlpacaAPIError) as exc:
        client.get_positions()

    assert exc.value.status_code == 403
    assert "forbidden" in str(exc.value)
