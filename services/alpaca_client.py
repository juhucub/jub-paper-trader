#Trading client wrapper for alpaca account apis

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import httpx

class AlpacaAPIError(RuntimeError):
    #Raise when Alpaca responds with API error or retries exhausted

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code

@dataclass(slots=True)
class AlpacaAccount:
    id: str
    status: str
    currency: str
    buying_power: float
    equity: float 

@dataclass(slots=True)
class AlpacaPosition:
    symbol: str
    qty: float
    side: str
    avg_entry_price: float | None
    current_price: float | None
    market_value: float | None
    unrealized_pl: float | None

@dataclass(slots=True)
class AlpacaOrder:
    id: str
    symbol: str
    qty: float
    side: str
    type: str
    time_in_force: str
    status: str
    created_at: str | None = None
    submitted_at: str | None = None
    limit_price: float | None = None
    filled_qty: float = 0.0
    filled_avg_price: float | None = None

@dataclass(slots=True)
class BuyingPowerEquity:
    buying_power: float
    equity: float

@dataclass(slots=True)
class AlpacaClient:
    api_key: str
    api_secret: str
    base_url: str = "https://paper-api.alpaca.markets/v2"
    timeout: float = 10.0
    max_retries: int = 2
    retry_delay_seconds: float = 0.1
    _sleep: Callable[[float], None] = field(default=lambda _: None, repr=False)
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

    def get_account(self) -> AlpacaAccount:
        payload = self._request_json("GET", "/v2/account")
        return AlpacaAccount(
            id=payload["id"],
            status=payload["status"],
            currency=payload.get("currency", "USD"),
            buying_power=_to_float(payload.get("buying_power"), field_name="buying_power"),
            equity=_to_float(payload.get("equity"), field_name="equity"),
        )

    def get_positions(self) -> list[AlpacaPosition]:
        payload = self._request_json("GET", "/v2/positions")
        return [
            AlpacaPosition(
                symbol=item["symbol"],
                qty=_to_float(item.get("qty"), field_name="qty"),
                side=item.get("side", "long"),
                avg_entry_price=_to_optional_float(item.get("avg_entry_price")),
                current_price=_to_optional_float(item.get("current_price")),
                market_value=_to_optional_float(item.get("market_value")),
                unrealized_pl=_to_optional_float(item.get("unrealized_pl")),
            )
            for item in payload
        ]

    def get_orders(
        self,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[AlpacaOrder]:
        params: dict[str, Any] = {}
        if status is not None:
            params["status"] = status
        if limit is not None:
            params["limit"] = limit

        payload = self._request_json("GET", "/v2/orders", params=params or None)
        return [
            AlpacaOrder(
                id=item["id"],
                symbol=item["symbol"],
                qty=_to_float(item.get("qty"), field_name="qty"),
                side=item["side"],
                type=item["type"],
                time_in_force=item["time_in_force"],
                status=item.get("status", "unknown"),
                created_at=item.get("created_at"),
                submitted_at=item.get("submitted_at"),
                limit_price=_to_optional_float(item.get("limit_price")),
                filled_qty=_to_optional_float(item.get("filled_qty")) or 0.0,
                filled_avg_price=_to_optional_float(item.get("filled_avg_price")),
            )
            for item in payload
        ]


    def submit_order(
        self,
        symbol: str,
        qty: float,
        side: str,
        type: str,
        time_in_force: str,
        trade_hour_type: str | None = None,
        limit_price: float | None = None,
        stop_price: float | None = None,
        client_order_id: str | None = None,
        order_class: str | None = None,
        **extra_fields: Any,
    ) -> AlpacaOrder:
        body: dict[str, Any] = {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": type,
            "time_in_force": time_in_force,
        }
        if limit_price is not None:
            body["limit_price"] = limit_price
        if stop_price is not None:
            body["stop_price"] = stop_price
        if client_order_id is not None:
            body["client_order_id"] = client_order_id
        if order_class is not None:
            body["order_class"] = order_class
        if trade_hour_type is not None:
            # keep trade-hour context available at the caller boundary without sending
            # unsupported fields to Alpaca's order endpoint.
            _ = trade_hour_type
        body.update(extra_fields)

        payload = self._request_json("POST", "/v2/orders", json_body=body)
        return AlpacaOrder(
            id=payload["id"],
            symbol=payload["symbol"],
            qty=_to_float(payload.get("qty"), field_name="qty"),
            side=payload["side"],
            type=payload["type"],
            time_in_force=payload["time_in_force"],
            status=payload.get("status", "unknown"),
            created_at=payload.get("created_at"),
            submitted_at=payload.get("submitted_at"),
            limit_price=_to_optional_float(payload.get("limit_price")),
            filled_qty=_to_optional_float(payload.get("filled_qty")) or 0.0,
            filled_avg_price=_to_optional_float(payload.get("filled_avg_price")),
        )

    def cancel_order(self, order_id: str) -> None:
        self._request_json("DELETE", f"/v2/orders/{order_id}")

    def close_position(self, symbol: str) -> AlpacaOrder:
        payload = self._request_json("DELETE", f"/v2/positions/{symbol}")
        return AlpacaOrder(
            id=payload["id"],
            symbol=payload["symbol"],
            qty=_to_float(payload.get("qty"), field_name="qty"),
            side=payload["side"],
            type=payload["type"],
            time_in_force=payload["time_in_force"],
            status=payload.get("status", "unknown"),
            created_at=payload.get("created_at"),
            submitted_at=payload.get("submitted_at"),
            limit_price=_to_optional_float(payload.get("limit_price")),
            filled_qty=_to_optional_float(payload.get("filled_qty")) or 0.0,
            filled_avg_price=_to_optional_float(payload.get("filled_avg_price")),
            
        )

    def get_buying_power_and_equity(self) -> BuyingPowerEquity:
        account = self.get_account()
        return BuyingPowerEquity(buying_power=account.buying_power, equity=account.equity)

    def _request_json(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        last_error: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.request(
                    method,
                    url,
                    headers=self._headers,
                    params=params,
                    json=json_body,
                )
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt < self.max_retries:
                    self._sleep(self.retry_delay_seconds)
                    continue
                raise AlpacaAPIError(f"Request failed after retries: {exc}") from exc

            if response.status_code in {429, 500, 502, 503, 504} and attempt < self.max_retries:
                last_error = AlpacaAPIError(
                    f"Transient Alpaca API error: {response.status_code}",
                    status_code=response.status_code,
                )
                self._sleep(self.retry_delay_seconds)
                continue

            if response.status_code >= 400:
                raise AlpacaAPIError(self._extract_error_message(response), status_code=response.status_code)

            if response.status_code == 204:
                return None
            return response.json()

        if last_error is not None:
            raise AlpacaAPIError(f"Request failed after retries: {last_error}") from last_error
        raise AlpacaAPIError("Request failed unexpectedly.")

    @staticmethod
    def _extract_error_message(response: httpx.Response) -> str:
        try:
            payload = response.json()
            if isinstance(payload, dict):
                return str(payload.get("message") or payload)
            return str(payload)
        except Exception:
            return f"HTTP {response.status_code} from Alpaca API"


def _to_float(value: Any, field_name: str) -> float:
    if value is None:
        raise ValueError(f"Missing required numeric field: {field_name}")
    return float(value)

def _to_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)

