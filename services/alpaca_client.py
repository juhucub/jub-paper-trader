#Trading client wrapper for alpaca account apis

from dataclasses import dataclass

@dataclass(slots=True)
class AlpacaClient:
    api_key: str
    api_secret: str
    base_url: str

    def submit_order(self, symbol: str, qty: float, side: str, type: str) -> dict:
        # submit order placeholder
        return {
            "symbol": symbol,
            "qty": qty,
            "side": side,
            "type": type,
            "status": "stubbed"
        }