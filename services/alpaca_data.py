#Market data wrapper for alpaca historical / real time feeds

from dataclasses import dataclass

@dataclass(slots=True)
class AlpacaDataClient:
    api_key: str
    api_secret: str
    data_url: str

    def get_latest_bar(self, symbol: str) -> dict:
        # get latest bar placeholder
        return {
            "symbol": symbol,
            "open": 0.0,
            "high": 0.0,
            "low": 0.0,
            "close": 0.0,
            "volume": 1000
        }