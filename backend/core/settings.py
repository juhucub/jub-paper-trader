from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    env: str = "dev"
    debug: bool = True
    debug_bot_cycle: bool = True
    log_level: str = "INFO"

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    database_url: str = "sqlite:///./jub_paper_trader.db"

    alpaca_api_key: str = ""
    alpaca_api_secret: str = ""
    alpaca_base_url: str = "https://paper-api.alpaca.markets"
    alpaca_data_url: str = "https://data.alpaca.markets"

    bot_symbols: str = "AAPL,MSFT,NVDA,AMZN,GOOG,META,TSLA,JPM,GS,V,CAT,BA,XOM,CVX,WMT,COST,SBUX,KR,UNH,JNJ,LLY,DIS,NFLX,ADP,SFM"
    bot_cycle_interval_seconds: int = 60
    bot_use_structured_signals: bool = True
    bot_order_ttl_seconds: int = 180
    bot_order_replace_slippage_bps: float = 15.0
    bot_order_replace_price_band_bps: float = 75.0
    bot_order_replace_enabled: bool = True
    bot_max_quote_age_seconds: int = 1200
    bot_enforce_quote_freshness_only_during_trading_session: bool = True
    #llm_provider
    #llm_model
    #max_reasoning_tokens

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
