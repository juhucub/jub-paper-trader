#dependency contrainer wiring for API <3

from dataclasses import dataclass

from backend.core.settings import Settings, get_settings
from db.base import SessionLocal

from services.alpaca_client import AlpacaClient
from services.alpaca_data import AlpacaDataClient
from services.portfolio_engine import PortfolioEngine
from services.risk_guardrails import RiskGuardrails

@dataclass(slots=True)
class AppContainer:
    settings: Settings
    alpaca_client: AlpacaClient
    alpaca_data_client: AlpacaDataClient
    risk_guardrails: RiskGuardrails
    portfolio_engine: PortfolioEngine

def build_container() -> AppContainer:
    settings = get_settings()
    alpaca_client = AlpacaClient(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        base_url=settings.alpaca_base_url,
    )
    alpaca_data_client = AlpacaDataClient(
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        data_url=settings.alpaca_data_url,
    )
    risk_guardrails = RiskGuardrails()
    portfolio_engine = PortfolioEngine( 
        alpaca_client=alpaca_client, 
        risk_guardrails=risk_guardrails,
        db_session=SessionLocal
    )

    return AppContainer(
        settings=settings,
        alpaca_client=alpaca_client,
        alpaca_data_client=alpaca_data_client,
        risk_guardrails=risk_guardrails,
        portfolio_engine=portfolio_engine
    )

def get_container() -> AppContainer:
    return build_container()

