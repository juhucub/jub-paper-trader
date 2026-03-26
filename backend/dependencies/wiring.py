#dependency contrainer wiring for API <3

from dataclasses import dataclass

from agent_service.bot_cycle import BotCycleService
from agent_service.optimizer_qpo import OptimizerQPO
from agent_service.strategy import StrategyDefinition, StrategyRegistry
from backend.core.settings import Settings, get_settings
from db.base import SessionLocal

from scheduler.cycle import BotScheduler
from services.alpaca_client import AlpacaClient
from services.alpaca_data import AlpacaDataClient
from services.execution_router import ExecutionRouter
from services.portfolio_engine import PortfolioEngine
from services.risk_guardrails import RiskGuardrails

@dataclass(slots=True)
class AppContainer:
    settings: Settings
    alpaca_client: AlpacaClient
    alpaca_data_client: AlpacaDataClient
    risk_guardrails: RiskGuardrails
    portfolio_engine: PortfolioEngine
    optimizer: OptimizerQPO
    execution_router: ExecutionRouter
    bot_cycle_service: BotCycleService
    bot_scheduler: BotScheduler
    strategy_registry: StrategyRegistry

def build_container() -> AppContainer:
    settings = get_settings()
    db_session = SessionLocal()

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
        db_session=db_session
    )
    optimizer = OptimizerQPO()
    execution_router = ExecutionRouter()
    bot_cycle_service = BotCycleService(
        alpaca_data_client=alpaca_data_client,
        alpaca_client=alpaca_client,
        risk_guardrails=risk_guardrails,
        portfolio_engine=portfolio_engine,
        optimizer=optimizer,
        execution_router=execution_router,
        db_session=db_session,
    )
    bot_scheduler = BotScheduler(orchestration_service=bot_cycle_service)

    strategy_registry = StrategyRegistry()
    strategy_registry.register(
        StrategyDefinition(name="v1_momentum_mean_reversion", signal_modes=("momentum", "mean_reversion"), benchmark_symbol="SPY")
    )
    
    return AppContainer(
        settings=settings,
        alpaca_client=alpaca_client,
        alpaca_data_client=alpaca_data_client,
        risk_guardrails=risk_guardrails,
        portfolio_engine=portfolio_engine,
        optimizer=optimizer,
        execution_router=execution_router,
        bot_cycle_service=bot_cycle_service,
        bot_scheduler=bot_scheduler,
        strategy_registry=strategy_registry,
    )

def get_container() -> AppContainer:
    return build_container()

