#Minute by minute scheduler for bot cycles

from datetime import datetime, timezone

from agent_service.strategy import StrategyContext, StrategyEngine

class BotScheduler: 
    def __init__(self, strategy_engine: StrategyEngine) -> None:
        self.strategy_engine = strategy_engine
    
    def run_cycle(self, symbol: str) -> dict:
        #run a single bot cycle for a given symbol
        context = StrategyContext(symbol=symbol)
        signal = self.strategy_engine.generate_signal(context)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "signal": signal.action,
            "confidence": signal.confidence,
        }