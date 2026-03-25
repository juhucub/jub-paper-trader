#Trading strategy orchestrator and signal evaluation boundary

from dataclasses import dataclass

from agent_service.interfaces.signals import Signal, SignalProvider

@dataclass(slots=True)
class StrategyContext:
    symbol: str
    timeframe: str = "1Min"

class StrategyEngine:
    #coordinates signal providers to propose an action

    def __init__(self, signal_providers: SignalProvider) -> None:
        self.signal_providers = signal_providers
    
    def generate_signal(self, context: StrategyContext) -> Signal:
        #for now just use a single signal provider, but could be extended to aggregate multiple providers
        signal = self.signal_providers.get_signal(symbol=context.symbol, timeframe=context.timeframe)
        return signal