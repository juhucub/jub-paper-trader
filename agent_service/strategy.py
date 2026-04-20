#Trading strategy orchestrator and signal evaluation boundary

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Protocol

from agent_service.interfaces import ReplayEvaluation
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
    
class BacktestHook(Protocol):
    def run(self, strategy_name: str, symbols: list[str], benchmark_symbol: str) -> ReplayEvaluation | dict[str, Any]:
        ...


@dataclass(slots=True)
class StrategyDefinition:
    name: str
    signal_modes: tuple[str, ...]
    benchmark_symbol: str = "SPY"
    backtest_hook: BacktestHook | None = None


@dataclass(slots=True)
class StrategyRegistry:
    _registry: dict[str, StrategyDefinition] = field(default_factory=dict)

    def register(self, definition: StrategyDefinition) -> None:
        self._registry[definition.name] = definition

    def get(self, strategy_name: str) -> StrategyDefinition:
        if strategy_name not in self._registry:
            raise KeyError(f"Unknown strategy: {strategy_name}")
        return self._registry[strategy_name]

    def list(self) -> list[StrategyDefinition]:
        return list(self._registry.values())

    def run_backtest(self, strategy_name: str, symbols: list[str]) -> ReplayEvaluation | dict[str, Any]:
        definition = self.get(strategy_name)
        if definition.backtest_hook is None:
            return {
                "strategy": strategy_name,
                "benchmark_symbol": definition.benchmark_symbol,
                "status": "missing_backtest_hook",
            }
        return definition.backtest_hook.run(strategy_name, symbols, definition.benchmark_symbol)
