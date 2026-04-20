#Signals interfaces and DTOs for strategy components

from dataclasses import dataclass
from typing import Protocol

@dataclass(slots=True)
class Signal:
    symbol: str
    action: str # buy | sell | hold
    confidence: float
    rationale: str

class SignalProvider(Protocol):
    def get_signal(self, symbol: str, timeframe: str) -> Signal:
        #return a normalized signature for a symbol and timeframre
        ...