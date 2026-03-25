#portfolio engine for sizing and execution decisions

from dataclasses import dataclass

from services.alpaca_client import AlpacaClient
from services.risk_guardrails import RiskGuardrails

@dataclass(slots=True)
class PortfolioEngine:
    alpaca_client: AlpacaClient
    risk_guardrails: RiskGuardrails

    def execute_signal(self, symbol: str, qty: float, side: str) -> dict:
        # check risk guardrails
        if not self.risk_guardrails.allow_order(symbol=symbol, qty=qty, side=side):
            return {"status": "Blocked", "reason": "risk_guardrail_violaiton"}
        
        # submit order to alpaca
        order_response = self.alpaca_client.submit_order(symbol=symbol, qty=qty, side=side)
        return order_response