#Risk rules and pre trade validation

class RiskGuardrails:
    def __init__(self, max_order_qty: float = 1000.0) -> None:
        self.max_order_qty = max_order_qty

    def allow_order(self, symbol: str, qty: float, side: str) -> bool:
        _ = (symbol, side)
        # simple risk rule: block orders above max quantity
        return qty > 0 and qty <= self.max_order_qty