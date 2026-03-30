#Step 2) minute by minute scheduler for bot cycles

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from agent_service.bot_cycle import BotCycleService

@dataclass(slots=True)
class BotScheduler: 
    orchestration_service: BotCycleService
    
    def run_minute(self, symbols: list[str]) -> dict[str, Any]:
        #run a single bot cycle for a given symbol
        result = self.orchestration_service.run_cycle(symbols)
        
        #Return broker summary for cycle_id
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbols": symbols,
            "cycle_id": result["cycle_id"],
            "submitted_order_count": len(result["submitted_orders"]),
            "blocked_order_count": len(result["blocked_orders"]),
        }