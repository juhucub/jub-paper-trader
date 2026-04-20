#Step 2) minute by minute scheduler for bot cycles

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from agent_service.bot_cycle import BotCycleService
from agent_service.interfaces import CycleReport

@dataclass(slots=True)
class BotScheduler: 
    orchestration_service: BotCycleService
    
    def run_minute(self, symbols: list[str]) -> dict[str, Any]:
        #run a single bot cycle for a given symbol
        result = self.orchestration_service.run_cycle(symbols)
        cycle_report = result.get("cycle_report")
        if isinstance(cycle_report, CycleReport):
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "symbols": symbols,
                "cycle_id": cycle_report.cycle_id,
                "submitted_order_count": cycle_report.submitted_order_count,
                "blocked_order_count": cycle_report.blocked_order_count,
                "status": cycle_report.status,
                "summary": cycle_report.summary,
                "next_action": cycle_report.next_action,
                "cycle_report": cycle_report,
            }

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbols": symbols,
            "cycle_id": result["cycle_id"],
            "submitted_order_count": len(result["submitted_orders"]),
            "blocked_order_count": len(result["blocked_orders"]),
        }
