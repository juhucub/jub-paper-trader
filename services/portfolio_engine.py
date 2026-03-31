#portfolio engine for account sync, fill application, and portfolio analytics

#1. Sync broker/account snapshots into local DB state
#2. Apply fills as they come in
#3. Recalculate portfolio equity / PnL / exposure metrix
#4. Gate signal execution through risk guardrails before routing to Alpaca API
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from db.models.orders import Order
from db.models.portfolio import PortfolioAccountState, TradeHistory
from db.models.positions import Position

from services.alpaca_client import AlpacaClient
from services.risk_guardrails import RiskGuardrails

@dataclass(slots=True)
class PortfolioEngine:
    alpaca_client: AlpacaClient
    
    #Centralized risk-validation layer used before execution 
    risk_guardrails: RiskGuardrails

    #Shared SQLAlchemy session for all DB R/W in our engine
    db_session: Session

    def _get_account_state(self) -> PortfolioAccountState:
        """
        Keep one account state row, if DNE lazily bootstrap

        GUARENTEES all major portfolio calculations operate on 
        our persistent account records. 
        """
        account = self.db_session.get(PortfolioAccountState, 1)
        if account: 
            return account
    
        #Bootstrap
        account = PortfolioAccountState(id=1)

        self.db_session.add(account)
        self.db_session.flush()

        return account
    
    def sync_account_state(
        self, 
        account: dict[str, Any],
        positions: list[dict[str, Any]],
        orders: list[dict[str, Any]]
    ) -> dict[str, float]:
        """
        Sync broker snapshots from Alpaca into local + recompile analytics

        Expected Input:
            -account: broker account state (cash/ equity/buying power)
            -positions: current open/known positions
            -orders: current broker order states

        """
        state = self._get_account_state()
        #Alpaca has historically been inconsistent with cash vs buying power field presence in their API responses, so we check both and default to existing state if neither are present
        state.cash = float(account.get("cash", account.get("buying_power", state.cash)))
        
        #Equity is the broker-reported total account equity
        state.equity = float(account.get("equity", state.equity))

        #Maintain peak equity and drawdown metrics over time
        state.peak_equity = max(state.peak_equity, state.equity)
        state.current_drawdown = max(0.0, state.peak_equity - state.equity)
        state.max_drawdown = max(state.max_drawdown, state.current_drawdown)
        state.updated_at = datetime.now(timezone.utc)

         #Upsert orders first so we can track incremental fills (including partial fills) safely.
        for item in orders:
            broker_id = item.get("id")
            existing = None

            if broker_id:
                existing = self.db_session.execute(
                    select(Order).where(Order.broker_owner_id == broker_id)
                ).scalar_one_or_none()

            order = existing or Order(
                broker_owner_id=broker_id,
                symbol=str(item["symbol"]).upper(),
                side=str(item.get("side", "buy")).lower(),
                quantity=float(item.get("qty", item.get("quantity", 0))),
            )

            prev_filled_qty = float(order.filled_quantity or 0.0)
            order.status = str(item.get("status", order.status))
            order.quantity = float(item.get("qty", item.get("quantity", order.quantity)))

            filled_qty_raw = item.get("filled_qty", item.get("filled_quantity"))
            if filled_qty_raw is None:
                new_filled_qty = prev_filled_qty
            else:
                new_filled_qty = float(filled_qty_raw)
            fill_delta = max(new_filled_qty - prev_filled_qty, 0.0)

            filled_avg_raw = item.get("filled_avg_price")
            filled_avg_price = float(filled_avg_raw) if filled_avg_raw not in (None, "") else order.filled_avg_price

            order.filled_quantity = new_filled_qty
            order.filled_avg_price = filled_avg_price
            order.updated_at = datetime.now(timezone.utc)

            if not existing:
                self.db_session.add(order)

            if fill_delta > 0 and filled_avg_price is not None:
                self._record_fill_event(
                    symbol=order.symbol,
                    side=order.side,
                    qty=fill_delta,
                    price=float(filled_avg_price),
                )

        #Upsert positions based on broker snapshot in local
        for item in positions:
            symbol = str(item["symbol"]).upper() #Normalize

            position = self.db_session.execute(
                select(Position).where(Position.symbol == symbol)
            ).scalar_one_or_none()

            #Create new position if DNE
            if not position:
                position = Position(symbol=symbol)
                self.db_session.add(position)

            #qty/avg/last are pulled from the broker snapshot
            qty = float(item.get("qty", item.get("quantity", position.quantity)))
            avg = float(item.get("avg_entry_price",  position.avg_entry_price))
            last = float(item.get("current_price", item.get("last_price", position.last_price or avg)))

            #Persist normalized portfolio state
            position.quantity = qty
            position.avg_entry_price = avg
            position.cost_basis = qty * avg
            position.last_price = last
            position.unrealized_pnl = (last - avg) * qty

            #Mark the position closed w/ a timestamp
            position.closed_at = datetime.now(timezone.utc) if qty == 0 else None
            position.updated_at = datetime.now(timezone.utc)

       
        #Recompute derived analytics after syncing state
        self.recalculate_equity()
        exposure = self.compute_exposure()

        #Commit the sync as one transaction
        self.db_session.commit()
        return exposure
    
    def _record_fill_event(self, symbol: str, side: str, qty: float, price: float) -> None:
        if qty <= 0 or price <= 0:
            return
        trade = TradeHistory(
            symbol=symbol,
            side=side,
            quantity=qty,
            price=price,
            realized_pnl=0.0,
            order_id=None,
        )
        self.db_session.add(trade)
        
    def apply_fill(self, fill_event: dict[str, Any]) -> dict[str, float]:
        """
        Apply a single execution / fill event to local portfolio state

        (Event driven update path)
        Typical Jobs:
            -Updated the affected position
            -Realize PnL when selling against an existing long
            -Update cash and daily realized PnL
            -Write trade history
            -Re-mark portfolio and recompute exposure/equity
        """
    
        symbol = str(fill_event["symbol"]).upper()
        side = str(fill_event["side"]).lower()
        qty = float(fill_event["qty"])
        price = float(fill_event["price"])
        order_id = fill_event.get("order_id")

        state = self._get_account_state()

        position = self.db_session.execute(
            select(Position).where(Position.symbol == symbol)
        ).scalar_one_or_none()

        #If first time seeing this symbol, create 
        if not position:
            position = Position(symbol=symbol)
            self.db_session.add(position)
            self.db_session.flush()

        #Buys add to position size, sells reduce it
        signed_qty = qty if side == "buy" else -qty

        prev_qty = position.quantity
        prev_avg = position.avg_entry_price

        #Realized_incremenet only applies when reducing/closing an existing long
        #(long-only)
        realized_increment = 0.0
        if prev_qty > 0 and signed_qty < 0:
            close_qty = min(prev_qty, abs(signed_qty))
            realized_increment = (price - prev_avg) * close_qty

        new_qty = prev_qty + signed_qty

        if new_qty > 0:
            #Recalculate weighted average entry only when adding to a long
            #Selling does not change avg basis for remaining shares
            if signed_qty > 0:
                position.avg_entry_price = (
                    (prev_qty * prev_avg) + (qty * price)) / new_qty if new_qty else 0.0
        else:
            #FIXME: If fully flat or net short, reset avg price
            position.avg_entry_price = 0.0

        position.quantity = new_qty
        position.cost_basis = max(new_qty, 0.0) * position.avg_entry_price
        position.last_price = price
        position.realized_pnl += realized_increment
        position.unrealized_pnl = (price - position.avg_entry_price) * max(new_qty, 0.0)
        position.closed_at = datetime.now(timezone.utc) if new_qty == 0 else None
        position.updated_at = datetime.now(timezone.utc)

        #cash decreases on buys and increases on sells
        cash_delta = -(qty * price) if side == "buy" else (qty * price)
        state.cash += cash_delta
        state.realized_pnl += realized_increment

#       #Reset daily realized PnL accumulator when trading day changes
        if state.daily_date != datetime.now(timezone.utc).date():
            state.daily_date = datetime.now(timezone.utc).date()
            state.daily_realized_pnl = 0.0
        state.daily_realized_pnl += realized_increment

        #Auditor
        trade = TradeHistory(
            symbol=symbol,
            side=side,
            quantity=qty,
            price=price,
            realized_pnl=realized_increment,
            order_id=int(order_id) if order_id is not None else None,
        )
        self.db_session.add(trade)

        #Remark only affected symbol w/ new trade price
        self.mark_to_market({symbol: price})

        #Recompute higher level analytics like
        equity = self.recalculate_equity()
        exposure = self.compute_exposure()

        self.db_session.commit()
        return {"equity": equity, **exposure, "realized_pnl": state.realized_pnl}
   
    def mark_to_market(self, latest_prices: dict[str, float]) -> float:
        """
        Update unrealized PnL based on latest market prices

        Expected Input:
            -latest_prices maps symbol -> latest trade/mark price

        Returns:
            -total_unrealized PnL across all tracked positions
        """
        total_unrealized = 0.0

        #Update only the symbols for which we receive fresh prices
        for symbol, latest in latest_prices.items():
            pos = self.db_session.execute(
                select(Position).where(Position.symbol == symbol.upper())
            ).scalar_one_or_none()
            if not pos:
                continue
            pos.last_price = float(latest)
            pos.unrealized_pnl = (pos.last_price - pos.avg_entry_price) * max(pos.quantity, 0.0)
            pos.updated_at = datetime.now(timezone.utc)

        #Re-aggregate unrealized PnL across the full book after updates
        all_positions = self.db_session.execute(select(Position)).scalars().all()
        for pos in all_positions:
            total_unrealized += pos.unrealized_pnl

        state = self._get_account_state()
        state.unrealized_pnl = total_unrealized
        return total_unrealized

    def recalculate_equity(self) -> float:
        """
        Recalculate total portfolio equity from cash + marked market val
        """
        state = self._get_account_state()

        #Sum position market values -> SQL
        market_value = self.db_session.execute(
            select(func.coalesce(func.sum(Position.quantity * Position.last_price), 0.0))
        ).scalar_one()

        state.equity = state.cash + float(market_value)

        #Keep drawdown metrics current whenever equity changes
        state.peak_equity = max(state.peak_equity, state.equity)
        state.current_drawdown = max(0.0, state.peak_equity - state.equity)
        state.max_drawdown = max(state.max_drawdown, state.current_drawdown)
        state.updated_at = datetime.now(timezone.utc)

        return state.equity

    def compute_exposure(self) -> dict[str, float]:
        """
        Compute simple exposure analytics

        Current outputs:
            -gross_exposure: absolute notional across all positions
            -net_exposure_pct: currently gross/equity, despite the name
            -largest_position_pct: largest single position concentration vs equity
            -open_positions: count of currently open positions
        """
        state = self._get_account_state()
        equity = state.equity if state.equity != 0 else 1.0

        rows = self.db_session.execute(select(Position)).scalars().all()
        gross = 0.0
        largest_position_pct = 0.0
        for pos in rows:
            market_value = abs(pos.quantity * pos.last_price)
            gross += market_value
            largest_position_pct = max(largest_position_pct, market_value / abs(equity))

        net_exposure_pct = gross / abs(equity)
        return {
            "gross_exposure": gross,
            "net_exposure_pct": net_exposure_pct,
            "largest_position_pct": largest_position_pct,
            "open_positions": float(sum(1 for p in rows if p.quantity != 0)),
        }

    def execute_signal(self, symbol: str, qty: float, side: str) -> dict[str, Any]:
        """
        Validate and route the treade signal

        Flow:
            1. Build a candidate order
            2. Build current portfolio state snapshot
            3. Run risk guardrails
            4. If passes, submit order to Alpaca and return response
        """
        candidate_order = {"symbol": symbol, "qty": qty, "side": side}

        #Provite guardrails layer w/ current portfolio context
        portfolio_state = {
            "equity": self._get_account_state().equity,
            **self.compute_exposure(),
        }

        #PLACEHOLDER 
        market_state = {}

        decision = self.risk_guardrails.validate_order(candidate_order, portfolio_state, market_state)
        if not decision["allowed"]:
            return {"status": "blocked", "reason": decision["reason"]}
        
        # submit order to alpaca
        order_response = self.alpaca_client.submit_order(symbol=symbol, qty=qty, side=side)
        return order_response