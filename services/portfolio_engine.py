# portfolio engine for account sync, fill application, and portfolio analytics

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from agent_service.interfaces import ExecutionResult, ReconciliationAnomaly, ReconciliationResult
from db.models.orders import Order
from db.models.portfolio import PortfolioAccountState, TradeHistory
from db.models.positions import Position
from services.alpaca_client import AlpacaClient
from services.risk_guardrails import RiskGuardrails


@dataclass(slots=True)
class PortfolioEngine:
    alpaca_client: AlpacaClient
    risk_guardrails: RiskGuardrails
    db_session: Session

    def _get_account_state(self) -> PortfolioAccountState:
        account = self.db_session.get(PortfolioAccountState, 1)
        if account:
            return account

        account = PortfolioAccountState(id=1)
        self.db_session.add(account)
        self.db_session.flush()
        return account

    def sync_account_state(
        self,
        account: dict[str, Any],
        positions: list[dict[str, Any]],
        orders: list[dict[str, Any]],
        *,
        cycle_id: str = "reconciliation",
        as_of: datetime | None = None,
        source_execution_result: ExecutionResult | None = None,
    ) -> ReconciliationResult:
        """Sync broker snapshots into local state and return a typed reconciliation artifact."""

        now = as_of or datetime.now(timezone.utc)
        state = self._get_account_state()
        previous_account = self._account_state_snapshot(state)
        previous_orders = {
            order.broker_owner_id or f"{order.symbol}:{order.side}:{order.quantity}": self._order_snapshot(order)
            for order in self.db_session.execute(select(Order)).scalars().all()
        }
        previous_positions = {
            position.symbol: self._position_snapshot(position)
            for position in self.db_session.execute(select(Position)).scalars().all()
        }

        broker_cash = float(account.get("cash", account.get("buying_power", state.cash)))
        broker_equity = float(account.get("equity", state.equity))
        state.cash = broker_cash
        state.equity = broker_equity
        state.peak_equity = max(state.peak_equity, state.equity)
        current_drawdown = max(0.0, state.peak_equity - state.equity)
        setattr(state, "current_drawdown", current_drawdown)
        state.max_drawdown = max(state.max_drawdown, current_drawdown)
        state.updated_at = now

        order_deltas: list[dict[str, Any]] = []
        fill_events: list[dict[str, Any]] = []
        anomalies: list[ReconciliationAnomaly] = []

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
                quantity=float(item.get("qty", item.get("quantity", 0.0)) or 0.0),
            )
            if not existing:
                self.db_session.add(order)

            previous_snapshot = previous_orders.get(
                order.broker_owner_id or f"{order.symbol}:{order.side}:{order.quantity}",
                {},
            )
            previous_filled_qty = float(order.filled_quantity or 0.0)
            new_quantity = float(item.get("qty", item.get("quantity", order.quantity)) or 0.0)
            filled_qty_raw = item.get("filled_qty", item.get("filled_quantity"))
            new_filled_qty = previous_filled_qty if filled_qty_raw is None else float(filled_qty_raw)
            fill_delta = max(new_filled_qty - previous_filled_qty, 0.0)
            filled_avg_raw = item.get("filled_avg_price")
            if filled_avg_raw in (None, ""):
                filled_avg_price = float(order.filled_avg_price) if order.filled_avg_price is not None else None
            else:
                filled_avg_price = float(cast(Any, filled_avg_raw))

            order.status = str(item.get("status", order.status))
            order.quantity = new_quantity
            order.filled_quantity = new_filled_qty
            order.filled_avg_price = filled_avg_price
            order.updated_at = now

            order_deltas.append(
                {
                    "broker_order_id": broker_id,
                    "symbol": order.symbol,
                    "side": order.side,
                    "previous_status": previous_snapshot.get("status"),
                    "status": order.status,
                    "previous_quantity": previous_snapshot.get("quantity", 0.0),
                    "quantity": order.quantity,
                    "previous_filled_quantity": previous_snapshot.get("filled_quantity", 0.0),
                    "filled_quantity": order.filled_quantity,
                    "fill_delta": fill_delta,
                }
            )
            if order.filled_quantity > order.quantity + 1e-6:
                anomalies.append(
                    ReconciliationAnomaly(
                        cycle_id=cycle_id,
                        as_of=now,
                        code="filled_quantity_exceeds_order_quantity",
                        message="Broker reported a filled quantity larger than the requested quantity.",
                        severity="warning",
                        symbol=order.symbol,
                        metadata={"filled_quantity": order.filled_quantity, "quantity": order.quantity},
                    )
                )

            if fill_delta > 0 and filled_avg_price is not None:
                fill_event = {
                    "symbol": order.symbol,
                    "side": order.side,
                    "qty": fill_delta,
                    "price": float(filled_avg_price),
                    "broker_order_id": broker_id,
                }
                fill_events.append(fill_event)
                self._record_fill_event(
                    symbol=order.symbol,
                    side=order.side,
                    qty=fill_delta,
                    price=float(filled_avg_price),
                )

        position_deltas: list[dict[str, Any]] = []
        seen_symbols: set[str] = set()
        for item in positions:
            symbol = str(item["symbol"]).upper()
            seen_symbols.add(symbol)
            position = self.db_session.execute(
                select(Position).where(Position.symbol == symbol)
            ).scalar_one_or_none()
            if not position:
                position = Position(symbol=symbol)
                self.db_session.add(position)

            previous_snapshot = previous_positions.get(symbol, {})
            qty = float(item.get("qty", item.get("quantity", position.quantity)) or 0.0)
            avg = float(item.get("avg_entry_price", position.avg_entry_price) or 0.0)
            last = float(item.get("current_price", item.get("last_price", position.last_price or avg)) or 0.0)

            position.quantity = qty
            position.avg_entry_price = avg
            position.cost_basis = qty * avg
            position.last_price = last
            position.unrealized_pnl = (last - avg) * qty
            position.closed_at = now if qty == 0 else None
            position.updated_at = now

            position_deltas.append(
                {
                    "symbol": symbol,
                    "previous_quantity": previous_snapshot.get("quantity", 0.0),
                    "quantity": qty,
                    "previous_last_price": previous_snapshot.get("last_price", 0.0),
                    "last_price": last,
                    "avg_entry_price": avg,
                    "unrealized_pnl": position.unrealized_pnl,
                }
            )

        for symbol, previous_snapshot in previous_positions.items():
            if symbol in seen_symbols:
                continue
            position = self.db_session.execute(
                select(Position).where(Position.symbol == symbol)
            ).scalar_one_or_none()
            if position is None or position.quantity == 0.0:
                continue
            position.quantity = 0.0
            position.cost_basis = 0.0
            position.unrealized_pnl = 0.0
            position.closed_at = now
            position.updated_at = now
            position_deltas.append(
                {
                    "symbol": symbol,
                    "previous_quantity": previous_snapshot.get("quantity", 0.0),
                    "quantity": 0.0,
                    "previous_last_price": previous_snapshot.get("last_price", 0.0),
                    "last_price": previous_snapshot.get("last_price", 0.0),
                    "avg_entry_price": previous_snapshot.get("avg_entry_price", 0.0),
                    "unrealized_pnl": 0.0,
                }
            )

        local_market_value = sum(
            float(position.quantity or 0.0) * float(position.last_price or 0.0)
            for position in self.db_session.execute(select(Position)).scalars().all()
        )
        local_unrealized = sum(
            float(position.unrealized_pnl or 0.0)
            for position in self.db_session.execute(select(Position)).scalars().all()
        )
        state.unrealized_pnl = local_unrealized
        exposure = self.compute_exposure(equity_override=state.equity)
        self.db_session.commit()

        account_snapshot = self._account_state_snapshot(state)
        realized_pnl_delta = float(account_snapshot["realized_pnl"]) - float(previous_account["realized_pnl"])
        status = cast("Any", "warning" if anomalies else "ok")
        return ReconciliationResult(
            cycle_id=cycle_id,
            as_of=now,
            status=status,
            account_state={**account_snapshot, **exposure},
            order_deltas=order_deltas,
            position_deltas=position_deltas,
            fill_events=fill_events,
            realized_pnl_delta=realized_pnl_delta,
            unrealized_pnl=float(account_snapshot["unrealized_pnl"]),
            anomalies=anomalies,
            diagnostics={
                "previous_account_state": previous_account,
                "source_execution_summary": source_execution_result.summary if source_execution_result else None,
                "broker_equity_authoritative": True,
                "local_market_value": local_market_value,
                "buying_power": float(account.get("buying_power", broker_cash) or broker_cash),
            },
            source_execution_at=source_execution_result.as_of if source_execution_result else None,
            lineage={
                "source_execution_at": source_execution_result.as_of.isoformat()
                if source_execution_result
                else None,
            },
        )

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
        symbol = str(fill_event["symbol"]).upper()
        side = str(fill_event["side"]).lower()
        qty = float(fill_event["qty"])
        price = float(fill_event["price"])
        order_id = fill_event.get("order_id")

        state = self._get_account_state()
        position = self.db_session.execute(
            select(Position).where(Position.symbol == symbol)
        ).scalar_one_or_none()
        if not position:
            position = Position(symbol=symbol)
            self.db_session.add(position)
            self.db_session.flush()

        signed_qty = qty if side == "buy" else -qty
        prev_qty = position.quantity
        prev_avg = position.avg_entry_price

        realized_increment = 0.0
        if prev_qty > 0 and signed_qty < 0:
            close_qty = min(prev_qty, abs(signed_qty))
            realized_increment = (price - prev_avg) * close_qty

        new_qty = prev_qty + signed_qty
        if new_qty > 0 and signed_qty > 0:
            position.avg_entry_price = ((prev_qty * prev_avg) + (qty * price)) / new_qty if new_qty else 0.0
        elif new_qty <= 0:
            position.avg_entry_price = 0.0

        position.quantity = new_qty
        position.cost_basis = max(new_qty, 0.0) * position.avg_entry_price
        position.last_price = price
        position.realized_pnl += realized_increment
        position.unrealized_pnl = (price - position.avg_entry_price) * max(new_qty, 0.0)
        position.closed_at = datetime.now(timezone.utc) if new_qty == 0 else None
        position.updated_at = datetime.now(timezone.utc)

        cash_delta = -(qty * price) if side == "buy" else (qty * price)
        state.cash += cash_delta
        state.realized_pnl += realized_increment

        if state.daily_date != datetime.now(timezone.utc).date():
            state.daily_date = datetime.now(timezone.utc).date()
            state.daily_realized_pnl = 0.0
        state.daily_realized_pnl += realized_increment

        trade = TradeHistory(
            symbol=symbol,
            side=side,
            quantity=qty,
            price=price,
            realized_pnl=realized_increment,
            order_id=int(order_id) if order_id is not None else None,
        )
        self.db_session.add(trade)

        self.mark_to_market({symbol: price})
        equity = self.recalculate_equity()
        exposure = self.compute_exposure()

        self.db_session.commit()
        return {"equity": equity, **exposure, "realized_pnl": state.realized_pnl}

    def mark_to_market(self, latest_prices: dict[str, float]) -> float:
        total_unrealized = 0.0
        for symbol, latest in latest_prices.items():
            pos = self.db_session.execute(
                select(Position).where(Position.symbol == symbol.upper())
            ).scalar_one_or_none()
            if not pos:
                continue
            pos.last_price = float(latest)
            pos.unrealized_pnl = (pos.last_price - pos.avg_entry_price) * max(pos.quantity, 0.0)
            pos.updated_at = datetime.now(timezone.utc)

        all_positions = self.db_session.execute(select(Position)).scalars().all()
        for pos in all_positions:
            total_unrealized += pos.unrealized_pnl

        state = self._get_account_state()
        state.unrealized_pnl = total_unrealized
        return total_unrealized

    def recalculate_equity(self) -> float:
        state = self._get_account_state()
        market_value = self.db_session.execute(
            select(func.coalesce(func.sum(Position.quantity * Position.last_price), 0.0))
        ).scalar_one()

        state.equity = state.cash + float(market_value)
        state.peak_equity = max(state.peak_equity, state.equity)
        current_drawdown = max(0.0, state.peak_equity - state.equity)
        setattr(state, "current_drawdown", current_drawdown)
        state.max_drawdown = max(state.max_drawdown, current_drawdown)
        state.updated_at = datetime.now(timezone.utc)
        return state.equity

    def compute_exposure(self, equity_override: float | None = None) -> dict[str, float]:
        state = self._get_account_state()
        equity = float(equity_override if equity_override is not None else state.equity)
        equity = equity if equity != 0 else 1.0

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
            "open_positions": float(sum(1 for position in rows if position.quantity != 0)),
        }

    def execute_signal(self, symbol: str, qty: float, side: str) -> dict[str, Any]:
        _ = (symbol, qty, side)
        return {
            "status": "blocked",
            "reason": "execute_signal_deprecated_use_order_proposals",
        }

    @staticmethod
    def _account_state_snapshot(state: PortfolioAccountState) -> dict[str, Any]:
        return {
            "cash": float(state.cash or 0.0),
            "equity": float(state.equity or 0.0),
            "peak_equity": float(state.peak_equity or 0.0),
            "max_drawdown": float(state.max_drawdown or 0.0),
            "current_drawdown": float(getattr(state, "current_drawdown", 0.0) or 0.0),
            "realized_pnl": float(state.realized_pnl or 0.0),
            "unrealized_pnl": float(state.unrealized_pnl or 0.0),
            "daily_realized_pnl": float(state.daily_realized_pnl or 0.0),
            "daily_date": state.daily_date.isoformat() if state.daily_date else None,
        }

    @staticmethod
    def _order_snapshot(order: Order) -> dict[str, Any]:
        return {
            "broker_order_id": order.broker_owner_id,
            "symbol": order.symbol,
            "side": order.side,
            "quantity": float(order.quantity or 0.0),
            "filled_quantity": float(order.filled_quantity or 0.0),
            "status": order.status,
        }

    @staticmethod
    def _position_snapshot(position: Position) -> dict[str, Any]:
        return {
            "symbol": position.symbol,
            "quantity": float(position.quantity or 0.0),
            "avg_entry_price": float(position.avg_entry_price or 0.0),
            "last_price": float(position.last_price or 0.0),
            "realized_pnl": float(position.realized_pnl or 0.0),
            "unrealized_pnl": float(position.unrealized_pnl or 0.0),
        }
