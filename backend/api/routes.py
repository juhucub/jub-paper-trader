#Top level API router
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse

from agent_service.debug_tools import (
    build_cycle_dashboard_payload_from_snapshot,
    render_cycle_dashboard_html,
)
from backend.dependencies.wiring import AppContainer, get_container
from db.repositories.snapshots import get_latest_bot_cycle_snapshot

router = APIRouter()

@router.get("/health")
def health(container: AppContainer = Depends(get_container)) -> dict[str, str]:
    #Health endpoint that does dependency wiring
    _ = container
    return {"status": "ok"}


@router.get("/debug/cycle/latest", response_class=HTMLResponse)
def latest_cycle_debug(container: AppContainer = Depends(get_container)) -> HTMLResponse:
    snapshot = get_latest_bot_cycle_snapshot(container.bot_cycle_service.db_session)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No persisted bot cycle snapshot found.")

    dashboard = build_cycle_dashboard_payload_from_snapshot(snapshot.payload or {})
    return HTMLResponse(render_cycle_dashboard_html(dashboard))
