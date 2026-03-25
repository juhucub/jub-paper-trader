#Top level API router
from fastapi import APIRouter, Depends

from backend.dependencies.wiring import AppContainer, get_container

router = APIRouter()

@router.get("/health")
def health(container: AppContainer = Depends(get_container)) -> dict[str, str]:
    #Health endpoint that does dependency wiring
    _ = container
    return {"status": "ok"}