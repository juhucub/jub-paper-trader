#Top level API router
from fastapi import APIRouter, Depends



router = APIRouter()

@router.get("/health")
def health_check():
    return {"status": "ok"}