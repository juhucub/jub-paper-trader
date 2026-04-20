from fastapi import FastAPI

from backend.api.routes import router as api_router
from backend.core.settings import get_settings    

def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="Jub Paper Trader API", debug=settings.debug)

    app.include_router(api_router, prefix="/api")

    return app

app = create_app()