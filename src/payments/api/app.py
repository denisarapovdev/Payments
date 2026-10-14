from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from payments.api.routes import health, payments
from payments.core.config import Settings, get_settings
from payments.core.logging import setup_logging
from payments.db.session import create_engine, create_session_factory
from payments.domain.exceptions import IdempotencyConflictError, PaymentNotFoundError


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings)
        app.state.settings = settings
        app.state.session_factory = create_session_factory(engine)
        yield
        await engine.dispose()

    app = FastAPI(title="Payments", version="1.0.0", lifespan=lifespan)
    app.include_router(payments.router)
    app.include_router(health.router)

    @app.exception_handler(PaymentNotFoundError)
    async def _not_found(_: Request, exc: PaymentNotFoundError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND, content={"detail": "Payment not found"}
        )

    @app.exception_handler(IdempotencyConflictError)
    async def _conflict(_: Request, exc: IdempotencyConflictError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": "Idempotency-Key was already used with a different request body"},
        )

    return app
