"""Error responses use the same {status, data} envelope as success ones:
status is always "error"; data carries a message plus optional errors list.
"""
from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_exception(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        # routes raise detail as either a plain message or {message, errors}
        data = exc.detail if isinstance(exc.detail, dict) else {"message": exc.detail}
        return JSONResponse(
            status_code=exc.status_code,
            content={"status": "error", "data": data},
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_exception(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # keep only the serializable, useful parts of pydantic's error dicts
        errors = [
            {"loc": list(e.get("loc", [])), "msg": e.get("msg"), "type": e.get("type")}
            for e in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content={
                "status": "error",
                "data": {"message": "request validation failed", "errors": errors},
            },
        )

    @app.exception_handler(Exception)
    async def unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "data": {"message": "internal server error"}},
        )
