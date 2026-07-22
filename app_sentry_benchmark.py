import os

import sentry_sdk
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from sentry_sdk.integrations.starlette import StarletteIntegration

if dsn := os.environ.get("SENTRY_DSN"):
    sample_rate = float(os.environ.get("SENTRY_SAMPLE_RATE", "1.0"))
    sentry_sdk.init(
        dsn=dsn,
        sample_rate=sample_rate,
        default_integrations=False,
        auto_enabling_integrations=False,
        integrations=[
            StarletteIntegration(),
        ],
    )
    print(f"{sample_rate=}")

app = FastAPI()


@app.get("/ok")
async def ok() -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/error")
async def error() -> PlainTextResponse:
    raise ValueError("benchmark error")
