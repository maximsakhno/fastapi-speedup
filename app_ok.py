"""Минимальное FastAPI-приложение для бенчмарка флагов uvicorn.

Одна пустая ручка GET /ok, отдающая текст `OK`. Без lifespan/БД — мерим
чисто HTTP/серверный слой (эффект флагов uvicorn), а не работу приложения.
"""

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()


@app.get("/ok")
async def ok() -> PlainTextResponse:
    return PlainTextResponse("OK")
