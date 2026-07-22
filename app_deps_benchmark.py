from typing import Annotated

from fastapi import Depends, FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()


def sync_dep1() -> str:
    return "sync_dep1"


def sync_dep2() -> str:
    return "sync_dep2"


def sync_dep3() -> str:
    return "sync_dep3"


def sync_dep4() -> str:
    return "sync_dep4"


def sync_dep5() -> str:
    return "sync_dep5"


def sync_dep6() -> str:
    return "sync_dep6"


def sync_dep7() -> str:
    return "sync_dep7"


def sync_dep8() -> str:
    return "sync_dep8"


def sync_dep9() -> str:
    return "sync_dep9"


def sync_dep10() -> str:
    return "sync_dep10"


async def async_dep1() -> str:
    return "async_dep1"


async def async_dep2() -> str:
    return "async_dep2"


async def async_dep3() -> str:
    return "async_dep3"


async def async_dep4() -> str:
    return "async_dep4"


async def async_dep5() -> str:
    return "async_dep5"


async def async_dep6() -> str:
    return "async_dep6"


async def async_dep7() -> str:
    return "async_dep7"


async def async_dep8() -> str:
    return "async_dep8"


async def async_dep9() -> str:
    return "async_dep9"


async def async_dep10() -> str:
    return "async_dep10"


@app.get("/empty")
async def empty() -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-1")
async def sync_deps_1(
    dep1: Annotated[str, Depends(sync_dep1)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-2")
async def sync_deps_2(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-3")
async def sync_deps_3(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
    dep3: Annotated[str, Depends(sync_dep3)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-4")
async def sync_deps_4(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
    dep3: Annotated[str, Depends(sync_dep3)],
    dep4: Annotated[str, Depends(sync_dep4)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-5")
async def sync_deps_5(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
    dep3: Annotated[str, Depends(sync_dep3)],
    dep4: Annotated[str, Depends(sync_dep4)],
    dep5: Annotated[str, Depends(sync_dep5)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-6")
async def sync_deps_6(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
    dep3: Annotated[str, Depends(sync_dep3)],
    dep4: Annotated[str, Depends(sync_dep4)],
    dep5: Annotated[str, Depends(sync_dep5)],
    dep6: Annotated[str, Depends(sync_dep6)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-7")
async def sync_deps_7(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
    dep3: Annotated[str, Depends(sync_dep3)],
    dep4: Annotated[str, Depends(sync_dep4)],
    dep5: Annotated[str, Depends(sync_dep5)],
    dep6: Annotated[str, Depends(sync_dep6)],
    dep7: Annotated[str, Depends(sync_dep7)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-8")
async def sync_deps_8(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
    dep3: Annotated[str, Depends(sync_dep3)],
    dep4: Annotated[str, Depends(sync_dep4)],
    dep5: Annotated[str, Depends(sync_dep5)],
    dep6: Annotated[str, Depends(sync_dep6)],
    dep7: Annotated[str, Depends(sync_dep7)],
    dep8: Annotated[str, Depends(sync_dep8)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-9")
async def sync_deps_9(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
    dep3: Annotated[str, Depends(sync_dep3)],
    dep4: Annotated[str, Depends(sync_dep4)],
    dep5: Annotated[str, Depends(sync_dep5)],
    dep6: Annotated[str, Depends(sync_dep6)],
    dep7: Annotated[str, Depends(sync_dep7)],
    dep8: Annotated[str, Depends(sync_dep8)],
    dep9: Annotated[str, Depends(sync_dep9)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/sync-deps-10")
async def sync_deps_10(
    dep1: Annotated[str, Depends(sync_dep1)],
    dep2: Annotated[str, Depends(sync_dep2)],
    dep3: Annotated[str, Depends(sync_dep3)],
    dep4: Annotated[str, Depends(sync_dep4)],
    dep5: Annotated[str, Depends(sync_dep5)],
    dep6: Annotated[str, Depends(sync_dep6)],
    dep7: Annotated[str, Depends(sync_dep7)],
    dep8: Annotated[str, Depends(sync_dep8)],
    dep9: Annotated[str, Depends(sync_dep9)],
    dep10: Annotated[str, Depends(sync_dep10)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-1")
async def async_deps_1(
    dep1: Annotated[str, Depends(async_dep1)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-2")
async def async_deps_2(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-3")
async def async_deps_3(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
    dep3: Annotated[str, Depends(async_dep3)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-4")
async def async_deps_4(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
    dep3: Annotated[str, Depends(async_dep3)],
    dep4: Annotated[str, Depends(async_dep4)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-5")
async def async_deps_5(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
    dep3: Annotated[str, Depends(async_dep3)],
    dep4: Annotated[str, Depends(async_dep4)],
    dep5: Annotated[str, Depends(async_dep5)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-6")
async def async_deps_6(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
    dep3: Annotated[str, Depends(async_dep3)],
    dep4: Annotated[str, Depends(async_dep4)],
    dep5: Annotated[str, Depends(async_dep5)],
    dep6: Annotated[str, Depends(async_dep6)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-7")
async def async_deps_7(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
    dep3: Annotated[str, Depends(async_dep3)],
    dep4: Annotated[str, Depends(async_dep4)],
    dep5: Annotated[str, Depends(async_dep5)],
    dep6: Annotated[str, Depends(async_dep6)],
    dep7: Annotated[str, Depends(async_dep7)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-8")
async def async_deps_8(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
    dep3: Annotated[str, Depends(async_dep3)],
    dep4: Annotated[str, Depends(async_dep4)],
    dep5: Annotated[str, Depends(async_dep5)],
    dep6: Annotated[str, Depends(async_dep6)],
    dep7: Annotated[str, Depends(async_dep7)],
    dep8: Annotated[str, Depends(async_dep8)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-9")
async def async_deps_9(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
    dep3: Annotated[str, Depends(async_dep3)],
    dep4: Annotated[str, Depends(async_dep4)],
    dep5: Annotated[str, Depends(async_dep5)],
    dep6: Annotated[str, Depends(async_dep6)],
    dep7: Annotated[str, Depends(async_dep7)],
    dep8: Annotated[str, Depends(async_dep8)],
    dep9: Annotated[str, Depends(async_dep9)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")


@app.get("/async-deps-10")
async def async_deps_10(
    dep1: Annotated[str, Depends(async_dep1)],
    dep2: Annotated[str, Depends(async_dep2)],
    dep3: Annotated[str, Depends(async_dep3)],
    dep4: Annotated[str, Depends(async_dep4)],
    dep5: Annotated[str, Depends(async_dep5)],
    dep6: Annotated[str, Depends(async_dep6)],
    dep7: Annotated[str, Depends(async_dep7)],
    dep8: Annotated[str, Depends(async_dep8)],
    dep9: Annotated[str, Depends(async_dep9)],
    dep10: Annotated[str, Depends(async_dep10)],
) -> PlainTextResponse:
    return PlainTextResponse("OK")
