"""Микробенчмарк TPS сценария `/surgery-offer`: 10 запросов happy-path нативно на каждом ORM.

Без веб-фреймворка, без ручки и без сервиса. Берём ровно те 10 запросов, что выполняет успешный
`GET /surgery-offer`, и гоняем их в одной транзакции — но каждый стек делает это СВОИМ способом
(query-builder + маппинг строк в объекты), как написал бы реальный разработчик:

    asyncpg     — сырой SQL (эталон, 10 запросов);
    sqlalchemy  — SQLAlchemy ORM (модели из bench_clinic/app_clinic_bench_sync.py, 10 запросов);
    tortoise    — Tortoise ORM   (scenario_tortoise.py, драйвер asyncpg, 11 запросов);
    piccolo     — Piccolo        (scenario_piccolo.py, драйвер asyncpg, 11 запросов);
    django      — Django ORM     (scenario_django.py, драйвер psycopg в пуле потоков, 11 запросов).

64 конкурентных воркера гоняют сценарий-транзакцию через пул на 64 соединения; меряем транзакции
в секунду (TPS). Гринлет-моста нет. Сценарий read-only и идемпотентен — TPS стабилен.

Запуск (из корня репозитория):
    .venv/bin/python -m bench_orm.benchmark_async_orm
Подмножество: DRIVERS=tortoise,piccolo .venv/bin/python -m bench_orm.benchmark_async_orm
Дым-тест:     DURATION_S=1 WARMUP_S=1 RUNS=1 .venv/bin/python -m bench_orm.benchmark_async_orm
"""

from __future__ import annotations

import asyncio
import csv
import os
import sys
import threading
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, time, timedelta

import asyncpg
from asgiref.sync import sync_to_async
from piccolo.engine.postgres import PostgresEngine
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import contains_eager, joinedload
from tortoise import Tortoise

from bench_clinic.app_clinic_bench_sync import (
    Consultation,
    Doctor,
    LabResult,
    LoyaltyAccount,
    PatientContraindication,
    Procedure,
    ProcedureContraindication,
    PromoCode,
    ScheduleSlot,
    Surgery,
)
from bench_clinic.app_clinic_bench_sync import Patient as SAPatient
from bench_clinic.app_clinic_bench_sync import User as SAUser
from bench_orm import scenario_piccolo, scenario_tortoise

# --- Параметры подключения (те же, что Settings у приложений клиники) ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_USER = os.environ.get("DB_USER", "speedup-fastapi")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "speedup-fastapi")
DB_NAME = os.environ.get("DB_NAME", "clinic")

ASYNCPG_DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
SA_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
TORTOISE_URL = f"asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
PICCOLO_CONFIG = {
    "host": DB_HOST,
    "port": DB_PORT,
    "user": DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
}

# --- Параметры нагрузки ---
POOL_SIZE = int(os.environ.get("POOL_SIZE", "64"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "64"))
WARMUP_S = float(os.environ.get("WARMUP_S", "3"))
DURATION_S = float(os.environ.get("DURATION_S", "10"))
RUNS = int(os.environ.get("RUNS", "3"))
COOLDOWN_S = float(os.environ.get("COOLDOWN_S", "1"))

# --- Параметры happy-path сценария (тот же вход, что даёт HTTP 200) ---
PARAMS = {
    "user_id": int(os.environ.get("USER_ID", "1")),
    "patient_id": int(os.environ.get("PATIENT_ID", "1")),
    "procedure_code": os.environ.get("PROCEDURE_CODE", "rhinoplasty"),
    "promo_code": os.environ.get("PROMO_CODE", "NOSIK15"),
    "preferred_date": date.today() + timedelta(days=int(os.environ.get("DAYS_AHEAD", "7"))),
}

ALL_DRIVERS = ("asyncpg", "sqlalchemy", "tortoise", "piccolo", "django")
DRIVERS = tuple(os.environ.get("DRIVERS", ",".join(ALL_DRIVERS)).split(","))

DRIVER_LABELS = {
    "asyncpg": "asyncpg",
    "sqlalchemy": "SQLAlchemy",
    "tortoise": "Tortoise",
    "piccolo": "Piccolo",
    "django": "Django",
}
# Фактическое число SQL-запросов на транзакцию (ORM дробят reverse-FL на отдельный запрос).
QUERIES_PER_TXN = {"asyncpg": 10, "sqlalchemy": 10, "tortoise": 11, "piccolo": 11, "django": 11}

CSV_PATH = "scenario_benchmark_results.csv"

# Django настраивается один раз на процесс.
_django_module = None
_django_connections = None


class Counters:
    __slots__ = ("count", "latencies", "measuring", "deadline")

    def __init__(self) -> None:
        self.count = 0
        self.latencies: list[float] = []
        self.measuring = False
        self.deadline = 0.0


# --- Сценарии уровня данных (asyncpg-эталон и SQLAlchemy-ORM живут здесь; остальные — в scenario_*.py) ---


async def asyncpg_scenario(conn: asyncpg.Connection) -> tuple:
    """Те же 10 запросов, что в app_clinic_bench_asyncpg.py, сырым SQL в одной транзакции."""
    pid = PARAMS["patient_id"]
    async with conn.transaction():
        await conn.fetchrow("SELECT id, name, email, role FROM users WHERE id = $1", PARAMS["user_id"])
        patient = await conn.fetchrow("SELECT id, name FROM patients WHERE id = $1", pid)
        procedure = await conn.fetchrow(
            """
            SELECT p.id, p.code, p.name, p.base_price, p.duration_minutes, p.required_specialization,
                   COALESCE(array_agg(t.test_type) FILTER (WHERE t.test_type IS NOT NULL), '{}') AS required_tests
            FROM procedures p
            LEFT JOIN procedure_required_tests t ON t.procedure_id = p.id
            WHERE p.code = $1
            GROUP BY p.id
            """,
            PARAMS["procedure_code"],
        )
        proc_id = procedure["id"]
        test_types = list(procedure["required_tests"])
        await conn.fetchrow(
            """
            SELECT id FROM consultations
            WHERE patient_id = $1 AND procedure_id = $2 AND approved IS TRUE
            ORDER BY held_at DESC LIMIT 1
            """,
            pid,
            proc_id,
        )
        await conn.fetch(
            """
            SELECT test_type, max(taken_at) AS taken_at
            FROM lab_results
            WHERE patient_id = $1 AND test_type = ANY($2::text[])
            GROUP BY test_type
            """,
            pid,
            test_types,
        )
        await conn.fetch(
            """
            SELECT pc.code
            FROM patient_contraindications pc
            JOIN procedure_contraindications prc ON prc.code = pc.code
            WHERE pc.patient_id = $1 AND prc.procedure_id = $2
            """,
            pid,
            proc_id,
        )
        day_start = datetime.combine(PARAMS["preferred_date"], time.min)
        day_end = day_start + timedelta(days=1)
        slot = await conn.fetchrow(
            """
            SELECT s.starts_at, s.duration_minutes, s.is_hot, d.name AS doctor_name, r.name AS room_name
            FROM schedule_slots s
            JOIN doctors d ON d.id = s.doctor_id
            JOIN operating_rooms r ON r.id = s.room_id
            WHERE d.specialization = $1
              AND s.starts_at >= $2 AND s.starts_at < $3
              AND s.duration_minutes >= $4
              AND NOT EXISTS (SELECT 1 FROM surgeries su WHERE su.slot_id = s.id)
            ORDER BY s.starts_at LIMIT 1
            """,
            procedure["required_specialization"],
            day_start,
            day_end,
            procedure["duration_minutes"],
        )
        promo = await conn.fetchrow(
            "SELECT id, code, discount_rate, valid_until, usage_limit, used_count, procedure_id "
            "FROM promo_codes WHERE code = $1",
            PARAMS["promo_code"],
        )
        await conn.fetchrow("SELECT status FROM loyalty_accounts WHERE patient_id = $1", pid)
        await conn.fetchval(
            "SELECT count(*) FROM surgeries WHERE patient_id = $1 AND status = 'completed'", pid
        )
    return (patient["name"], procedure["name"], slot is not None, promo is not None)


async def sqlalchemy_scenario(session) -> tuple:
    """Те же 10 запросов через SQLAlchemy ORM (модели и запросы как в репозиториях приложений клиники)."""
    pid = PARAMS["patient_id"]
    async with session.begin():
        await session.execute(select(SAUser).where(SAUser.id == PARAMS["user_id"]))
        patient = (
            await session.execute(select(SAPatient).where(SAPatient.id == pid))
        ).scalar_one_or_none()
        procedure = (
            await session.execute(
                select(Procedure)
                .where(Procedure.code == PARAMS["procedure_code"])
                .options(joinedload(Procedure.required_tests))
            )
        ).unique().scalar_one_or_none()
        (
            await session.execute(
                select(Consultation)
                .where(
                    Consultation.patient_id == pid,
                    Consultation.procedure_id == procedure.id,
                    Consultation.approved.is_(True),
                )
                .order_by(Consultation.held_at.desc())
                .limit(1)
            )
        ).scalars().first()
        test_types = [t.test_type for t in procedure.required_tests]
        (
            await session.execute(
                select(LabResult.test_type, func.max(LabResult.taken_at))
                .where(LabResult.patient_id == pid, LabResult.test_type.in_(test_types))
                .group_by(LabResult.test_type)
            )
        ).all()
        (
            await session.execute(
                select(PatientContraindication.code)
                .join(ProcedureContraindication, ProcedureContraindication.code == PatientContraindication.code)
                .where(
                    PatientContraindication.patient_id == pid,
                    ProcedureContraindication.procedure_id == procedure.id,
                )
            )
        ).scalars().all()
        day_start = datetime.combine(PARAMS["preferred_date"], time.min)
        day_end = day_start + timedelta(days=1)
        slot_is_taken = select(Surgery.id).where(Surgery.slot_id == ScheduleSlot.id).exists()
        slot = (
            await session.execute(
                select(ScheduleSlot)
                .join(ScheduleSlot.doctor)
                .where(
                    Doctor.specialization == procedure.required_specialization,
                    ScheduleSlot.starts_at >= day_start,
                    ScheduleSlot.starts_at < day_end,
                    ScheduleSlot.duration_minutes >= procedure.duration_minutes,
                    ~slot_is_taken,
                )
                .options(contains_eager(ScheduleSlot.doctor), joinedload(ScheduleSlot.room))
                .order_by(ScheduleSlot.starts_at)
                .limit(1)
            )
        ).scalars().first()
        promo = (
            await session.execute(select(PromoCode).where(PromoCode.code == PARAMS["promo_code"]))
        ).scalar_one_or_none()
        await session.execute(select(LoyaltyAccount).where(LoyaltyAccount.patient_id == pid))
        (
            await session.execute(
                select(func.count(Surgery.id)).where(Surgery.patient_id == pid, Surgery.status == "completed")
            )
        ).scalar_one()
        # Собираем результат ВНУТРИ транзакции: после commit атрибуты истекают (expire_on_commit).
        result = (patient.name, procedure.name, slot is not None, promo is not None)
    return result


# --- Единицы работы: unit(resource) -> одна сценарий-транзакция ---


async def asyncpg_unit(pool: asyncpg.Pool) -> tuple:
    async with pool.acquire() as conn:
        return await asyncpg_scenario(conn)


async def sqlalchemy_unit(session_factory: async_sessionmaker) -> tuple:
    async with session_factory() as session:
        return await sqlalchemy_scenario(session)


async def tortoise_unit(_resource: object) -> tuple:
    return await scenario_tortoise.scenario(PARAMS)


async def piccolo_unit(_resource: object) -> tuple:
    return await scenario_piccolo.scenario(PARAMS)


async def django_unit(call) -> tuple:
    return await call(PARAMS)


# --- Нагрузочный цикл ---


async def worker(unit, resource, counters: Counters) -> None:
    loop = asyncio.get_running_loop()
    while loop.time() < counters.deadline:
        t0 = loop.time()
        await unit(resource)
        if counters.measuring:
            counters.count += 1
            counters.latencies.append(loop.time() - t0)


async def run_window(unit, resource) -> tuple[float, float, float]:
    loop = asyncio.get_running_loop()
    counters = Counters()

    counters.measuring = False
    counters.deadline = loop.time() + WARMUP_S
    await asyncio.gather(*[asyncio.create_task(worker(unit, resource, counters)) for _ in range(CONCURRENCY)])

    counters.count = 0
    counters.latencies.clear()
    counters.measuring = True
    started = loop.time()
    counters.deadline = started + DURATION_S
    await asyncio.gather(*[asyncio.create_task(worker(unit, resource, counters)) for _ in range(CONCURRENCY)])
    elapsed = loop.time() - started

    tps = counters.count / elapsed if elapsed else 0.0
    lats = sorted(counters.latencies)
    lat_avg_ms = (sum(lats) / len(lats) * 1000) if lats else 0.0
    lat_p99_ms = (lats[min(len(lats) - 1, int(len(lats) * 0.99))] * 1000) if lats else 0.0
    return tps, lat_avg_ms, lat_p99_ms


# --- Setup/teardown per driver ---


async def _tortoise_init() -> None:
    url = f"{TORTOISE_URL}?minsize={POOL_SIZE}&maxsize={POOL_SIZE}"
    await Tortoise.init(db_url=url, modules={"models": ["bench_orm.scenario_tortoise"]}, use_tz=False)


def _django_setup() -> None:
    global _django_module, _django_connections
    if _django_module is not None:
        return
    import django
    from django.conf import settings

    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": DB_NAME,
                "USER": DB_USER,
                "PASSWORD": DB_PASSWORD,
                "HOST": DB_HOST,
                "PORT": str(DB_PORT),
                "CONN_MAX_AGE": None,  # соединение живёт в потоке => 64 потока = 64 соединения
                "CONN_HEALTH_CHECKS": False,
                "AUTOCOMMIT": True,
            }
        },
        INSTALLED_APPS=[],
        USE_TZ=False,
    )
    django.setup()
    from django.db import connections

    from bench_orm import scenario_django

    _django_module = scenario_django
    _django_connections = connections


def _django_teardown(executor: ThreadPoolExecutor) -> None:
    barrier = threading.Barrier(CONCURRENCY)

    def _close() -> None:
        barrier.wait()
        _django_connections.close_all()

    for fut in [executor.submit(_close) for _ in range(CONCURRENCY)]:
        fut.result()
    executor.shutdown(wait=True)


async def run_config(driver: str) -> tuple[float, float, float]:
    """Поднять свежий пул/движок для драйвера, прогнать окно, закрыть ресурсы."""
    if driver == "asyncpg":
        pool = await asyncpg.create_pool(dsn=ASYNCPG_DSN, min_size=POOL_SIZE, max_size=POOL_SIZE)
        try:
            return await run_window(asyncpg_unit, pool)
        finally:
            await pool.close()
    if driver == "sqlalchemy":
        engine = create_async_engine(SA_URL, pool_size=POOL_SIZE, max_overflow=0)
        session_factory = async_sessionmaker(engine)
        try:
            return await run_window(sqlalchemy_unit, session_factory)
        finally:
            await engine.dispose()
    if driver == "tortoise":
        await _tortoise_init()
        try:
            return await run_window(tortoise_unit, None)
        finally:
            await Tortoise.close_connections()
    if driver == "piccolo":
        engine = PostgresEngine(config=PICCOLO_CONFIG)
        await engine.start_connection_pool(max_size=POOL_SIZE, min_size=POOL_SIZE)
        scenario_piccolo.bind(engine)
        try:
            return await run_window(piccolo_unit, None)
        finally:
            await engine.close_connection_pool()
    if driver == "django":
        _django_setup()
        executor = ThreadPoolExecutor(max_workers=CONCURRENCY)
        call = sync_to_async(_django_module.scenario, thread_sensitive=False, executor=executor)
        try:
            return await run_window(django_unit, call)
        finally:
            _django_teardown(executor)
    raise ValueError(f"Неизвестный драйвер: {driver}")


# --- Sanity-check: все драйверы должны отдать одинаковый happy-path результат ---


async def _sanity_one(driver: str) -> tuple:
    if driver == "asyncpg":
        pool = await asyncpg.create_pool(dsn=ASYNCPG_DSN, min_size=1, max_size=1)
        try:
            return await asyncpg_unit(pool)
        finally:
            await pool.close()
    if driver == "sqlalchemy":
        engine = create_async_engine(SA_URL, pool_size=1, max_overflow=0)
        try:
            return await sqlalchemy_unit(async_sessionmaker(engine))
        finally:
            await engine.dispose()
    if driver == "tortoise":
        await _tortoise_init()
        try:
            return await tortoise_unit(None)
        finally:
            await Tortoise.close_connections()
    if driver == "piccolo":
        engine = PostgresEngine(config=PICCOLO_CONFIG)
        await engine.start_connection_pool(max_size=1, min_size=1)
        scenario_piccolo.bind(engine)
        try:
            return await piccolo_unit(None)
        finally:
            await engine.close_connection_pool()
    if driver == "django":
        _django_setup()
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            return await sync_to_async(_django_module.scenario, thread_sensitive=False, executor=executor)(PARAMS)
        finally:
            _django_teardown_single(executor)
    raise ValueError(driver)


def _django_teardown_single(executor: ThreadPoolExecutor) -> None:
    def _close() -> None:
        _django_connections.close_all()

    executor.submit(_close).result()
    executor.shutdown(wait=True)


async def sanity_check() -> None:
    """Прогнать каждый сценарий по разу и убедиться, что все нашли happy-path (иначе не тот patient_id)."""
    print("Sanity-check (patient.name, procedure.name, slot_found, promo_found):")
    results = {}
    for driver in DRIVERS:
        res = await _sanity_one(driver)
        results[driver] = res
        ok = res[2] and res[3] and res[0] and res[1]
        print(f"  {DRIVER_LABELS.get(driver, driver):>11}: {res}  {'OK' if ok else 'ПРОВАЛ'}")
        if not ok:
            print(
                f"\nОШИБКА: сценарий {driver} не прошёл happy-path (slot/promo не найдены). "
                f"Проверь PATIENT_ID/PROCEDURE_CODE/PROMO_CODE — данные не те.",
                file=sys.stderr,
            )
            sys.exit(1)
    # slot_found/promo_found у всех True; имена совпадают
    names = {(r[0], r[1]) for r in results.values()}
    if len(names) != 1:
        print(f"\nОШИБКА: драйверы вернули разные имена: {names}", file=sys.stderr)
        sys.exit(1)
    print("  → все драйверы согласованы на одном happy-path.\n")


# --- CSV и сводка ---


def _write_csv(rows: list[dict], avg_tps: dict[str, float], avg_lat: dict[str, tuple]) -> None:
    fieldnames = ["driver", "run", "tps", "latency_avg_ms", "latency_p99_ms"]
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        for driver, tps in avg_tps.items():
            lat_avg, lat_p99 = avg_lat[driver]
            writer.writerow(
                {
                    "driver": driver,
                    "run": "avg",
                    "tps": f"{tps:.1f}",
                    "latency_avg_ms": f"{lat_avg:.3f}",
                    "latency_p99_ms": f"{lat_p99:.3f}",
                }
            )
    print(f"\nРезультаты записаны в {CSV_PATH}")


def _load_prior(skip_drivers: set[str]) -> tuple[list[dict], dict, dict]:
    rows: list[dict] = []
    avg_tps: dict[str, float] = {}
    avg_lat: dict[str, tuple[float, float]] = {}
    if not os.path.exists(CSV_PATH):
        return rows, avg_tps, avg_lat
    with open(CSV_PATH, newline="") as f:
        for r in csv.DictReader(f):
            if r["driver"] in skip_drivers:
                continue
            if r["run"] == "avg":
                avg_tps[r["driver"]] = float(r["tps"])
                avg_lat[r["driver"]] = (float(r["latency_avg_ms"]), float(r["latency_p99_ms"]))
            else:
                rows.append(r)
    return rows, avg_tps, avg_lat


def _print_summary(avg_tps: dict[str, float], avg_lat: dict[str, tuple]) -> None:
    base = avg_tps.get("asyncpg", 0.0)
    print("\n" + "=" * 72)
    print(f"{'драйвер':>11}  {'TPS':>10}  {'avg ms':>8}  {'p99 ms':>8}  {'доля asyncpg':>13}  {'SQL/txn':>7}")
    print("-" * 72)
    for driver in ALL_DRIVERS:
        if driver not in avg_tps:
            continue
        tps = avg_tps[driver]
        lat_avg, lat_p99 = avg_lat[driver]
        share = f"{tps / base * 100:5.1f}%" if base else "  n/a"
        print(
            f"{DRIVER_LABELS.get(driver, driver):>11}  {tps:>10,.0f}  {lat_avg:>8.2f}  {lat_p99:>8.2f}  "
            f"{share:>13}  {QUERIES_PER_TXN.get(driver, '?'):>7}"
        )
    print("=" * 72)
    print(
        "Сценарий = 10 логических запросов успешного /surgery-offer в одной транзакции.\n"
        "У ORM фактических запросов 11: список required_tests (reverse-FK) тянется отдельным запросом."
    )


async def main() -> None:
    print(
        f"Бенчмарк сценария /surgery-offer: pool={POOL_SIZE}, concurrency={CONCURRENCY}, "
        f"warmup={WARMUP_S}s, window={DURATION_S}s, runs={RUNS}, drivers={','.join(DRIVERS)}"
    )
    print(f"Вход: {PARAMS}\n")

    await sanity_check()

    rows, prior_avg_tps, prior_avg_lat = _load_prior(skip_drivers=set(DRIVERS))
    per_tps: dict[str, list[float]] = {}
    per_lat: dict[str, list[tuple[float, float]]] = {}

    for run in range(1, RUNS + 1):
        for driver in DRIVERS:
            tps, lat_avg, lat_p99 = await run_config(driver)
            per_tps.setdefault(driver, []).append(tps)
            per_lat.setdefault(driver, []).append((lat_avg, lat_p99))
            rows.append(
                {
                    "driver": driver,
                    "run": run,
                    "tps": f"{tps:.1f}",
                    "latency_avg_ms": f"{lat_avg:.3f}",
                    "latency_p99_ms": f"{lat_p99:.3f}",
                }
            )
            print(f"  run {run}  {DRIVER_LABELS.get(driver, driver):>11}  tps={tps:>9,.0f}  p99={lat_p99:>7.2f}ms")
            await asyncio.sleep(COOLDOWN_S)

    avg_tps = dict(prior_avg_tps)
    avg_tps.update({d: sum(v) / len(v) for d, v in per_tps.items()})
    avg_lat = dict(prior_avg_lat)
    avg_lat.update(
        {d: (sum(a for a, _ in v) / len(v), sum(p for _, p in v) / len(v)) for d, v in per_lat.items()}
    )

    _write_csv(rows, avg_tps, avg_lat)
    _print_summary(avg_tps, avg_lat)


if __name__ == "__main__":
    warnings.simplefilter("ignore", RuntimeWarning)  # Tortoise: "module has no models" при init
    asyncio.run(main())
