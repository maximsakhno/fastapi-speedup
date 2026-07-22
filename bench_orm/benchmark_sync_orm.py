"""Синхронный микробенчмарк TPS сценария `/surgery-offer`: 10 запросов happy-path на sync-ORM.

Зеркало async-версии (benchmark_async_orm.py), но БЕЗ asyncio: те же 10 запросов успешного
`GET /surgery-offer` в одной транзакции крутят 64 обычных ПОТОКА с блокирующим синхронным
ORM-кодом. Эталон — сырой синхронный драйвер Postgres, который эти ORM используют под капотом:

    psycopg2    — сырой SQL (эталон, 10 запросов);
    sqlalchemy  — SQLAlchemy sync ORM (модели из bench_clinic/, драйвер psycopg2, 10 запросов);
    django      — Django ORM sync    (scenario_django.py, драйвер psycopg3, 11 запросов);
    peewee      — Peewee             (scenario_peewee.py, драйвер psycopg2, 11 запросов).

64 потока держат по своему соединению (=64 соединения) и гоняют сценарий-транзакцию в цикле;
меряем транзакции в секунду (TPS). Pony ORM исключён (не поддерживает Python 3.14).

GIL: psycopg2/psycopg3 отпускают GIL на время сетевых вызовов libpq, поэтому потоки реально
перекрываются на ожидании БД (как синхронный ORM в async-приложении через пул потоков).

Запуск (из корня репозитория):
    .venv/bin/python -m bench_orm.benchmark_sync_orm
Подмножество: DRIVERS=peewee,psycopg2 .venv/bin/python -m bench_orm.benchmark_sync_orm
Дым-тест:     DURATION_S=1 WARMUP_S=1 RUNS=1 .venv/bin/python -m bench_orm.benchmark_sync_orm
"""

from __future__ import annotations

import csv
import os
import sys
import threading
import time
from datetime import date, datetime, time as dtime, timedelta

import psycopg2
from peewee import PostgresqlDatabase
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session, contains_eager, joinedload

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
from bench_orm import scenario_peewee

# --- Параметры подключения ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_USER = os.environ.get("DB_USER", "speedup-fastapi")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "speedup-fastapi")
DB_NAME = os.environ.get("DB_NAME", "clinic")

PSYCOPG2_DSN = f"host={DB_HOST} port={DB_PORT} user={DB_USER} password={DB_PASSWORD} dbname={DB_NAME}"
SA_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Параметры нагрузки ---
POOL_SIZE = int(os.environ.get("POOL_SIZE", "64"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "64"))
WARMUP_S = float(os.environ.get("WARMUP_S", "3"))
DURATION_S = float(os.environ.get("DURATION_S", "10"))
RUNS = int(os.environ.get("RUNS", "3"))
COOLDOWN_S = float(os.environ.get("COOLDOWN_S", "1"))

# --- Happy-path вход (тот же, что даёт HTTP 200) ---
PARAMS = {
    "user_id": int(os.environ.get("USER_ID", "1")),
    "patient_id": int(os.environ.get("PATIENT_ID", "1")),
    "procedure_code": os.environ.get("PROCEDURE_CODE", "rhinoplasty"),
    "promo_code": os.environ.get("PROMO_CODE", "NOSIK15"),
    "preferred_date": date.today() + timedelta(days=int(os.environ.get("DAYS_AHEAD", "7"))),
}

ALL_DRIVERS = ("psycopg2", "sqlalchemy", "django", "peewee")
DRIVERS = tuple(os.environ.get("DRIVERS", ",".join(ALL_DRIVERS)).split(","))

DRIVER_LABELS = {
    "psycopg2": "psycopg2",
    "sqlalchemy": "SQLAlchemy",
    "django": "Django",
    "peewee": "Peewee",
}
QUERIES_PER_TXN = {"psycopg2": 10, "sqlalchemy": 10, "django": 11, "peewee": 11}

CSV_PATH = "scenario_sync_benchmark_results.csv"

# Django настраивается один раз на процесс.
_django_module = None
_django_connections = None


# --- Сценарии уровня данных ---

_PROCEDURE_SQL = """
    SELECT p.id, p.code, p.name, p.base_price, p.duration_minutes, p.required_specialization,
           COALESCE(array_agg(t.test_type) FILTER (WHERE t.test_type IS NOT NULL), '{}') AS required_tests
    FROM procedures p
    LEFT JOIN procedure_required_tests t ON t.procedure_id = p.id
    WHERE p.code = %s
    GROUP BY p.id
"""
_SLOT_SQL = """
    SELECT s.starts_at, s.duration_minutes, s.is_hot, d.name AS doctor_name, r.name AS room_name
    FROM schedule_slots s
    JOIN doctors d ON d.id = s.doctor_id
    JOIN operating_rooms r ON r.id = s.room_id
    WHERE d.specialization = %s
      AND s.starts_at >= %s AND s.starts_at < %s
      AND s.duration_minutes >= %s
      AND NOT EXISTS (SELECT 1 FROM surgeries su WHERE su.slot_id = s.id)
    ORDER BY s.starts_at LIMIT 1
"""


def psycopg2_scenario(conn) -> tuple:
    """Те же 10 запросов, что в app_clinic_bench_asyncpg.py, сырым SQL (psycopg2) в одной транзакции."""
    pid = PARAMS["patient_id"]
    with conn, conn.cursor() as cur:
        cur.execute("SELECT id, name, email, role FROM users WHERE id = %s", (PARAMS["user_id"],))
        cur.fetchone()
        cur.execute("SELECT id, name FROM patients WHERE id = %s", (pid,))
        patient = cur.fetchone()
        cur.execute(_PROCEDURE_SQL, (PARAMS["procedure_code"],))
        procedure = cur.fetchone()
        proc_id, proc_name = procedure[0], procedure[2]
        duration_minutes, specialization, test_types = procedure[4], procedure[5], procedure[6]
        cur.execute(
            "SELECT id FROM consultations WHERE patient_id = %s AND procedure_id = %s AND approved IS TRUE "
            "ORDER BY held_at DESC LIMIT 1",
            (pid, proc_id),
        )
        cur.fetchone()
        cur.execute(
            "SELECT test_type, max(taken_at) AS taken_at FROM lab_results "
            "WHERE patient_id = %s AND test_type = ANY(%s) GROUP BY test_type",
            (pid, test_types),
        )
        cur.fetchall()
        cur.execute(
            "SELECT pc.code FROM patient_contraindications pc "
            "JOIN procedure_contraindications prc ON prc.code = pc.code "
            "WHERE pc.patient_id = %s AND prc.procedure_id = %s",
            (pid, proc_id),
        )
        cur.fetchall()
        day_start = datetime.combine(PARAMS["preferred_date"], dtime.min)
        day_end = day_start + timedelta(days=1)
        cur.execute(_SLOT_SQL, (specialization, day_start, day_end, duration_minutes))
        slot = cur.fetchone()
        cur.execute(
            "SELECT id, code, discount_rate, valid_until, usage_limit, used_count, procedure_id "
            "FROM promo_codes WHERE code = %s",
            (PARAMS["promo_code"],),
        )
        promo = cur.fetchone()
        cur.execute("SELECT status FROM loyalty_accounts WHERE patient_id = %s", (pid,))
        cur.fetchone()
        cur.execute("SELECT count(*) FROM surgeries WHERE patient_id = %s AND status = 'completed'", (pid,))
        cur.fetchone()
    return (patient[1], proc_name, slot is not None, promo is not None)


def sqlalchemy_scenario(session: Session) -> tuple:
    """Те же 10 запросов через SQLAlchemy sync ORM (модели/запросы как в репозиториях приложений клиники)."""
    pid = PARAMS["patient_id"]
    with session.begin():
        session.execute(select(SAUser).where(SAUser.id == PARAMS["user_id"]))
        patient = session.execute(select(SAPatient).where(SAPatient.id == pid)).scalar_one_or_none()
        procedure = (
            session.execute(
                select(Procedure)
                .where(Procedure.code == PARAMS["procedure_code"])
                .options(joinedload(Procedure.required_tests))
            )
            .unique()
            .scalar_one_or_none()
        )
        session.execute(
            select(Consultation)
            .where(
                Consultation.patient_id == pid,
                Consultation.procedure_id == procedure.id,
                Consultation.approved.is_(True),
            )
            .order_by(Consultation.held_at.desc())
            .limit(1)
        ).scalars().first()
        test_types = [t.test_type for t in procedure.required_tests]
        session.execute(
            select(LabResult.test_type, func.max(LabResult.taken_at))
            .where(LabResult.patient_id == pid, LabResult.test_type.in_(test_types))
            .group_by(LabResult.test_type)
        ).all()
        session.execute(
            select(PatientContraindication.code)
            .join(ProcedureContraindication, ProcedureContraindication.code == PatientContraindication.code)
            .where(
                PatientContraindication.patient_id == pid,
                ProcedureContraindication.procedure_id == procedure.id,
            )
        ).scalars().all()
        day_start = datetime.combine(PARAMS["preferred_date"], dtime.min)
        day_end = day_start + timedelta(days=1)
        slot_is_taken = select(Surgery.id).where(Surgery.slot_id == ScheduleSlot.id).exists()
        slot = session.execute(
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
        ).scalars().first()
        promo = session.execute(
            select(PromoCode).where(PromoCode.code == PARAMS["promo_code"])
        ).scalar_one_or_none()
        session.execute(select(LoyaltyAccount).where(LoyaltyAccount.patient_id == pid))
        session.execute(
            select(func.count(Surgery.id)).where(Surgery.patient_id == pid, Surgery.status == "completed")
        ).scalar_one()
        # Результат собираем ВНУТРИ транзакции (после commit атрибуты истекают).
        result = (patient.name, procedure.name, slot is not None, promo is not None)
    return result


# --- Ресурс psycopg2: соединение на поток ---


class Psycopg2Pool:
    """По одному соединению psycopg2 на поток (thread-local); закрытие — из самого потока."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._local = threading.local()

    def conn(self):
        c = getattr(self._local, "conn", None)
        if c is None:
            c = psycopg2.connect(self._dsn)
            self._local.conn = c
        return c

    def close_current(self) -> None:
        c = getattr(self._local, "conn", None)
        if c is not None:
            c.close()
            self._local.conn = None


# --- Потоковый движок нагрузки ---


def _percentiles(latencies: list[float]) -> tuple[float, float]:
    if not latencies:
        return 0.0, 0.0
    lats = sorted(latencies)
    avg = sum(lats) / len(lats) * 1000
    p99 = lats[min(len(lats) - 1, int(len(lats) * 0.99))] * 1000
    return avg, p99


def run_window(unit, thread_teardown=None) -> tuple[float, float, float]:
    """Warmup + окно замера на CONCURRENCY потоках. unit() — одна сценарий-транзакция."""
    ctrl = {"deadline": 0.0, "measuring": False}
    results: list[tuple[int, list[float]]] = []
    results_lock = threading.Lock()
    ready = threading.Barrier(CONCURRENCY + 1)

    def thread_fn() -> None:
        count = 0
        latencies: list[float] = []
        ready.wait()
        while time.monotonic() < ctrl["deadline"]:
            t0 = time.monotonic()
            unit()
            if ctrl["measuring"]:
                count += 1
                latencies.append(time.monotonic() - t0)
        if thread_teardown is not None:
            thread_teardown()
        with results_lock:
            results.append((count, latencies))

    threads = [threading.Thread(target=thread_fn) for _ in range(CONCURRENCY)]
    for t in threads:
        t.start()

    ctrl["deadline"] = time.monotonic() + WARMUP_S + DURATION_S
    ready.wait()  # все потоки + main стартуют одновременно
    time.sleep(WARMUP_S)
    measure_start = time.monotonic()
    ctrl["measuring"] = True
    time.sleep(DURATION_S)
    ctrl["measuring"] = False
    measure_elapsed = time.monotonic() - measure_start

    for t in threads:
        t.join()

    total = sum(c for c, _ in results)
    all_lats = [lat for _, lats in results for lat in lats]
    tps = total / measure_elapsed if measure_elapsed else 0.0
    lat_avg, lat_p99 = _percentiles(all_lats)
    return tps, lat_avg, lat_p99


# --- Django setup/teardown ---


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


# --- Запуск конфигурации драйвера ---


def run_config(driver: str) -> tuple[float, float, float]:
    if driver == "psycopg2":
        pool = Psycopg2Pool(PSYCOPG2_DSN)
        return run_window(lambda: psycopg2_scenario(pool.conn()), thread_teardown=pool.close_current)
    if driver == "sqlalchemy":
        engine = create_engine(SA_URL, pool_size=CONCURRENCY, max_overflow=0)
        try:
            return run_window(lambda: sqlalchemy_scenario(Session(engine)))
        finally:
            engine.dispose()
    if driver == "django":
        _django_setup()
        return run_window(
            lambda: _django_module.scenario(PARAMS),
            thread_teardown=lambda: _django_connections.close_all(),
        )
    if driver == "peewee":
        db = PostgresqlDatabase(DB_NAME, host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
        scenario_peewee.database_proxy.initialize(db)
        return run_window(lambda: scenario_peewee.scenario(db, PARAMS), thread_teardown=db.close)
    raise ValueError(f"Неизвестный драйвер: {driver}")


# --- Sanity-check (по одному разу, в главном потоке) ---


def _sanity_one(driver: str) -> tuple:
    if driver == "psycopg2":
        conn = psycopg2.connect(PSYCOPG2_DSN)
        try:
            return psycopg2_scenario(conn)
        finally:
            conn.close()
    if driver == "sqlalchemy":
        engine = create_engine(SA_URL, pool_size=1, max_overflow=0)
        try:
            return sqlalchemy_scenario(Session(engine))
        finally:
            engine.dispose()
    if driver == "django":
        _django_setup()
        try:
            return _django_module.scenario(PARAMS)
        finally:
            _django_connections.close_all()
    if driver == "peewee":
        db = PostgresqlDatabase(DB_NAME, host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
        scenario_peewee.database_proxy.initialize(db)
        try:
            return scenario_peewee.scenario(db, PARAMS)
        finally:
            db.close()
    raise ValueError(driver)


def sanity_check() -> None:
    print("Sanity-check (patient.name, procedure.name, slot_found, promo_found):")
    results = {}
    for driver in DRIVERS:
        res = _sanity_one(driver)
        results[driver] = res
        ok = res[2] and res[3] and res[0] and res[1]
        print(f"  {DRIVER_LABELS.get(driver, driver):>11}: {res}  {'OK' if ok else 'ПРОВАЛ'}")
        if not ok:
            print(
                f"\nОШИБКА: сценарий {driver} не прошёл happy-path (slot/promo не найдены). "
                f"Проверь PATIENT_ID/PROCEDURE_CODE/PROMO_CODE.",
                file=sys.stderr,
            )
            sys.exit(1)
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
    base = avg_tps.get("psycopg2", 0.0)
    print("\n" + "=" * 74)
    print(f"{'драйвер':>11}  {'TPS':>10}  {'avg ms':>8}  {'p99 ms':>8}  {'доля psycopg2':>14}  {'SQL/txn':>7}")
    print("-" * 74)
    for driver in ALL_DRIVERS:
        if driver not in avg_tps:
            continue
        tps = avg_tps[driver]
        lat_avg, lat_p99 = avg_lat[driver]
        share = f"{tps / base * 100:5.1f}%" if base else "  n/a"
        print(
            f"{DRIVER_LABELS.get(driver, driver):>11}  {tps:>10,.0f}  {lat_avg:>8.2f}  {lat_p99:>8.2f}  "
            f"{share:>14}  {QUERIES_PER_TXN.get(driver, '?'):>7}"
        )
    print("=" * 74)
    print(
        "Сценарий = 10 логических запросов успешного /surgery-offer в одной транзакции, 64 потока.\n"
        "У ORM фактических запросов 11: required_tests тянется отдельным запросом.\n"
        "Драйверы: psycopg2/SQLAlchemy/Peewee → psycopg2; Django → psycopg3."
    )


def main() -> None:
    print(
        f"Синхронный бенчмарк /surgery-offer: threads={CONCURRENCY}, "
        f"warmup={WARMUP_S}s, window={DURATION_S}s, runs={RUNS}, drivers={','.join(DRIVERS)}"
    )
    print(f"Вход: {PARAMS}\n")

    sanity_check()

    rows, prior_avg_tps, prior_avg_lat = _load_prior(skip_drivers=set(DRIVERS))
    per_tps: dict[str, list[float]] = {}
    per_lat: dict[str, list[tuple[float, float]]] = {}

    for run in range(1, RUNS + 1):
        for driver in DRIVERS:
            tps, lat_avg, lat_p99 = run_config(driver)
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
            time.sleep(COOLDOWN_S)

    avg_tps = dict(prior_avg_tps)
    avg_tps.update({d: sum(v) / len(v) for d, v in per_tps.items()})
    avg_lat = dict(prior_avg_lat)
    avg_lat.update(
        {d: (sum(a for a, _ in v) / len(v), sum(p for _, p in v) / len(v)) for d, v in per_lat.items()}
    )

    _write_csv(rows, avg_tps, avg_lat)
    _print_summary(avg_tps, avg_lat)


if __name__ == "__main__":
    main()
