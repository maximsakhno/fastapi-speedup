"""Та же ручка и бизнес-логика, что app_clinic_bench_asyncpg_starlette.py, но веб-слой —
чистая ASGI-функция без Starlette/FastAPI.

Бизнес-классы, репозитории, сервисы и pydantic-модели идентичны Starlette-версии; ответ по-прежнему
сериализуется pydantic-ом (result.model_dump_json()). Меняется только веб-слой: приложение — голая
ASGI-функция app(scope, receive, send), которая сама разбирает lifespan/http, парсит query/headers и
шлёт http.response.start/body. Сравнивая RPS против app_clinic_bench_asyncpg_starlette, получаем
стоимость самого Starlette — Request/Response/Router/middleware.

Sentry включается через env: SENTRY_DSN (+ SENTRY_SAMPLE_RATE, по умолчанию 1.0).
"""

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from http import HTTPStatus
from urllib.parse import parse_qsl

import asyncpg
import sentry_sdk
from multidict import CIMultiDict
from pydantic import BaseModel
from pydantic_settings import BaseSettings

if dsn := os.environ.get("SENTRY_DSN"):
    sample_rate = float(os.environ.get("SENTRY_SAMPLE_RATE", "1.0"))
    sentry_sdk.init(dsn=dsn, sample_rate=sample_rate, traces_sample_rate=None)


# Своё исключение — без импорта фреймворка; та же сигнатура, что зовёт бизнес-код.
class HTTPException(Exception):  # noqa: N818
    def __init__(self, status_code: int, detail: object) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"HTTP {status_code}: {detail}")


# --- Доменные объекты: та же форма атрибутов, что у ORM-моделей SA-версии ---


@dataclass
class User:
    id: int
    name: str
    email: str
    role: str


@dataclass
class Patient:
    id: int
    name: str


@dataclass
class RequiredTest:
    test_type: str


@dataclass
class Procedure:
    id: int
    code: str
    name: str
    base_price: Decimal
    duration_minutes: int
    required_specialization: str
    required_tests: list[RequiredTest]


@dataclass
class Doctor:
    name: str


@dataclass
class Room:
    name: str


@dataclass
class ScheduleSlot:
    starts_at: datetime
    duration_minutes: int
    is_hot: bool
    doctor: Doctor
    room: Room


@dataclass
class PromoCode:
    id: int
    code: str
    discount_rate: Decimal
    valid_until: date
    usage_limit: int
    used_count: int
    procedure_id: int | None


@dataclass
class LoyaltyAccount:
    status: str


# --- Pydantic-модели запроса/ответа (идентичны SA-версии) ---


class SurgeryOfferParams(BaseModel):
    patient_id: int
    procedure_code: str
    preferred_date: date
    promo_code: str | None = None


class DiscountLine(BaseModel):
    reason: str
    rate: Decimal
    amount: Decimal


class PriceBreakdown(BaseModel):
    base_price: Decimal
    discounts: list[DiscountLine]
    total_discount: Decimal
    final_price: Decimal


class SurgeryOfferResponse(BaseModel):
    patient_name: str
    procedure_name: str
    doctor_name: str
    room_name: str
    starts_at: datetime
    is_hot_slot: bool
    price: PriceBreakdown
    offer_valid_until: datetime


class Settings(BaseSettings):
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "speedup-fastapi"
    db_password: str = "speedup-fastapi"
    db_name: str = "clinic"

    db_pool_size: int = 64
    db_pool_min_size: int = 16
    # Сроки годности анализов в днях
    lab_ttl_days: dict[str, int] = {
        "blood_general": 14,
        "coagulation": 14,
        "ecg": 30,
        "fluorography": 365,
    }
    default_lab_ttl_days: int = 30
    # Скидки по статусу лояльности
    loyalty_discount_rates: dict[str, Decimal] = {
        "bronze": Decimal("0.00"),
        "silver": Decimal("0.05"),
        "gold": Decimal("0.10"),
        "platinum": Decimal("0.15"),
    }
    # «Вторая процедура −50%: вы уже наш человек»
    repeat_client_discount_rate: Decimal = Decimal("0.50")
    # «Горящий слот −40%: пациент передумал, а операционная — нет»
    hot_slot_discount_rate: Decimal = Decimal("0.40")
    # Предел щедрости: суммарная скидка не больше 70%
    max_total_discount_rate: Decimal = Decimal("0.70")
    # Сколько часов действует предложение («цена заморожена, Луна — нет»)
    offer_ttl_hours: int = 24


class UserRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._conn = connection

    async def get_user_by_id(self, user_id: int) -> User | None:
        row = await self._conn.fetchrow("SELECT id, name, email, role FROM users WHERE id = $1", user_id)
        return User(**row) if row is not None else None


class PatientRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._conn = connection

    async def get_patient_by_id(self, patient_id: int) -> Patient | None:
        row = await self._conn.fetchrow("SELECT id, name FROM patients WHERE id = $1", patient_id)
        return Patient(**row) if row is not None else None


class ProcedureRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._conn = connection

    async def get_procedure_by_code(self, code: str) -> Procedure | None:
        # required_tests подтягиваем тем же одним запросом — через array_agg
        row = await self._conn.fetchrow(
            """
            SELECT p.id, p.code, p.name, p.base_price, p.duration_minutes, p.required_specialization,
                   COALESCE(array_agg(t.test_type) FILTER (WHERE t.test_type IS NOT NULL), '{}') AS required_tests
            FROM procedures p
            LEFT JOIN procedure_required_tests t ON t.procedure_id = p.id
            WHERE p.code = $1
            GROUP BY p.id
            """,
            code,
        )
        if row is None:
            return None
        return Procedure(
            id=row["id"],
            code=row["code"],
            name=row["name"],
            base_price=row["base_price"],
            duration_minutes=row["duration_minutes"],
            required_specialization=row["required_specialization"],
            required_tests=[RequiredTest(test_type=t) for t in row["required_tests"]],
        )


class MedicalRecordRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._conn = connection

    async def get_approved_consultation(self, patient_id: int, procedure_id: int) -> asyncpg.Record | None:
        return await self._conn.fetchrow(
            """
            SELECT id FROM consultations
            WHERE patient_id = $1 AND procedure_id = $2 AND approved IS TRUE
            ORDER BY held_at DESC
            LIMIT 1
            """,
            patient_id,
            procedure_id,
        )

    async def get_latest_lab_results(self, patient_id: int, test_types: list[str]) -> dict[str, date]:
        rows = await self._conn.fetch(
            """
            SELECT test_type, max(taken_at) AS taken_at
            FROM lab_results
            WHERE patient_id = $1 AND test_type = ANY($2::text[])
            GROUP BY test_type
            """,
            patient_id,
            test_types,
        )
        return {row["test_type"]: row["taken_at"] for row in rows}

    async def get_contraindication_conflicts(self, patient_id: int, procedure_id: int) -> list[str]:
        rows = await self._conn.fetch(
            """
            SELECT pc.code
            FROM patient_contraindications pc
            JOIN procedure_contraindications prc ON prc.code = pc.code
            WHERE pc.patient_id = $1 AND prc.procedure_id = $2
            """,
            patient_id,
            procedure_id,
        )
        return [row["code"] for row in rows]


class ScheduleRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._conn = connection

    async def find_free_slot(self, specialization: str, day: date, min_duration: int) -> ScheduleSlot | None:
        day_start = datetime.combine(day, time.min)
        day_end = day_start + timedelta(days=1)
        row = await self._conn.fetchrow(
            """
            SELECT s.starts_at, s.duration_minutes, s.is_hot, d.name AS doctor_name, r.name AS room_name
            FROM schedule_slots s
            JOIN doctors d ON d.id = s.doctor_id
            JOIN operating_rooms r ON r.id = s.room_id
            WHERE d.specialization = $1
              AND s.starts_at >= $2 AND s.starts_at < $3
              AND s.duration_minutes >= $4
              AND NOT EXISTS (SELECT 1 FROM surgeries su WHERE su.slot_id = s.id)
            ORDER BY s.starts_at
            LIMIT 1
            """,
            specialization,
            day_start,
            day_end,
            min_duration,
        )
        if row is None:
            return None
        return ScheduleSlot(
            starts_at=row["starts_at"],
            duration_minutes=row["duration_minutes"],
            is_hot=row["is_hot"],
            doctor=Doctor(name=row["doctor_name"]),
            room=Room(name=row["room_name"]),
        )


class BillingRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._conn = connection

    async def get_promo_code(self, code: str) -> PromoCode | None:
        row = await self._conn.fetchrow(
            """
            SELECT id, code, discount_rate, valid_until, usage_limit, used_count, procedure_id
            FROM promo_codes WHERE code = $1
            """,
            code,
        )
        return PromoCode(**row) if row is not None else None

    async def get_loyalty_account(self, patient_id: int) -> LoyaltyAccount | None:
        row = await self._conn.fetchrow("SELECT status FROM loyalty_accounts WHERE patient_id = $1", patient_id)
        return LoyaltyAccount(**row) if row is not None else None


class SurgeryRepository:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._conn = connection

    async def count_completed_surgeries(self, patient_id: int) -> int:
        return await self._conn.fetchval(
            "SELECT count(*) FROM surgeries WHERE patient_id = $1 AND status = 'completed'", patient_id
        )


class PricingService:
    """Считает смету: базовая цена минус все скидки, до которых дотянулся пациент."""

    def __init__(
        self,
        settings: Settings,
        billing_repo: BillingRepository,
        surgery_repo: SurgeryRepository,
    ) -> None:
        self._settings = settings
        self._billing_repo = billing_repo
        self._surgery_repo = surgery_repo

    async def calculate_price(
        self,
        patient: Patient,
        procedure: Procedure,
        slot: ScheduleSlot,
        promo_code: str | None,
    ) -> PriceBreakdown:
        base_price = procedure.base_price
        discounts: list[DiscountLine] = []

        # 8. Промокод от инфлюенсера
        if promo_code is not None:
            promo = await self._validate_promo_code(promo_code, procedure)
            discounts.append(self._discount(f"Промокод {promo.code}", promo.discount_rate, base_price))

        # 9. Статус в программе лояльности
        account = await self._billing_repo.get_loyalty_account(patient.id)
        loyalty_status = account.status if account is not None else "bronze"
        loyalty_rate = self._settings.loyalty_discount_rates.get(loyalty_status, Decimal("0.00"))
        if loyalty_rate > 0:
            discounts.append(self._discount(f"Статус лояльности {loyalty_status}", loyalty_rate, base_price))

        # 10. «Вторая процедура −50%» — считаем по прошлым операциям пациента
        if await self._surgery_repo.count_completed_surgeries(patient.id) > 0:
            discounts.append(
                self._discount("Вторая процедура", self._settings.repeat_client_discount_rate, base_price)
            )

        # «Горящий слот»: кто-то передумал ложиться под нож — ваш шанс
        if slot.is_hot:
            discounts.append(self._discount("Горящий слот", self._settings.hot_slot_discount_rate, base_price))

        max_discount = round(base_price * self._settings.max_total_discount_rate, 2)
        total_discount = min(sum((line.amount for line in discounts), Decimal("0.00")), max_discount)
        return PriceBreakdown(
            base_price=base_price,
            discounts=discounts,
            total_discount=total_discount,
            final_price=base_price - total_discount,
        )

    async def _validate_promo_code(self, code: str, procedure: Procedure) -> PromoCode:
        promo = await self._billing_repo.get_promo_code(code)
        if promo is None:
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Промокод не найден")
        if promo.valid_until < date.today():
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=f"Промокод истёк {promo.valid_until}")
        if promo.used_count >= promo.usage_limit:
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Промокод исчерпан")
        if promo.procedure_id is not None and promo.procedure_id != procedure.id:
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail="Промокод не действует на эту процедуру")
        return promo

    @staticmethod
    def _discount(reason: str, rate: Decimal, base_price: Decimal) -> DiscountLine:
        return DiscountLine(reason=reason, rate=rate, amount=round(base_price * rate, 2))


class SurgeryOfferService:
    """Готовит предварительное предложение: проверки, слот и смета — без брони."""

    def __init__(
        self,
        settings: Settings,
        patient_repo: PatientRepository,
        procedure_repo: ProcedureRepository,
        medical_repo: MedicalRecordRepository,
        schedule_repo: ScheduleRepository,
        pricing_service: PricingService,
    ) -> None:
        self._settings = settings
        self._patient_repo = patient_repo
        self._procedure_repo = procedure_repo
        self._medical_repo = medical_repo
        self._schedule_repo = schedule_repo
        self._pricing_service = pricing_service

    async def make_offer(self, data: SurgeryOfferParams, current_user: User) -> SurgeryOfferResponse:
        # Проверка прав доступа: предложения готовят админы и регистраторы
        if current_user.role not in ("admin", "receptionist"):
            raise HTTPException(
                status_code=HTTPStatus.FORBIDDEN, detail="Готовить предложения могут только администраторы"
            )
        if data.preferred_date <= date.today():
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail="Операцию можно запланировать только на будущую дату",
            )

        # 2. Пациент
        patient = await self._patient_repo.get_patient_by_id(data.patient_id)
        if patient is None:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Пациент не найден")

        # 3. Процедура из прайса вместе со списком требуемых анализов
        procedure = await self._procedure_repo.get_procedure_by_code(data.procedure_code)
        if procedure is None:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="Такой процедуры нет в прайсе")

        # 4. Хирург должен был одобрить процедуру на консультации
        consultation = await self._medical_repo.get_approved_consultation(patient.id, procedure.id)
        if consultation is None:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail="Сначала запишитесь на консультацию: хирург должен одобрить процедуру",
            )

        # 5. Анализы должны быть сданы и не просрочены на дату операции
        await self._check_lab_results(patient.id, procedure, data.preferred_date)

        # 6. Сверяем анамнез пациента с противопоказаниями процедуры
        conflicts = await self._medical_repo.get_contraindication_conflicts(patient.id, procedure.id)
        if conflicts:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f"Операция невозможна, противопоказания: {', '.join(sorted(conflicts))}",
            )

        # 7. Свободная операционная и хирург нужной специализации
        slot = await self._schedule_repo.find_free_slot(
            procedure.required_specialization, data.preferred_date, procedure.duration_minutes
        )
        if slot is None:
            raise HTTPException(status_code=HTTPStatus.CONFLICT, detail="На эту дату нет свободных операционных")

        # 8–10. Считаем смету со всеми скидками
        price = await self._pricing_service.calculate_price(patient, procedure, slot, data.promo_code)

        return SurgeryOfferResponse(
            patient_name=patient.name,
            procedure_name=procedure.name,
            doctor_name=slot.doctor.name,
            room_name=slot.room.name,
            starts_at=slot.starts_at,
            is_hot_slot=slot.is_hot,
            price=price,
            offer_valid_until=datetime.now() + timedelta(hours=self._settings.offer_ttl_hours),
        )

    async def _check_lab_results(self, patient_id: int, procedure: Procedure, surgery_date: date) -> None:
        required_types = [test.test_type for test in procedure.required_tests]
        latest_results = await self._medical_repo.get_latest_lab_results(patient_id, required_types)
        problems = []
        for test_type in required_types:
            taken_at = latest_results.get(test_type)
            ttl_days = self._settings.lab_ttl_days.get(test_type, self._settings.default_lab_ttl_days)
            if taken_at is None:
                problems.append(f"{test_type}: не сдан")
            elif taken_at + timedelta(days=ttl_days) < surgery_date:
                problems.append(f"{test_type}: просрочен (сдан {taken_at})")
        if problems:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail={"message": "Сначала пересдайте анализы", "tests": problems},
            )


# Авторизация — обычная async-функция (несёт логику 401), вызывается вручную из хендлера.
async def get_current_user(x_user_id: int, user_repo: UserRepository) -> User:
    user = await user_repo.get_user_by_id(x_user_id)
    if user is None:
        raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail="Сотрудник не найден")
    return user


async def handle_surgery_offer(scope: dict, send) -> None:
    qs = dict(parse_qsl(scope["query_string"].decode()))
    params = SurgeryOfferParams(
        patient_id=qs["patient_id"],
        procedure_code=qs["procedure_code"],
        preferred_date=qs["preferred_date"],
        promo_code=qs.get("promo_code"),
    )
    headers = CIMultiDict((k.decode("latin-1"), v.decode("latin-1")) for k, v in scope["headers"])
    x_user_id = int(headers["x-user-id"])

    settings = scope["state"]["settings"]
    pool = scope["state"]["pool"]
    async with pool.acquire() as conn:
        user_repo = UserRepository(conn)
        patient_repo = PatientRepository(conn)
        procedure_repo = ProcedureRepository(conn)
        medical_repo = MedicalRecordRepository(conn)
        schedule_repo = ScheduleRepository(conn)
        billing_repo = BillingRepository(conn)
        surgery_repo = SurgeryRepository(conn)
        pricing_service = PricingService(settings, billing_repo, surgery_repo)
        offer_service = SurgeryOfferService(
            settings, patient_repo, procedure_repo, medical_repo, schedule_repo, pricing_service
        )
        current_user = await get_current_user(x_user_id, user_repo)
        result = await offer_service.make_offer(params, current_user)
    body = result.model_dump_json().encode()
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"application/json")]})
    await send({"type": "http.response.body", "body": body})


async def app(scope: dict, receive, send) -> None:
    if scope["type"] == "lifespan":
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                settings = Settings()
                pool = await asyncpg.create_pool(
                    host=settings.db_host,
                    port=settings.db_port,
                    user=settings.db_user,
                    password=settings.db_password,
                    database=settings.db_name,
                    min_size=settings.db_pool_min_size,
                    max_size=settings.db_pool_size,
                )
                scope["state"]["settings"] = settings
                scope["state"]["pool"] = pool
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await scope["state"]["pool"].close()
                await send({"type": "lifespan.shutdown.complete"})
                return
    elif scope["type"] == "http":
        try:
            if scope["method"] == "GET" and scope["path"] == "/surgery-offer":
                await handle_surgery_offer(scope, send)
            else:
                await send(
                    {"type": "http.response.start", "status": 404, "headers": [(b"content-type", b"text/plain")]}
                )
                await send({"type": "http.response.body", "body": b"Not Found"})
        except HTTPException as e:
            body = json.dumps({"detail": e.detail}, ensure_ascii=False).encode()
            await send(
                {
                    "type": "http.response.start",
                    "status": e.status_code,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await send({"type": "http.response.body", "body": body})
    else:
        raise RuntimeError("This server doesn't support WebSocket.")
