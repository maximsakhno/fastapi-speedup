"""Бенчмарк-вариант №1: SQLAlchemy + СИНХРОННЫЕ зависимости.

Отправная точка серии: провайдеры-конструкторы объектов объявлены обычными `def`.
Такие sync-зависимости FastAPI выполняет в threadpool (anyio) — это измеряемый антипаттерн.
Провайдеры, которые реально await-ят (get_session, get_current_user), остаются async.

Этот файл — ещё и источник схемы БД: seed_clinic.py и бенчмарки bench_orm импортируют
отсюда SQLAlchemy-модели, поэтому засеянная база гарантированно совпадает с приложениями.

Sentry включается через env: SENTRY_DSN (+ SENTRY_SAMPLE_RATE, по умолчанию 1.0).
Запуск и команды замеров — см. README.md.
"""

import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from http import HTTPStatus
from typing import Annotated, TypedDict, cast

import sentry_sdk
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    contains_eager,
    joinedload,
    mapped_column,
    relationship,
)

if dsn := os.environ.get("SENTRY_DSN"):
    sample_rate = float(os.environ.get("SENTRY_SAMPLE_RATE", "1.0"))
    sentry_sdk.init(dsn=dsn, sample_rate=sample_rate, traces_sample_rate=None)


class Base(DeclarativeBase):
    pass


class User(Base):
    """Сотрудники клиники: админы, регистраторы, хирурги."""

    __tablename__ = "users"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)  # admin | receptionist | surgeon


class Doctor(Base):
    __tablename__ = "doctors"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    specialization: Mapped[str] = mapped_column(String)  # facial | body


class OperatingRoom(Base):
    __tablename__ = "operating_rooms"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)


class Procedure(Base):
    """Каталог процедур: ринопластика, липосакция и прочие услуги из прайса."""

    __tablename__ = "procedures"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String, unique=True)
    name: Mapped[str] = mapped_column(String)
    base_price: Mapped[Decimal] = mapped_column(Numeric)
    duration_minutes: Mapped[int] = mapped_column(Integer)
    required_specialization: Mapped[str] = mapped_column(String)
    required_tests: Mapped[list["ProcedureRequiredTest"]] = relationship()


class ProcedureRequiredTest(Base):
    """Какие анализы нужны для процедуры."""

    __tablename__ = "procedure_required_tests"
    procedure_id: Mapped[int] = mapped_column(ForeignKey("procedures.id"), primary_key=True)
    test_type: Mapped[str] = mapped_column(String, primary_key=True)


class ProcedureContraindication(Base):
    """При каких противопоказаниях процедура запрещена."""

    __tablename__ = "procedure_contraindications"
    procedure_id: Mapped[int] = mapped_column(ForeignKey("procedures.id"), primary_key=True)
    code: Mapped[str] = mapped_column(String, primary_key=True)


class PatientContraindication(Base):
    """Противопоказания пациента из анамнеза."""

    __tablename__ = "patient_contraindications"
    patient_id: Mapped[int] = mapped_column(ForeignKey("patients.id"), primary_key=True)
    code: Mapped[str] = mapped_column(String, primary_key=True)


class Consultation(Base):
    """Первичная консультация: хирург одобряет (или нет) процедуру."""

    __tablename__ = "consultations"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    patient_id: Mapped[int] = mapped_column(ForeignKey("patients.id"))
    doctor_id: Mapped[int] = mapped_column(ForeignKey("doctors.id"))
    procedure_id: Mapped[int] = mapped_column(ForeignKey("procedures.id"))
    approved: Mapped[bool] = mapped_column(Boolean)
    held_at: Mapped[datetime] = mapped_column(DateTime)


class LabResult(Base):
    """Сданные анализы: у каждого свой срок годности (задаётся в настройках)."""

    __tablename__ = "lab_results"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    patient_id: Mapped[int] = mapped_column(ForeignKey("patients.id"))
    test_type: Mapped[str] = mapped_column(String)
    taken_at: Mapped[date] = mapped_column(Date)


class ScheduleSlot(Base):
    """Слот в расписании: операционная + хирург + время. Горящие слоты со скидкой."""

    __tablename__ = "schedule_slots"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    room_id: Mapped[int] = mapped_column(ForeignKey("operating_rooms.id"))
    doctor_id: Mapped[int] = mapped_column(ForeignKey("doctors.id"))
    starts_at: Mapped[datetime] = mapped_column(DateTime)
    duration_minutes: Mapped[int] = mapped_column(Integer)
    is_hot: Mapped[bool] = mapped_column(Boolean, default=False)
    doctor: Mapped[Doctor] = relationship()
    room: Mapped[OperatingRoom] = relationship()


class PromoCode(Base):
    """Промокоды от инфлюенсеров: NOSIK15, KRASOTKA50 и другие."""

    __tablename__ = "promo_codes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String, unique=True)
    discount_rate: Mapped[Decimal] = mapped_column(Numeric)
    valid_until: Mapped[date] = mapped_column(Date)
    usage_limit: Mapped[int] = mapped_column(Integer)
    used_count: Mapped[int] = mapped_column(Integer, default=0)
    procedure_id: Mapped[int | None] = mapped_column(ForeignKey("procedures.id"), nullable=True)


class LoyaltyAccount(Base):
    """Программа лояльности: каждая шестая операция… ну вы поняли."""

    __tablename__ = "loyalty_accounts"
    patient_id: Mapped[int] = mapped_column(ForeignKey("patients.id"), primary_key=True)
    status: Mapped[str] = mapped_column(String)  # bronze | silver | gold | platinum
    points: Mapped[int] = mapped_column(Integer, default=0)


class Surgery(Base):
    """История операций: по ней считается скидка «вторая процедура»."""

    __tablename__ = "surgeries"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    patient_id: Mapped[int] = mapped_column(ForeignKey("patients.id"))
    procedure_id: Mapped[int] = mapped_column(ForeignKey("procedures.id"))
    slot_id: Mapped[int] = mapped_column(ForeignKey("schedule_slots.id"))
    status: Mapped[str] = mapped_column(String)  # planned | completed | cancelled
    base_price: Mapped[Decimal] = mapped_column(Numeric)
    discount_amount: Mapped[Decimal] = mapped_column(Numeric)
    final_price: Mapped[Decimal] = mapped_column(Numeric)
    created_at: Mapped[datetime] = mapped_column(DateTime)


class Patient(Base):
    __tablename__ = "patients"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    birth_date: Mapped[date] = mapped_column(Date)
    phone: Mapped[str] = mapped_column(String)


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
    db_max_overflow: int = 0
    db_echo: bool = False
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
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_user_by_id(self, user_id: int) -> User | None:
        result = await self._session.execute(select(User).where(User.id == user_id))
        return result.scalar_one_or_none()


class PatientRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_patient_by_id(self, patient_id: int) -> Patient | None:
        result = await self._session.execute(select(Patient).where(Patient.id == patient_id))
        return result.scalar_one_or_none()


class ProcedureRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_procedure_by_code(self, code: str) -> Procedure | None:
        result = await self._session.execute(
            select(Procedure).where(Procedure.code == code).options(joinedload(Procedure.required_tests))
        )
        return result.unique().scalar_one_or_none()


class MedicalRecordRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_approved_consultation(self, patient_id: int, procedure_id: int) -> Consultation | None:
        result = await self._session.execute(
            select(Consultation)
            .where(
                Consultation.patient_id == patient_id,
                Consultation.procedure_id == procedure_id,
                Consultation.approved.is_(True),
            )
            .order_by(Consultation.held_at.desc())
            .limit(1)
        )
        return result.scalars().first()

    async def get_latest_lab_results(self, patient_id: int, test_types: list[str]) -> dict[str, date]:
        result = await self._session.execute(
            select(LabResult.test_type, func.max(LabResult.taken_at).label("taken_at"))
            .where(LabResult.patient_id == patient_id, LabResult.test_type.in_(test_types))
            .group_by(LabResult.test_type)
        )
        return {row.test_type: row.taken_at for row in result}

    async def get_contraindication_conflicts(self, patient_id: int, procedure_id: int) -> list[str]:
        result = await self._session.execute(
            select(PatientContraindication.code)
            .join(
                ProcedureContraindication,
                ProcedureContraindication.code == PatientContraindication.code,
            )
            .where(
                PatientContraindication.patient_id == patient_id,
                ProcedureContraindication.procedure_id == procedure_id,
            )
        )
        return list(result.scalars().all())


class ScheduleRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find_free_slot(self, specialization: str, day: date, min_duration: int) -> ScheduleSlot | None:
        day_start = datetime.combine(day, time.min)
        day_end = day_start + timedelta(days=1)
        slot_is_taken = select(Surgery.id).where(Surgery.slot_id == ScheduleSlot.id).exists()
        result = await self._session.execute(
            select(ScheduleSlot)
            .join(ScheduleSlot.doctor)
            .where(
                Doctor.specialization == specialization,
                ScheduleSlot.starts_at >= day_start,
                ScheduleSlot.starts_at < day_end,
                ScheduleSlot.duration_minutes >= min_duration,
                ~slot_is_taken,
            )
            .options(contains_eager(ScheduleSlot.doctor), joinedload(ScheduleSlot.room))
            .order_by(ScheduleSlot.starts_at)
            .limit(1)
        )
        return result.scalars().first()


class BillingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_promo_code(self, code: str) -> PromoCode | None:
        result = await self._session.execute(select(PromoCode).where(PromoCode.code == code))
        return result.scalar_one_or_none()

    async def get_loyalty_account(self, patient_id: int) -> LoyaltyAccount | None:
        result = await self._session.execute(select(LoyaltyAccount).where(LoyaltyAccount.patient_id == patient_id))
        return result.scalar_one_or_none()


class SurgeryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def count_completed_surgeries(self, patient_id: int) -> int:
        result = await self._session.execute(
            select(func.count(Surgery.id)).where(Surgery.patient_id == patient_id, Surgery.status == "completed")
        )
        return result.scalar_one()


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


class State(TypedDict):
    settings: Settings
    session_factory: async_sessionmaker


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[State]:
    settings = Settings()

    engine = create_async_engine(
        url=f"postgresql+asyncpg://{settings.db_user}:{settings.db_password}@{settings.db_host}:{settings.db_port}/{settings.db_name}",
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        echo=settings.db_echo,
    )
    session_factory = async_sessionmaker(engine)
    try:
        yield {
            "settings": settings,
            "session_factory": session_factory,
        }
    finally:
        await engine.dispose()


def get_state(request: Request) -> State:
    return cast(State, request.state)


def get_settings(state: Annotated[State, Depends(get_state)]) -> Settings:
    return state["settings"]


def get_session_factory(state: Annotated[State, Depends(get_state)]) -> async_sessionmaker:
    return state["session_factory"]


async def get_session(
    session_factory: Annotated[async_sessionmaker, Depends(get_session_factory)],
) -> AsyncIterator[AsyncSession]:
    async with session_factory() as session:
        yield session


def get_user_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> UserRepository:
    return UserRepository(session)


def get_patient_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> PatientRepository:
    return PatientRepository(session)


def get_procedure_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> ProcedureRepository:
    return ProcedureRepository(session)


def get_medical_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> MedicalRecordRepository:
    return MedicalRecordRepository(session)


def get_schedule_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> ScheduleRepository:
    return ScheduleRepository(session)


def get_billing_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> BillingRepository:
    return BillingRepository(session)


def get_surgery_repository(session: Annotated[AsyncSession, Depends(get_session)]) -> SurgeryRepository:
    return SurgeryRepository(session)


async def get_current_user(
    x_user_id: Annotated[int, Header(..., alias="X-User-Id")],
    user_repo: Annotated[UserRepository, Depends(get_user_repository)],
) -> User:
    # 1. Авторизация: ищем сотрудника по заголовку
    user = await user_repo.get_user_by_id(x_user_id)
    if user is None:
        raise HTTPException(status_code=HTTPStatus.UNAUTHORIZED, detail="Сотрудник не найден")
    return user


def get_pricing_service(
    settings: Annotated[Settings, Depends(get_settings)],
    billing_repo: Annotated[BillingRepository, Depends(get_billing_repository)],
    surgery_repo: Annotated[SurgeryRepository, Depends(get_surgery_repository)],
) -> PricingService:
    return PricingService(settings, billing_repo, surgery_repo)


def get_surgery_offer_service(
    settings: Annotated[Settings, Depends(get_settings)],
    patient_repo: Annotated[PatientRepository, Depends(get_patient_repository)],
    procedure_repo: Annotated[ProcedureRepository, Depends(get_procedure_repository)],
    medical_repo: Annotated[MedicalRecordRepository, Depends(get_medical_repository)],
    schedule_repo: Annotated[ScheduleRepository, Depends(get_schedule_repository)],
    pricing_service: Annotated[PricingService, Depends(get_pricing_service)],
) -> SurgeryOfferService:
    return SurgeryOfferService(settings, patient_repo, procedure_repo, medical_repo, schedule_repo, pricing_service)


app = FastAPI(
    title="Клиника пластической хирургии «До и После» — bench sync deps",
    description="Делай это, чтобы ускорить FastAPI в 10 раз",
    lifespan=lifespan,
)


@app.get("/surgery-offer")
async def get_surgery_offer(
    data: Annotated[SurgeryOfferParams, Query()],
    current_user: Annotated[User, Depends(get_current_user)],
    offer_service: Annotated[SurgeryOfferService, Depends(get_surgery_offer_service)],
) -> SurgeryOfferResponse:
    return await offer_service.make_offer(data, current_user)
