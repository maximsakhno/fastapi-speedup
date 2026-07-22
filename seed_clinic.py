"""Создание и наполнение БД клиники «До и После».

Создаёт базу clinic (если её нет), пересоздаёт таблицы и наполняет данными.
Первые четыре пациента — детерминированные сценарии для демо, остальные случайные.
"""

import asyncio
import random
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from faker import Faker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# Схема берётся из эталонного приложения клиники (вариант №1) — так засеянная БД
# гарантированно совпадает с тем, что ожидают все бенчмарк-приложения.
from bench_clinic.app_clinic_bench_sync import (
    Base,
    Consultation,
    Doctor,
    LabResult,
    LoyaltyAccount,
    OperatingRoom,
    Patient,
    PatientContraindication,
    Procedure,
    ProcedureContraindication,
    ProcedureRequiredTest,
    PromoCode,
    ScheduleSlot,
    Surgery,
    User,
)

# Подключение к «служебной» БД, чтобы создать базу clinic
ADMIN_DATABASE_URL = "postgresql+asyncpg://speedup-fastapi:speedup-fastapi@localhost:5432/speedup-fastapi"
DATABASE_URL = "postgresql+asyncpg://speedup-fastapi:speedup-fastapi@localhost:5432/clinic"
DB_NAME = "clinic"

fake = Faker("ru_RU")
Faker.seed(42)
random.seed(42)

# Конфигурация генерации
NUM_PATIENTS = 500
NUM_STAFF_USERS = 10
NUM_ROOMS = 3
SLOTS_DAYS_AHEAD = 30
SLOT_HOURS = [9, 11, 13, 15, 17]
SLOT_DURATIONS = [120, 180, 240]
HOT_SLOT_PROBABILITY = 0.1
CONTRAINDICATION_PROBABILITY = 0.15
CONSULTATION_PROBABILITY = 0.7
COMPLETED_SURGERY_PROBABILITY = 0.25
LOYALTY_STATUSES = ["bronze", "silver", "gold", "platinum"]
LOYALTY_WEIGHTS = [0.60, 0.25, 0.12, 0.03]

TEST_TYPES = ["blood_general", "coagulation", "ecg", "fluorography"]
CONTRAINDICATION_CODES = ["diabetes", "heart_disease", "blood_clotting_disorder", "nicotine_addiction"]

PROCEDURES = [
    {
        "code": "rhinoplasty",
        "name": "Ринопластика",
        "base_price": Decimal("250000.00"),
        "duration_minutes": 120,
        "required_specialization": "facial",
        "required_tests": TEST_TYPES,
        "contraindications": ["blood_clotting_disorder", "nicotine_addiction"],
    },
    {
        "code": "blepharoplasty",
        "name": "Блефаропластика",
        "base_price": Decimal("120000.00"),
        "duration_minutes": 60,
        "required_specialization": "facial",
        "required_tests": ["blood_general", "coagulation", "ecg"],
        "contraindications": ["blood_clotting_disorder", "diabetes"],
    },
    {
        "code": "otoplasty",
        "name": "Отопластика",
        "base_price": Decimal("90000.00"),
        "duration_minutes": 60,
        "required_specialization": "facial",
        "required_tests": ["blood_general", "coagulation"],
        "contraindications": ["blood_clotting_disorder"],
    },
    {
        "code": "liposuction",
        "name": "Липосакция",
        "base_price": Decimal("180000.00"),
        "duration_minutes": 90,
        "required_specialization": "body",
        "required_tests": TEST_TYPES,
        "contraindications": ["blood_clotting_disorder", "diabetes", "heart_disease"],
    },
    {
        "code": "abdominoplasty",
        "name": "Абдоминопластика",
        "base_price": Decimal("300000.00"),
        "duration_minutes": 150,
        "required_specialization": "body",
        "required_tests": TEST_TYPES,
        "contraindications": ["blood_clotting_disorder", "diabetes", "heart_disease"],
    },
]

DOCTORS = [
    ("facial", 4),  # 4 хирурга по лицу
    ("body", 4),  # 4 хирурга по телу
]


async def create_database() -> None:
    """Создаём базу clinic, если её ещё нет."""
    admin_engine = create_async_engine(ADMIN_DATABASE_URL, isolation_level="AUTOCOMMIT")
    async with admin_engine.connect() as conn:
        exists = await conn.execute(text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": DB_NAME})
        if exists.scalar() is None:
            await conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
            print(f"База данных {DB_NAME} создана")
    await admin_engine.dispose()


async def create_tables(engine) -> None:
    """Пересоздаём таблицы для чистоты эксперимента."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)


async def seed_users(session: AsyncSession) -> list[User]:
    """Сотрудники клиники. Первые трое — детерминированные для демо."""
    users = [
        User(name="Главврачёв Админ Батькович", email="admin@do-i-posle.ru", role="admin"),
        User(name="Регистратурова Милана", email="front-desk@do-i-posle.ru", role="receptionist"),
        User(name="Скальпелев Ланцет Иглович", email="surgeon@do-i-posle.ru", role="surgeon"),
    ]
    for _ in range(NUM_STAFF_USERS - len(users)):
        users.append(User(name=fake.name(), email=fake.unique.email(), role="receptionist"))
    session.add_all(users)
    await session.flush()
    return users


async def seed_doctors(session: AsyncSession) -> list[Doctor]:
    doctors = []
    for specialization, count in DOCTORS:
        for _ in range(count):
            doctors.append(Doctor(name=fake.name(), specialization=specialization))
    session.add_all(doctors)
    await session.flush()
    return doctors


async def seed_rooms(session: AsyncSession) -> list[OperatingRoom]:
    rooms = [OperatingRoom(name=f"Операционная №{i}") for i in range(1, NUM_ROOMS + 1)]
    session.add_all(rooms)
    await session.flush()
    return rooms


async def seed_procedures(session: AsyncSession) -> dict[str, Procedure]:
    procedures = {}
    for spec in PROCEDURES:
        procedure = Procedure(
            code=spec["code"],
            name=spec["name"],
            base_price=spec["base_price"],
            duration_minutes=spec["duration_minutes"],
            required_specialization=spec["required_specialization"],
        )
        session.add(procedure)
        procedures[spec["code"]] = procedure
    await session.flush()
    for spec in PROCEDURES:
        procedure = procedures[spec["code"]]
        session.add_all(
            [ProcedureRequiredTest(procedure_id=procedure.id, test_type=t) for t in spec["required_tests"]]
        )
        session.add_all(
            [ProcedureContraindication(procedure_id=procedure.id, code=c) for c in spec["contraindications"]]
        )
    await session.flush()
    return procedures


async def seed_promo_codes(session: AsyncSession, procedures: dict[str, Procedure]) -> list[PromoCode]:
    today = date.today()
    promos = [
        # Рабочий промокод от бьюти-блогера
        PromoCode(
            code="NOSIK15",
            discount_rate=Decimal("0.15"),
            valid_until=today + timedelta(days=90),
            usage_limit=1000,
            used_count=137,
        ),
        # Исчерпанный: разлетелся за день
        PromoCode(
            code="KRASOTKA50",
            discount_rate=Decimal("0.50"),
            valid_until=today + timedelta(days=30),
            usage_limit=100,
            used_count=100,
        ),
        # Протухший с прошлого года
        PromoCode(
            code="BOTOX2025",
            discount_rate=Decimal("0.30"),
            valid_until=date(2025, 12, 31),
            usage_limit=1000,
            used_count=421,
        ),
        # Только на ринопластику
        PromoCode(
            code="HOLLYWOOD30",
            discount_rate=Decimal("0.30"),
            valid_until=today + timedelta(days=60),
            usage_limit=500,
            used_count=12,
            procedure_id=procedures["rhinoplasty"].id,
        ),
    ]
    session.add_all(promos)
    await session.flush()
    return promos


async def seed_slots(session: AsyncSession, doctors: list[Doctor], rooms: list[OperatingRoom]) -> None:
    """Расписание на SLOTS_DAYS_AHEAD дней вперёд: у каждого хирурга 3 слота в день."""
    slots = []
    today = date.today()
    for day_offset in range(1, SLOTS_DAYS_AHEAD + 1):
        day = today + timedelta(days=day_offset)
        for doctor in doctors:
            for hour in random.sample(SLOT_HOURS, 3):
                slots.append(
                    ScheduleSlot(
                        room_id=random.choice(rooms).id,
                        doctor_id=doctor.id,
                        starts_at=datetime.combine(day, time(hour=hour)),
                        duration_minutes=random.choice(SLOT_DURATIONS),
                        is_hot=random.random() < HOT_SLOT_PROBABILITY,
                    )
                )
    session.add_all(slots)
    await session.flush()


async def create_past_surgery(
    session: AsyncSession,
    patient: Patient,
    procedure: Procedure,
    doctors: list[Doctor],
    rooms: list[OperatingRoom],
) -> None:
    """Завершённая операция в прошлом (для скидки «вторая процедура»)."""
    doctor = random.choice([d for d in doctors if d.specialization == procedure.required_specialization])
    days_ago = random.randint(30, 365)
    slot = ScheduleSlot(
        room_id=random.choice(rooms).id,
        doctor_id=doctor.id,
        starts_at=datetime.combine(date.today() - timedelta(days=days_ago), time(hour=random.choice(SLOT_HOURS))),
        duration_minutes=240,
        is_hot=False,
    )
    session.add(slot)
    await session.flush()
    session.add(
        Surgery(
            patient_id=patient.id,
            procedure_id=procedure.id,
            slot_id=slot.id,
            status="completed",
            base_price=procedure.base_price,
            discount_amount=Decimal("0.00"),
            final_price=procedure.base_price,
            created_at=slot.starts_at - timedelta(days=14),
        )
    )


async def seed_demo_patients(
    session: AsyncSession,
    procedures: dict[str, Procedure],
    doctors: list[Doctor],
    rooms: list[OperatingRoom],
) -> None:
    """Четыре детерминированных пациента под сценарии демо."""
    today = date.today()
    facial_doctor = next(d for d in doctors if d.specialization == "facial")

    # Пациент 1: идеальный. Консультация одобрена, анализы свежие, gold,
    # одна операция уже была → промокод + лояльность + «вторая процедура» упрутся в потолок 70%
    perfect = Patient(name="Иванова Анна Безупречновна", birth_date=date(1990, 5, 12), phone=fake.phone_number())
    # Пациент 2: просроченные и несданные анализы → 422
    expired = Patient(name="Петрова Мария Просроченовна", birth_date=date(1985, 3, 8), phone=fake.phone_number())
    # Пациент 3: пришёл без консультации → 409
    hasty = Patient(name="Сидорова Ольга Торопыговна", birth_date=date(1998, 11, 30), phone=fake.phone_number())
    # Пациент 4: противопоказание nicotine_addiction → 409
    smoker = Patient(name="Смирнов Игорь Никотинович", birth_date=date(1979, 7, 21), phone=fake.phone_number())
    session.add_all([perfect, expired, hasty, smoker])
    await session.flush()

    rhinoplasty = procedures["rhinoplasty"]

    # Пациент 1
    session.add(
        Consultation(
            patient_id=perfect.id,
            doctor_id=facial_doctor.id,
            procedure_id=rhinoplasty.id,
            approved=True,
            held_at=datetime.now() - timedelta(days=10),
        )
    )
    session.add_all(
        [LabResult(patient_id=perfect.id, test_type=t, taken_at=today - timedelta(days=3)) for t in TEST_TYPES]
    )
    session.add(LoyaltyAccount(patient_id=perfect.id, status="gold", points=1200))
    await create_past_surgery(session, perfect, procedures["blepharoplasty"], doctors, rooms)

    # Пациент 2: кровь и ЭКГ просрочены, флюорография древняя, коагулограмма не сдана
    session.add(
        Consultation(
            patient_id=expired.id,
            doctor_id=facial_doctor.id,
            procedure_id=rhinoplasty.id,
            approved=True,
            held_at=datetime.now() - timedelta(days=60),
        )
    )
    session.add_all(
        [
            LabResult(patient_id=expired.id, test_type="blood_general", taken_at=today - timedelta(days=60)),
            LabResult(patient_id=expired.id, test_type="ecg", taken_at=today - timedelta(days=45)),
            LabResult(patient_id=expired.id, test_type="fluorography", taken_at=today - timedelta(days=400)),
        ]
    )
    session.add(LoyaltyAccount(patient_id=expired.id, status="silver", points=300))

    # Пациент 3: анализы отличные, а консультации нет
    session.add_all(
        [LabResult(patient_id=hasty.id, test_type=t, taken_at=today - timedelta(days=2)) for t in TEST_TYPES]
    )
    session.add(LoyaltyAccount(patient_id=hasty.id, status="bronze", points=0))

    # Пациент 4: всё есть, кроме здоровья
    session.add(
        Consultation(
            patient_id=smoker.id,
            doctor_id=facial_doctor.id,
            procedure_id=rhinoplasty.id,
            approved=True,
            held_at=datetime.now() - timedelta(days=5),
        )
    )
    session.add_all(
        [LabResult(patient_id=smoker.id, test_type=t, taken_at=today - timedelta(days=4)) for t in TEST_TYPES]
    )
    session.add(PatientContraindication(patient_id=smoker.id, code="nicotine_addiction"))
    session.add(LoyaltyAccount(patient_id=smoker.id, status="platinum", points=9000))

    await session.flush()


async def seed_random_patients(
    session: AsyncSession,
    procedures: dict[str, Procedure],
    doctors: list[Doctor],
    rooms: list[OperatingRoom],
) -> None:
    today = date.today()
    procedure_list = list(procedures.values())

    patients = [
        Patient(
            name=fake.name(),
            birth_date=fake.date_of_birth(minimum_age=18, maximum_age=70),
            phone=fake.phone_number(),
        )
        for _ in range(NUM_PATIENTS - 4)
    ]
    session.add_all(patients)
    await session.flush()

    for patient in patients:
        # Программа лояльности есть у всех — попробуйте от неё отказаться
        status = random.choices(LOYALTY_STATUSES, weights=LOYALTY_WEIGHTS)[0]
        session.add(LoyaltyAccount(patient_id=patient.id, status=status, points=random.randint(0, 5000)))

        # Противопоказания из анамнеза
        if random.random() < CONTRAINDICATION_PROBABILITY:
            for code in random.sample(CONTRAINDICATION_CODES, random.randint(1, 2)):
                session.add(PatientContraindication(patient_id=patient.id, code=code))

        # Анализы: случайный набор разной свежести
        for test_type in random.sample(TEST_TYPES, random.randint(0, len(TEST_TYPES))):
            session.add(
                LabResult(
                    patient_id=patient.id,
                    test_type=test_type,
                    taken_at=today - timedelta(days=random.randint(1, 200)),
                )
            )

        # Консультации: одна-две, чаще всего одобренные
        if random.random() < CONSULTATION_PROBABILITY:
            for procedure in random.sample(procedure_list, random.randint(1, 2)):
                doctor = random.choice([d for d in doctors if d.specialization == procedure.required_specialization])
                session.add(
                    Consultation(
                        patient_id=patient.id,
                        doctor_id=doctor.id,
                        procedure_id=procedure.id,
                        approved=random.random() < 0.85,
                        held_at=datetime.now() - timedelta(days=random.randint(1, 90)),
                    )
                )

        # У части пациентов уже были операции
        if random.random() < COMPLETED_SURGERY_PROBABILITY:
            await create_past_surgery(session, patient, random.choice(procedure_list), doctors, rooms)


def print_demo_commands() -> None:
    demo_date = date.today() + timedelta(days=7)

    def url(patient_id: int, promo: str | None = None) -> str:
        query = f"patient_id={patient_id}&procedure_code=rhinoplasty&preferred_date={demo_date}"
        if promo is not None:
            query += f"&promo_code={promo}"
        return f"curl -H 'X-User-Id: 1' 'http://localhost:8000/surgery-offer?{query}'"

    print("\nДемо-сценарии:")
    print(f"  200 (все скидки):       {url(1, 'NOSIK15')}")
    print(f"  422 (анализы):          {url(2)}")
    print(f"  409 (нет консультации): {url(3)}")
    print(f"  409 (противопоказания): {url(4)}")
    print("  403: то же с -H 'X-User-Id: 3' (хирург), 401: с -H 'X-User-Id: 999'")
    print("  400: promo_code=KRASOTKA50 (исчерпан) или BOTOX2025 (истёк)")


async def main() -> None:
    print(f"Создание базы данных {DB_NAME}...")
    await create_database()

    engine = create_async_engine(DATABASE_URL)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    print("Создание/пересоздание таблиц...")
    await create_tables(engine)

    async with session_factory() as session:
        print("Добавление сотрудников...")
        users = await seed_users(session)
        print(f"Добавлено сотрудников: {len(users)}")

        print("Добавление врачей и операционных...")
        doctors = await seed_doctors(session)
        rooms = await seed_rooms(session)
        print(f"Добавлено врачей: {len(doctors)}, операционных: {len(rooms)}")

        print("Добавление процедур и промокодов...")
        procedures = await seed_procedures(session)
        promos = await seed_promo_codes(session, procedures)
        print(f"Добавлено процедур: {len(procedures)}, промокодов: {len(promos)}")

        print(f"Генерация расписания на {SLOTS_DAYS_AHEAD} дней...")
        await seed_slots(session, doctors, rooms)

        print("Добавление демо-пациентов (1-4)...")
        await seed_demo_patients(session, procedures, doctors, rooms)

        print(f"Генерация {NUM_PATIENTS - 4} случайных пациентов...")
        await seed_random_patients(session, procedures, doctors, rooms)

        print("Сохранение в БД...")
        await session.commit()

    await engine.dispose()
    print("Готово!")
    print_demo_commands()


if __name__ == "__main__":
    asyncio.run(main())
