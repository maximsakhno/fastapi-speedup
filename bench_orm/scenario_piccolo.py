"""Piccolo: таблицы клиники + сценарий happy-path `/surgery-offer` (10 запросов).

Таблицы описывают только нужные колонки. У ассоциативных таблиц без суррогатного `id` мы никогда
не делаем `.objects()` / `SELECT *` — только явный `.select(<колонки>)`, поэтому авто-`id` Piccolo
не участвует в запросах. Драйвер — asyncpg (PostgresEngine). Движок привязывается в рантайме через
`bind(engine)` (оркестратор создаёт свежий пул на прогон).

Число фактических запросов больше 10: список `required_tests` тянется отдельным запросом
(reverse-FK у Piccolo отдельным запросом).
"""

from __future__ import annotations

from datetime import datetime, time, timedelta

from piccolo.columns import Boolean, Date, ForeignKey, Integer, Numeric, Timestamp, Varchar
from piccolo.engine.postgres import PostgresEngine
from piccolo.query.functions import Max
from piccolo.table import Table

_ENGINE: PostgresEngine | None = None


class User(Table, tablename="users"):
    role = Varchar()


class Patient(Table, tablename="patients"):
    name = Varchar()


class Procedure(Table, tablename="procedures"):
    code = Varchar()
    name = Varchar()
    base_price = Numeric()
    duration_minutes = Integer()
    required_specialization = Varchar()


class ProcedureRequiredTest(Table, tablename="procedure_required_tests"):
    procedure_id = Integer()
    test_type = Varchar()


class ProcedureContraindication(Table, tablename="procedure_contraindications"):
    procedure_id = Integer()
    code = Varchar()


class PatientContraindication(Table, tablename="patient_contraindications"):
    patient_id = Integer()
    code = Varchar()


class Consultation(Table, tablename="consultations"):
    patient_id = Integer()
    procedure_id = Integer()
    approved = Boolean()
    held_at = Timestamp()


class LabResult(Table, tablename="lab_results"):
    patient_id = Integer()
    test_type = Varchar()
    taken_at = Date()


class Doctor(Table, tablename="doctors"):
    name = Varchar()
    specialization = Varchar()


class OperatingRoom(Table, tablename="operating_rooms"):
    name = Varchar()


class ScheduleSlot(Table, tablename="schedule_slots"):
    starts_at = Timestamp()
    duration_minutes = Integer()
    is_hot = Boolean()
    doctor = ForeignKey(references=Doctor, db_column_name="doctor_id")
    room = ForeignKey(references=OperatingRoom, db_column_name="room_id")


class PromoCode(Table, tablename="promo_codes"):
    code = Varchar()


class LoyaltyAccount(Table, tablename="loyalty_accounts"):
    patient_id = Integer()
    status = Varchar()


class Surgery(Table, tablename="surgeries"):
    patient_id = Integer()
    slot_id = Integer()
    status = Varchar()


TABLES = [
    User, Patient, Procedure, ProcedureRequiredTest, ProcedureContraindication,
    PatientContraindication, Consultation, LabResult, Doctor, OperatingRoom,
    ScheduleSlot, PromoCode, LoyaltyAccount, Surgery,
]


def bind(engine: PostgresEngine) -> None:
    """Привязать все таблицы к движку прогона."""
    global _ENGINE
    _ENGINE = engine
    for table in TABLES:
        table._meta.db = engine


async def scenario(params: dict) -> tuple:
    """10 запросов happy-path в одной транзакции. Возвращает ключевые значения для sanity-сверки."""
    uid = params["user_id"]
    pid = params["patient_id"]
    code = params["procedure_code"]
    promo_code = params["promo_code"]
    preferred_date = params["preferred_date"]

    async with _ENGINE.transaction():
        # 1. Сотрудник
        await User.select(User.role).where(User.id == uid).first()
        # 2. Пациент
        patient = await Patient.select(Patient.name).where(Patient.id == pid).first()
        # 3. Процедура + список требуемых анализов (reverse-FK => отдельный запрос)
        procedure = await Procedure.select(
            Procedure.id, Procedure.name, Procedure.duration_minutes, Procedure.required_specialization
        ).where(Procedure.code == code).first()
        proc_id = procedure["id"]
        tests = await ProcedureRequiredTest.select(ProcedureRequiredTest.test_type).where(
            ProcedureRequiredTest.procedure_id == proc_id
        )
        test_types = [t["test_type"] for t in tests]
        # 4. Одобренная консультация
        await Consultation.select(Consultation.id).where(
            (Consultation.patient_id == pid)
            & (Consultation.procedure_id == proc_id)
            & (Consultation.approved.eq(True))
        ).order_by(Consultation.held_at, ascending=False).first()
        # 5. Свежесть анализов: max(taken_at) по типам
        await LabResult.select(LabResult.test_type, Max(LabResult.taken_at)).where(
            (LabResult.patient_id == pid) & LabResult.test_type.is_in(test_types)
        ).group_by(LabResult.test_type)
        # 6. Конфликты противопоказаний: пересечение через подзапрос
        await PatientContraindication.select(PatientContraindication.code).where(
            (PatientContraindication.patient_id == pid)
            & PatientContraindication.code.is_in(
                ProcedureContraindication.select(ProcedureContraindication.code).where(
                    ProcedureContraindication.procedure_id == proc_id
                )
            )
        )
        # 7. Свободный слот + хирург + операционная (JOIN по FK, NOT EXISTS через подзапрос)
        day_start = datetime.combine(preferred_date, time.min)
        day_end = day_start + timedelta(days=1)
        slot = await ScheduleSlot.select(
            ScheduleSlot.starts_at,
            ScheduleSlot.duration_minutes,
            ScheduleSlot.is_hot,
            ScheduleSlot.doctor.name.as_alias("doctor_name"),
            ScheduleSlot.room.name.as_alias("room_name"),
        ).where(
            (ScheduleSlot.doctor.specialization == procedure["required_specialization"])
            & (ScheduleSlot.starts_at >= day_start)
            & (ScheduleSlot.starts_at < day_end)
            & (ScheduleSlot.duration_minutes >= procedure["duration_minutes"])
            & ScheduleSlot.id.not_in(Surgery.select(Surgery.slot_id))
        ).order_by(ScheduleSlot.starts_at).first()
        # 8. Промокод
        promo = await PromoCode.select(PromoCode.id).where(PromoCode.code == promo_code).first()
        # 9. Статус лояльности
        await LoyaltyAccount.select(LoyaltyAccount.status).where(LoyaltyAccount.patient_id == pid).first()
        # 10. Число завершённых операций
        await Surgery.count().where((Surgery.patient_id == pid) & (Surgery.status == "completed"))

    return (patient["name"], procedure["name"], slot is not None, promo is not None)
