"""Peewee: модели клиники + синхронный сценарий happy-path `/surgery-offer` (10 запросов).

Модели покрывают только нужные колонки. Ассоциативные таблицы без суррогатного `id` используют
`CompositeKey` (иначе Peewee добавит `id AutoField`, которого в таблице нет). Движок привязывается
в рантайме через `DatabaseProxy` (оркестратор создаёт свежую БД на прогон). Драйвер — psycopg2.

Число фактических запросов больше 10: список `required_tests` тянется отдельным запросом.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta

from peewee import (
    BooleanField,
    CharField,
    CompositeKey,
    DatabaseProxy,
    DateField,
    DateTimeField,
    DecimalField,
    ForeignKeyField,
    IntegerField,
    Model,
    fn,
)

database_proxy = DatabaseProxy()


class BaseModel(Model):
    class Meta:
        database = database_proxy


class User(BaseModel):
    role = CharField()

    class Meta:
        table_name = "users"


class Patient(BaseModel):
    name = CharField()

    class Meta:
        table_name = "patients"


class Procedure(BaseModel):
    code = CharField()
    name = CharField()
    base_price = DecimalField()
    duration_minutes = IntegerField()
    required_specialization = CharField()

    class Meta:
        table_name = "procedures"


class ProcedureRequiredTest(BaseModel):
    procedure_id = IntegerField()
    test_type = CharField()

    class Meta:
        table_name = "procedure_required_tests"
        primary_key = CompositeKey("procedure_id", "test_type")


class ProcedureContraindication(BaseModel):
    procedure_id = IntegerField()
    code = CharField()

    class Meta:
        table_name = "procedure_contraindications"
        primary_key = CompositeKey("procedure_id", "code")


class PatientContraindication(BaseModel):
    patient_id = IntegerField()
    code = CharField()

    class Meta:
        table_name = "patient_contraindications"
        primary_key = CompositeKey("patient_id", "code")


class Consultation(BaseModel):
    patient_id = IntegerField()
    procedure_id = IntegerField()
    approved = BooleanField()
    held_at = DateTimeField()

    class Meta:
        table_name = "consultations"


class LabResult(BaseModel):
    patient_id = IntegerField()
    test_type = CharField()
    taken_at = DateField()

    class Meta:
        table_name = "lab_results"


class Doctor(BaseModel):
    name = CharField()
    specialization = CharField()

    class Meta:
        table_name = "doctors"


class OperatingRoom(BaseModel):
    name = CharField()

    class Meta:
        table_name = "operating_rooms"


class ScheduleSlot(BaseModel):
    starts_at = DateTimeField()
    duration_minutes = IntegerField()
    is_hot = BooleanField()
    doctor = ForeignKeyField(Doctor, column_name="doctor_id")
    room = ForeignKeyField(OperatingRoom, column_name="room_id")

    class Meta:
        table_name = "schedule_slots"


class PromoCode(BaseModel):
    code = CharField()

    class Meta:
        table_name = "promo_codes"


class LoyaltyAccount(BaseModel):
    patient_id = IntegerField(primary_key=True)
    status = CharField()

    class Meta:
        table_name = "loyalty_accounts"


class Surgery(BaseModel):
    patient_id = IntegerField()
    slot_id = IntegerField()
    status = CharField()

    class Meta:
        table_name = "surgeries"


def scenario(db, params: dict) -> tuple:
    """10 запросов happy-path в одной транзакции (синхронно). Значения для sanity-сверки."""
    uid = params["user_id"]
    pid = params["patient_id"]
    code = params["procedure_code"]
    promo_code = params["promo_code"]
    preferred_date = params["preferred_date"]

    with db.atomic():
        # 1. Сотрудник
        User.get_or_none(User.id == uid)
        # 2. Пациент
        patient = Patient.get_or_none(Patient.id == pid)
        # 3. Процедура + список требуемых анализов (отдельный запрос)
        procedure = Procedure.get_or_none(Procedure.code == code)
        test_types = [
            r.test_type
            for r in ProcedureRequiredTest.select(ProcedureRequiredTest.test_type).where(
                ProcedureRequiredTest.procedure_id == procedure.id
            )
        ]
        # 4. Одобренная консультация
        (
            Consultation.select()
            .where(
                (Consultation.patient_id == pid)
                & (Consultation.procedure_id == procedure.id)
                & (Consultation.approved == True)  # noqa: E712
            )
            .order_by(Consultation.held_at.desc())
            .first()
        )
        # 5. Свежесть анализов: max(taken_at) по типам
        list(
            LabResult.select(LabResult.test_type, fn.MAX(LabResult.taken_at))
            .where((LabResult.patient_id == pid) & (LabResult.test_type.in_(test_types)))
            .group_by(LabResult.test_type)
        )
        # 6. Конфликты противопоказаний: пересечение через подзапрос
        proc_codes = ProcedureContraindication.select(ProcedureContraindication.code).where(
            ProcedureContraindication.procedure_id == procedure.id
        )
        list(
            PatientContraindication.select(PatientContraindication.code).where(
                (PatientContraindication.patient_id == pid) & (PatientContraindication.code.in_(proc_codes))
            )
        )
        # 7. Свободный слот + хирург + операционная (JOIN, NOT EXISTS через подзапрос)
        day_start = datetime.combine(preferred_date, time.min)
        day_end = day_start + timedelta(days=1)
        taken = Surgery.select(Surgery.slot_id)
        slot = (
            ScheduleSlot.select(ScheduleSlot, Doctor, OperatingRoom)
            .join(Doctor, on=(ScheduleSlot.doctor == Doctor.id))
            .switch(ScheduleSlot)
            .join(OperatingRoom, on=(ScheduleSlot.room == OperatingRoom.id))
            .where(
                (Doctor.specialization == procedure.required_specialization)
                & (ScheduleSlot.starts_at >= day_start)
                & (ScheduleSlot.starts_at < day_end)
                & (ScheduleSlot.duration_minutes >= procedure.duration_minutes)
                & (ScheduleSlot.id.not_in(taken))
            )
            .order_by(ScheduleSlot.starts_at)
            .first()
        )
        if slot is not None:
            _ = (slot.doctor.name, slot.room.name)  # уже в JOIN, доп. запроса нет
        # 8. Промокод
        promo = PromoCode.get_or_none(PromoCode.code == promo_code)
        # 9. Статус лояльности
        LoyaltyAccount.get_or_none(LoyaltyAccount.patient_id == pid)
        # 10. Число завершённых операций
        Surgery.select().where((Surgery.patient_id == pid) & (Surgery.status == "completed")).count()

    return (patient.name, procedure.name, slot is not None, promo is not None)
