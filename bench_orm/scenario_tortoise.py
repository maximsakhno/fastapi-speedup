"""Tortoise ORM: модели клиники + сценарий happy-path `/surgery-offer` (10 запросов).

Модели покрывают только колонки, нужные для сценария. Составные PK у ассоциативных таблиц
Tortoise не поддерживает — помечаем одну колонку `pk=True` (read-only, уникальность не важна,
суррогатный `id` не выбираем). Драйвер — asyncpg (задаётся в db_url оркестратором).

Число фактических запросов у Tortoise больше 10: список `required_tests` тянется отдельным
запросом (reverse-FK у Tortoise всегда отдельный запрос).
"""

from __future__ import annotations

from datetime import datetime, time, timedelta

from tortoise import fields
from tortoise.expressions import Subquery
from tortoise.functions import Max
from tortoise.models import Model
from tortoise.transactions import in_transaction


class User(Model):
    id = fields.IntField(pk=True)
    role = fields.CharField(max_length=255)

    class Meta:
        table = "users"


class Patient(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=255)

    class Meta:
        table = "patients"


class Procedure(Model):
    id = fields.IntField(pk=True)
    code = fields.CharField(max_length=255)
    name = fields.CharField(max_length=255)
    base_price = fields.DecimalField(max_digits=12, decimal_places=2)
    duration_minutes = fields.IntField()
    required_specialization = fields.CharField(max_length=255)

    class Meta:
        table = "procedures"


class ProcedureRequiredTest(Model):
    procedure_id = fields.IntField()
    test_type = fields.CharField(max_length=255, pk=True)

    class Meta:
        table = "procedure_required_tests"


class ProcedureContraindication(Model):
    procedure_id = fields.IntField()
    code = fields.CharField(max_length=255, pk=True)

    class Meta:
        table = "procedure_contraindications"


class PatientContraindication(Model):
    patient_id = fields.IntField()
    code = fields.CharField(max_length=255, pk=True)

    class Meta:
        table = "patient_contraindications"


class Consultation(Model):
    id = fields.IntField(pk=True)
    patient_id = fields.IntField()
    procedure_id = fields.IntField()
    approved = fields.BooleanField()
    held_at = fields.DatetimeField()

    class Meta:
        table = "consultations"


class LabResult(Model):
    id = fields.IntField(pk=True)
    patient_id = fields.IntField()
    test_type = fields.CharField(max_length=255)
    taken_at = fields.DateField()

    class Meta:
        table = "lab_results"


class Doctor(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=255)
    specialization = fields.CharField(max_length=255)

    class Meta:
        table = "doctors"


class OperatingRoom(Model):
    id = fields.IntField(pk=True)
    name = fields.CharField(max_length=255)

    class Meta:
        table = "operating_rooms"


class ScheduleSlot(Model):
    id = fields.IntField(pk=True)
    starts_at = fields.DatetimeField()
    duration_minutes = fields.IntField()
    is_hot = fields.BooleanField()
    doctor = fields.ForeignKeyField("models.Doctor", source_field="doctor_id")
    room = fields.ForeignKeyField("models.OperatingRoom", source_field="room_id")

    class Meta:
        table = "schedule_slots"


class PromoCode(Model):
    id = fields.IntField(pk=True)
    code = fields.CharField(max_length=255)
    discount_rate = fields.DecimalField(max_digits=5, decimal_places=2)
    valid_until = fields.DateField()
    usage_limit = fields.IntField()
    used_count = fields.IntField()
    procedure_id = fields.IntField(null=True)

    class Meta:
        table = "promo_codes"


class LoyaltyAccount(Model):
    patient_id = fields.IntField(pk=True)
    status = fields.CharField(max_length=255)

    class Meta:
        table = "loyalty_accounts"


class Surgery(Model):
    id = fields.IntField(pk=True)
    patient_id = fields.IntField()
    slot_id = fields.IntField()
    status = fields.CharField(max_length=255)

    class Meta:
        table = "surgeries"


async def scenario(params: dict) -> tuple:
    """10 запросов happy-path в одной транзакции. Возвращает ключевые значения для sanity-сверки."""
    uid = params["user_id"]
    pid = params["patient_id"]
    code = params["procedure_code"]
    promo_code = params["promo_code"]
    preferred_date = params["preferred_date"]

    async with in_transaction():
        # 1. Сотрудник
        await User.get_or_none(id=uid)
        # 2. Пациент
        patient = await Patient.get_or_none(id=pid)
        # 3. Процедура + список требуемых анализов (reverse-FK => отдельный запрос)
        procedure = await Procedure.get_or_none(code=code)
        test_types = await ProcedureRequiredTest.filter(procedure_id=procedure.id).values_list("test_type", flat=True)
        # 4. Одобренная консультация
        await Consultation.filter(patient_id=pid, procedure_id=procedure.id, approved=True).order_by("-held_at").first()
        # 5. Свежесть анализов: max(taken_at) по типам
        await (
            LabResult.filter(patient_id=pid, test_type__in=list(test_types))
            .annotate(taken_at_max=Max("taken_at"))
            .group_by("test_type")
            .values("test_type", "taken_at_max")
        )
        # 6. Конфликты противопоказаний: пересечение через подзапрос
        await PatientContraindication.filter(
            patient_id=pid,
            code__in=Subquery(ProcedureContraindication.filter(procedure_id=procedure.id).values("code")),
        ).values_list("code", flat=True)
        # 7. Свободный слот + хирург + операционная (JOIN через values, NOT EXISTS через подзапрос)
        day_start = datetime.combine(preferred_date, time.min)
        day_end = day_start + timedelta(days=1)
        slot_rows = await (
            ScheduleSlot.filter(
                doctor__specialization=procedure.required_specialization,
                starts_at__gte=day_start,
                starts_at__lt=day_end,
                duration_minutes__gte=procedure.duration_minutes,
            )
            .exclude(id__in=Subquery(Surgery.all().values("slot_id")))
            .order_by("starts_at")
            .limit(1)
            .values("starts_at", "duration_minutes", "is_hot", "doctor__name", "room__name")
        )
        slot = slot_rows[0] if slot_rows else None
        # 8. Промокод
        promo = await PromoCode.get_or_none(code=promo_code)
        # 9. Статус лояльности
        await LoyaltyAccount.get_or_none(patient_id=pid)
        # 10. Число завершённых операций
        await Surgery.filter(patient_id=pid, status="completed").count()

    return (patient.name, procedure.name, slot is not None, promo is not None)
