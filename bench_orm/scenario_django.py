"""Django ORM: модели клиники + синхронный сценарий happy-path `/surgery-offer` (10 запросов).

ВАЖНО: этот модуль импортируется ТОЛЬКО после `settings.configure()` + `django.setup()`
(это делает оркестратор) — иначе определение моделей упадёт. Модели `managed=False` (таблицы уже
созданы seed'ом), у ассоциативных таблиц без суррогатного `id` одна колонка помечена
`primary_key=True` (read-only, уникальность не проверяется).

Django НЕ умеет asyncpg — драйвер psycopg. Сценарий синхронный; оркестратор гоняет его через
`sync_to_async` в пуле потоков (1 переход в поток на транзакцию).

Число фактических запросов больше 10: `required_tests` тянется отдельным запросом.
"""

from __future__ import annotations

from django.db import models, transaction
from django.db.models import Max

APP_LABEL = "bench"


class User(models.Model):
    role = models.CharField(max_length=255)

    class Meta:
        app_label = APP_LABEL
        db_table = "users"
        managed = False


class Patient(models.Model):
    name = models.CharField(max_length=255)

    class Meta:
        app_label = APP_LABEL
        db_table = "patients"
        managed = False


class Procedure(models.Model):
    code = models.CharField(max_length=255)
    name = models.CharField(max_length=255)
    base_price = models.DecimalField(max_digits=12, decimal_places=2)
    duration_minutes = models.IntegerField()
    required_specialization = models.CharField(max_length=255)

    class Meta:
        app_label = APP_LABEL
        db_table = "procedures"
        managed = False


class ProcedureRequiredTest(models.Model):
    procedure_id = models.IntegerField()
    test_type = models.CharField(max_length=255, primary_key=True)

    class Meta:
        app_label = APP_LABEL
        db_table = "procedure_required_tests"
        managed = False


class ProcedureContraindication(models.Model):
    procedure_id = models.IntegerField()
    code = models.CharField(max_length=255, primary_key=True)

    class Meta:
        app_label = APP_LABEL
        db_table = "procedure_contraindications"
        managed = False


class PatientContraindication(models.Model):
    patient_id = models.IntegerField()
    code = models.CharField(max_length=255, primary_key=True)

    class Meta:
        app_label = APP_LABEL
        db_table = "patient_contraindications"
        managed = False


class Consultation(models.Model):
    patient_id = models.IntegerField()
    procedure_id = models.IntegerField()
    approved = models.BooleanField()
    held_at = models.DateTimeField()

    class Meta:
        app_label = APP_LABEL
        db_table = "consultations"
        managed = False


class LabResult(models.Model):
    patient_id = models.IntegerField()
    test_type = models.CharField(max_length=255)
    taken_at = models.DateField()

    class Meta:
        app_label = APP_LABEL
        db_table = "lab_results"
        managed = False


class Doctor(models.Model):
    name = models.CharField(max_length=255)
    specialization = models.CharField(max_length=255)

    class Meta:
        app_label = APP_LABEL
        db_table = "doctors"
        managed = False


class OperatingRoom(models.Model):
    name = models.CharField(max_length=255)

    class Meta:
        app_label = APP_LABEL
        db_table = "operating_rooms"
        managed = False


class ScheduleSlot(models.Model):
    starts_at = models.DateTimeField()
    duration_minutes = models.IntegerField()
    is_hot = models.BooleanField()
    doctor = models.ForeignKey(Doctor, on_delete=models.DO_NOTHING, db_column="doctor_id", related_name="+")
    room = models.ForeignKey(OperatingRoom, on_delete=models.DO_NOTHING, db_column="room_id", related_name="+")

    class Meta:
        app_label = APP_LABEL
        db_table = "schedule_slots"
        managed = False


class PromoCode(models.Model):
    code = models.CharField(max_length=255)

    class Meta:
        app_label = APP_LABEL
        db_table = "promo_codes"
        managed = False


class LoyaltyAccount(models.Model):
    patient_id = models.IntegerField(primary_key=True)
    status = models.CharField(max_length=255)

    class Meta:
        app_label = APP_LABEL
        db_table = "loyalty_accounts"
        managed = False


class Surgery(models.Model):
    patient_id = models.IntegerField()
    slot_id = models.IntegerField()
    status = models.CharField(max_length=255)

    class Meta:
        app_label = APP_LABEL
        db_table = "surgeries"
        managed = False


def scenario(params: dict) -> tuple:
    """10 запросов happy-path в одной транзакции (синхронно). Значения для sanity-сверки."""
    from datetime import datetime, time, timedelta

    uid = params["user_id"]
    pid = params["patient_id"]
    code = params["procedure_code"]
    promo_code = params["promo_code"]
    preferred_date = params["preferred_date"]

    with transaction.atomic():
        # 1. Сотрудник
        User.objects.filter(id=uid).first()
        # 2. Пациент
        patient = Patient.objects.filter(id=pid).first()
        # 3. Процедура + список требуемых анализов (отдельный запрос)
        procedure = Procedure.objects.filter(code=code).first()
        test_types = list(
            ProcedureRequiredTest.objects.filter(procedure_id=procedure.id).values_list("test_type", flat=True)
        )
        # 4. Одобренная консультация
        Consultation.objects.filter(patient_id=pid, procedure_id=procedure.id, approved=True).order_by("-held_at").first()
        # 5. Свежесть анализов: max(taken_at) по типам
        list(
            LabResult.objects.filter(patient_id=pid, test_type__in=test_types)
            .values("test_type")
            .annotate(taken_at_max=Max("taken_at"))
        )
        # 6. Конфликты противопоказаний: пересечение через подзапрос
        list(
            PatientContraindication.objects.filter(
                patient_id=pid,
                code__in=ProcedureContraindication.objects.filter(procedure_id=procedure.id).values("code"),
            ).values_list("code", flat=True)
        )
        # 7. Свободный слот + хирург + операционная (select_related => JOIN, NOT EXISTS через exclude)
        day_start = datetime.combine(preferred_date, time.min)
        day_end = day_start + timedelta(days=1)
        slot = (
            ScheduleSlot.objects.select_related("doctor", "room")
            .filter(
                doctor__specialization=procedure.required_specialization,
                starts_at__gte=day_start,
                starts_at__lt=day_end,
                duration_minutes__gte=procedure.duration_minutes,
            )
            .exclude(id__in=Surgery.objects.values("slot_id"))
            .order_by("starts_at")
            .first()
        )
        if slot is not None:
            _ = (slot.doctor.name, slot.room.name)  # материализуем связанные (уже в JOIN)
        # 8. Промокод
        promo = PromoCode.objects.filter(code=promo_code).first()
        # 9. Статус лояльности
        LoyaltyAccount.objects.filter(patient_id=pid).first()
        # 10. Число завершённых операций
        Surgery.objects.filter(patient_id=pid, status="completed").count()

    return (patient.name, procedure.name, slot is not None, promo is not None)
