import structlog
from django.db.models.signals import post_save
from django.dispatch import receiver
import strategy.models as s
from .models import Candle
from celery.signals import task_success, task_postrun, task_failure
from .tasks import loader

log = structlog.get_logger()
