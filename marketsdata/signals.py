import structlog
from django.db.models.signals import post_save
from django.dispatch import receiver
import strategy.models as s
from .models import Candle

log = structlog.get_logger()


