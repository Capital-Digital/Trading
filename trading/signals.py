from django.db.models.signals import post_save
from django.dispatch import receiver
from .models import Order

import structlog

log = structlog.get_logger(__name__)


@receiver(post_save, sender=Order)
def place_order(sender, instance, created, **kwargs):
    pass
    # if created:
    #     instance.place()
    #     instance.account.refresh_positions()
    #     instance.account.refresh_orders()
