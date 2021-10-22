from django.db.models.signals import post_save, pre_delete
from django.db.backends.signals import connection_created
from django.dispatch import receiver
from .models import Order

import structlog

log = structlog.get_logger(__name__)


# # Execute at startup
# @receiver(connection_created)
# def my_receiver(connection, **kwargs):
#     with connection.cursor() as cursor:
#         # do something to the database
#         print('Hello world')


@receiver(pre_delete, sender=Order)
def cancel_order(sender, instance, **kwargs):
    pass
