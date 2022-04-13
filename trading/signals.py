from django.db.models.signals import post_save, pre_delete
from django.db.backends.signals import connection_created
from django.dispatch import receiver
from trading.models import Account
from trading.tasks import *
from celery.signals import task_success, task_postrun, task_failure

import structlog

log = structlog.get_logger(__name__)


@task_postrun.connect
def task_postrun_handler(task_id=None, task=None, args=None, state=None, retval=None, **kwargs):

    if task.name == 'Trading_place_order':

        if state == 'SUCCESS':

            if retval['info']['status'] in ['FILLED', 'PARTIALLY_FILLED']:
                log.info('Order filled')
                account = Account.objects.get(id=args[0])
                account.update_orders(retval)

            elif retval['info']['status'] == 'NEW':
                log.info('Order is open')

            elif retval['info']['status'] == 'CANCELED':
                log.info('Order has been canceled')


@receiver(pre_delete, sender=Order)
def cancel_order(sender, instance, **kwargs):
    pass
