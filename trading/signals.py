from django.db.models.signals import post_save, pre_delete
from django.db.backends.signals import connection_created
from django.dispatch import receiver
from trading.models import Account
from trading.tasks import *
from celery.signals import task_success, task_postrun, task_failure

import structlog

log = structlog.get_logger(__name__)


@task_postrun.connect
def task_postrun_handler(task_id=None, task=None, args=None, kwargs=None, retval=None, state=None):

    if task.name == 'Trading_place_order':

        log.info('Order placed', task=task.name, state=state)
        if state == 'SUCCESS':

            log.info('SUCCESS')
            log.info(retval['info']['orderId'])
            log.info(retval['info']['status'])
            log.info(len(args))

            account_id, wallet, symbol, size, price, order_type, side, reduce_only, valid = args

            log.info('account', id=account_id)
            account = Account.objects.get(id=account_id)


@receiver(pre_delete, sender=Order)
def cancel_order(sender, instance, **kwargs):
    pass
