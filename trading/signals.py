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

            # Unpack arguments
            account_id, action, code, order_type, price, reduce_only, side, size, symbol, wallet = args

            if retval['info']['status'] in ['FILLED', 'PARTIALLY_FILLED']:
                log.info('Order filled')
                account = Account.objects.get(id=account_id)
                account.update_df(action, wallet, code, retval)

            elif retval['info']['status'] == 'NEW':
                log.info('Order is open')

            elif retval['info']['status'] == 'CANCELED':
                log.info('Order has been canceled')

        else:
            log.info('State {0}'.format(state))
            log.info('Retval {0}'.format(retval))


@receiver(pre_delete, sender=Order)
def cancel_order(sender, instance, **kwargs):
    pass
