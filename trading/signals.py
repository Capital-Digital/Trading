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
            account_id, action, code, order_id, order_type, price, reduce_only, side, size, symbol, wallet = args

            log.info('')
            log.info('*** SIGNAL ***')
            log.info('Order {1} {0}'.format(order_id, code))
            log.info('')

            if retval['info']['status'] in ['NEW', 'FILLED', 'PARTIALLY_FILLED']:
                update_order.delay(account_id, retval)

            elif retval['info']['status'] == 'CANCELED':
                log.info('Order has been canceled')

        else:
            log.info('kwargs {0}'.format(kwargs))

    # Recalculate balances and trades size after new resources become available
    if task.name == 'Trading_____Update_order':
        if state == 'SUCCESS':
            if retval:

                log.info('New resources available')
                account = Account.objects.get(id=retval)

                # Calculate new delta
                account.get_delta()

                # Allocate available resources
                account.buy_spot_all()
                account.open_short_all()

        else:
            log.error('Error while updating orders')


@task_failure.connect
def task_failure_notifier(sender=None, args=None, exception=None, **kwargs):

    if sender.name == 'Trading_place_order':
        
        log.info(exception)
        log.info(type(exception))

        if 'insufficient balance' in exception:

            # Unpack arguments
            account_id, action, code, order_id, order_type, price, reduce_only, side, size, symbol, wallet = args

            log.error('TASK {0} FAILED'.format(action))


@receiver(pre_delete, sender=Order)
def cancel_order(sender, instance, **kwargs):
    pass
