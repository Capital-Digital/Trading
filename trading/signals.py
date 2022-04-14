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

    # Unpack arguments
    account_id, action, code, clientid, order_type, price, reduce_only, side, size, symbol, wallet = args

    if task.name == 'Trading_place_order':
        if state == 'SUCCESS':

            log.info('')
            log.info('Order placement success')
            log.info('code {0}'.format(code))
            log.info('action {0}'.format(action))
            log.info('clientid {0}'.format(clientid))
            log.info('size {0}'.format(size))
            log.info('wallet {0}'.format(wallet))

            if retval['info']['status'] in ['NEW', 'FILLED', 'PARTIALLY_FILLED']:
                update_order.delay(account_id, retval)

            elif retval['info']['status'] == 'CANCELED':
                log.info('Order has been canceled')

        else:
            log.info('')
            log.error('Order placement failed')
            log.info('code {0}'.format(code))
            log.info('action {0}'.format(action))
            log.info('clientid {0}'.format(clientid))
            log.info('size {0}'.format(size))
            log.info('wallet {0}'.format(wallet))

    # Recalculate balances and trades size after new resources become available
    if task.name == 'Trading_____Update_order':
        if state == 'SUCCESS':
            if retval:

                log.info('')
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
def task_failure_notifier(sender=None, task_id=None, args=None, exception=None, traceback=None, einfo=None, **kwargs):

    if sender.name == 'Trading_place_order':

        # Unpack arguments
        account_id, action, code, clientid, order_type, price, reduce_only, side, size, symbol, wallet = args

        if exception.__class__.__name__ == 'InsufficientFunds':

            # Update order status
            order = Order.objects.get(clientid=clientid)
            order.status = 'failed'
            order.response = dict(execption='InsufficientFunds')
            order.save()


@receiver(pre_delete, sender=Order)
def cancel_order(sender, instance, **kwargs):
    pass
