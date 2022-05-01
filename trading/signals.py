from django.db.models.signals import post_save, pre_delete
from django.db.backends.signals import connection_created
from django.dispatch import receiver

import marketsdata.tasks
from trading.models import Account
from trading.tasks import *
from celery.signals import task_success, task_postrun, task_failure
from celery.signals import worker_ready
import structlog

log = structlog.get_logger(__name__)


@task_postrun.connect
def task_postrun_handler(task_id=None, task=None, args=None, state=None, retval=None, **kwargs):

    if task.name == 'cancel_accounts_orders':

        if state == 'SUCCESS':
            pass

    if task.name == 'Trading_____Rebalance_account':

        print(args)
        log.info(args)

        account_id = args
        account = Account.objects.get(id=account_id)
        log.info('signal received, sync. complete', account=account.name)

        if state == 'SUCCESS':

            log.info('')
            log.info('Account {0} update successful'.format(account.name))
        else:
            log.info('Account {0} update failure'.format(account.name))
            account.busy = False
            account.save()

    if task.name in ['Trading_____Send_fetch_orderid']:

        account_id, quantity = retval

        if state == 'SUCCESS':
            if quantity:
                pass
        else:
            log.info('')
            log.error('Error with task {0}'.format(task.name))


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
