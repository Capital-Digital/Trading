import structlog
from django.db.models.signals import post_save
from django.dispatch import receiver
from strategy.models import Strategy
from celery.signals import task_success, task_postrun, task_failure
from .tasks import loader, update_strategies, update_accounts

log = structlog.get_logger()


@task_success.connect(sender=loader)
def monitor(sender, **kwargs):
    log.info('task scan completed - %s', kwargs['result'])


@task_postrun.connect
def task_postrun_handler(task_id=None, task=None, args=None, state=None, **kwargs):

    if task.name == 'Update_exchange':
        exid = args[0]
        if state == 'SUCCESS':
            log.info('Exchange update success', exchange=exid)
            update_strategies.delay(exid)

    if task.name == 'Update_strategy':
        stid = args[0]
        if state == 'SUCCESS':
            log.info('Strategy update success', strategy=stid)
            update_accounts.delay(stid)

    if task.name == 'Update_account':
        acid = args[0]
        if state == 'SUCCESS':
            log.info('Account update success', account=acid)
