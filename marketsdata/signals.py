import structlog
from django.db.models.signals import post_save
from django.dispatch import receiver
from strategy.models import Strategy
from celery.signals import task_success, task_postrun, task_failure
from .tasks import update_strategies, update_accounts

log = structlog.get_logger()


@task_postrun.connect
def task_postrun_handler(task_id=None, task=None, args=None, state=None, **kwargs):

    if task.name == 'Update_dataframe':
        log.info('Signal received')
        exid, signal = args
        log.info('{0} {1} {2}'.format(exid, signal, state))
        if signal and state == 'SUCCESS':
            log.info('Dataframe update success', exchange=exid)
            update_strategies.delay(exid, signal)

    if task.name == 'Update_strategy':
        stid, signal = args
        if signal and state == 'SUCCESS':
            log.info('Strategy update success', strategy=stid)
            update_accounts.delay(stid, signal)

    if task.name == 'Update_account':
        acid, signal = args
        if signal and state == 'SUCCESS':
            log.info('Account update success', account=acid)
