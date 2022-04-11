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
        exid, signal = args
        if state == 'SUCCESS':
            if signal:
                log.info('Dataframe update success', exchange=exid)
                update_strategies.delay(exid, signal)
        else:
            log.error('Dataframe update failure')

    if task.name == 'Update_strategy':
        stid, signal = args
        if state == 'SUCCESS':
            if signal:
                log.info('Strategy update success', strategy=stid)
                update_accounts.delay(stid, signal)
        else:
            log.error('Strategies update failure')

    if task.name == 'Update_account':
        acid, signal = args
        if state == 'SUCCESS':
            if signal:
                log.info('Account update success', account=acid)
        else:
            log.error('Accounts update failure')
