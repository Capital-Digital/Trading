import time
import structlog
from django.db.models.signals import post_save
from django.dispatch import receiver
from marketsdata.models import Exchange
from trading.models import Account
from strategy.tasks import bulk_update_strategies
from celery.signals import task_success, task_postrun, task_failure
from billiard.process import current_process

log = structlog.get_logger(__name__)


@task_postrun.connect
def task_postrun_handler(task_id=None, task=None, args=None, state=None, retval=None, **kwargs):

    if task.name == 'Markets_____Update_dataframe':
        exid, tickers, admin = args

        if state == 'SUCCESS':
            if not admin:
                bulk_update_strategies.delay(exid, trade=True)

        else:
            log.error('Dataframe update failure')

    if task.name == 'Markets_____Update_exchange_prices':
        if state == 'SUCCESS':
            pass
