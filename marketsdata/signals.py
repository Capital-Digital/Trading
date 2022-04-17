import time
import structlog
from django.db.models.signals import post_save
from django.dispatch import receiver
from strategy.models import Strategy
from celery.signals import task_success, task_postrun, task_failure
from strategy.tasks import update_strategies

log = structlog.get_logger(__name__)


@task_postrun.connect
def task_postrun_handler(task_id=None, task=None, args=None, state=None, **kwargs):

    if task.name == 'Update_dataframe':
        exid, signal = args
        if state == 'SUCCESS':

            log.info('')
            log.info('Dataframes update successful')

            if signal:
                update_strategies.delay(exid, signal)
        else:
            log.error('Dataframe update failure')
