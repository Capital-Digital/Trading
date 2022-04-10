import structlog
from django.db.models.signals import post_save
from django.dispatch import receiver
import strategy.models as s
from .models import Candle
from celery.signals import task_success, task_postrun, task_failure
from .tasks import loader

log = structlog.get_logger()


@task_success.connect(sender=loader)
def monitor(sender, **kwargs):
    log.info('task scan completed - %s', kwargs['result'])


@task_postrun.connect
def task_postrun_handler(task_id=None, task=None, args=None, state=None, **kwargs):

    if task.name == 'marketsdata.tasks.loader':
        log.info('Task ID {0}'.format(task_id))
        log.info('Args {0}'.format(args))
        log.info('State {0}'.format(state))
        loader.delay("exid")