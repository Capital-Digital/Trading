from __future__ import absolute_import, unicode_literals
import os
import logging
import structlog
from celery import Celery
from celery.signals import setup_logging
from django_structlog.celery.steps import DjangoStructLogInitStep
import settings

from kombu import Queue, Exchange
from django_structlog.celery import signals
from django.dispatch import receiver

# log = structlog.get_logger(__name__)

# set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'capital.settings')

# define Celery instance
app = Celery('capital', broker='redis://localhost:6379/0')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
app.config_from_object(settings, namespace='CELERY')

# Load task modules from all registered Django app configs.
app.autodiscover_tasks()

# A step to initialize django-structlog
app.steps['worker'].add(DjangoStructLogInitStep)

# Change name of the default queue
app.conf.task_default_queue = 'default'

app.conf.CELERY_ENABLE_UTC = True


###########################
# Configure Celery queues #
###########################

default_queue_name = 'default'
default_exchange_name = 'default'
default_routing_key = 'default'

slow_queue_name = 'slow'
slow_routing_key = 'slow'

default_exchange = Exchange(default_exchange_name, type='direct')

default_queue = Queue(
    default_queue_name,
    default_exchange,
    routing_key=default_routing_key)

slow_queue = Queue(
    slow_queue_name,
    default_exchange,
    routing_key=slow_routing_key)

app.conf.task_queues = (default_queue, slow_queue)

app.conf.task_default_queue = default_queue_name
app.conf.task_default_exchange = default_exchange_name
app.conf.task_default_routing_key = default_routing_key


@setup_logging.connect
def receiver_setup_logging(loglevel, logfile, format, colorize, **kwargs):  # pragma: no cover
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": True,
            "formatters": {
                "json_formatter": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processor": structlog.processors.JSONRenderer(sort_keys=False),
                },
                "plain_console": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processor": structlog.dev.ConsoleRenderer(pad_event=43,
                                                               colors=True,
                                                               force_colors=True
                                                               ),
                },
                "key_value": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "processor": structlog.processors.KeyValueRenderer(
                        sort_keys=False,
                        key_order=['timestamp', 'level', 'logger', 'event']),
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "plain_console",
                },
                "json_file": {
                    "class": "logging.handlers.WatchedFileHandler",
                    "filename": "log/celery_json.log",
                    "formatter": "json_formatter",
                },
                "flat_line_file": {
                    "class": "logging.handlers.WatchedFileHandler",
                    "filename": "log/celery_flat_line.log",
                    "formatter": "key_value",
                },
            },
            "loggers": {
                '': {
                    "handlers": ["console", "flat_line_file", "json_file"],
                    "level": "ERROR",
                    'propagate': False,
                },
                'marketsdata': {
                    "handlers": ["console", "flat_line_file", "json_file"],
                    "level": "INFO",
                    'propagate': False,
                },
                'strategy': {
                    "handlers": ["console", "flat_line_file", "json_file"],
                    "level": "INFO",
                    'propagate': False,
                },
                'trading': {
                    "handlers": ["console", "flat_line_file", "json_file"],
                    "level": "INFO",
                    'propagate': False,
                }
            }
        }
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f"),
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.ExceptionPrettyPrinter(),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=structlog.threadlocal.wrap_dict(dict),
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


@receiver(signals.modify_context_before_task_publish)
def receiver_modify_context_before_task_publish(sender, signal, context):
    keys_to_keep = {"request_id", "parent_task_id"}
    new_dict = {key_to_keep: context[key_to_keep] for key_to_keep in keys_to_keep if key_to_keep in context}
    context.clear()
    context.update(new_dict)


@receiver(signals.bind_extra_task_metadata)
def receiver_bind_extra_request_metadata(sender, signal, task=None, logger=None):
    logger.unbind('task_id')
