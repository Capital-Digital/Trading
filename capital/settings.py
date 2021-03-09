import os
from django.conf.locale.en import formats as en_formats
import structlog
from django.dispatch import receiver
from django_structlog.signals import bind_extra_request_metadata
import environ


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # Contains manage.py and PROJECT_ROOT
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))  # Contains settings.py

environ.Env.read_env()
# <snip other settings>

env = environ.Env(DEBUG=(bool, False))
env_file = os.path.join(PROJECT_ROOT, ".env")
environ.Env.read_env(env_file)

# <snip>

SECRET_KEY = env("SECRET_KEY")
DEBUG = env("DEBUG")
ALLOWED_HOSTS = env('ALLOWED_HOSTS')

INSTALLED_APPS = [
    'marketsdata.apps.MarketsdataConfig',
    'trading.apps.TradingConfig',
    'strategy.apps.StrategyConfig',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'django_extensions',
    'django_celery_beat',
    'reset_migrations',
    'prettyjson'
]

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'whitenoise.middleware.WhiteNoiseMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'django_structlog.middlewares.RequestMiddleware',
    'django_structlog.middlewares.CeleryMiddleware',
]

ROOT_URLCONF = 'capital.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'capital.wsgi.application'

# Parse database connection url strings like psql://user:pass@127.0.0.1:8458/db
DATABASES = {
    'default': env.db()
}

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
STATIC_URL = '/static/'
STATICFILES_DIRS = (os.path.join(BASE_DIR, "static"),)
STATIC_ROOT = os.path.join(PROJECT_ROOT, 'static')  # Static files settings
STATICFILES_STORAGE = 'whitenoise.storage.CompressedManifestStaticFilesStorage'

# set datetime format in the admin section
en_formats.DATETIME_FORMAT = "Y-m-d H:i:s"

# run
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

# celery configuration
CELERY_WORKER_REDIRECT_STDOUTS_LEVEL = 'WARNING'  # The log level output to stdout and stderr is logged as.
CELERY_WORKER_REDIRECT_STDOUTS = False  # If enabled stdout and stderr will be redirected to the current logger.
CELERY_BROKER_URL = 'redis://localhost:6379/0'  # /0 for first database
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'  # The backend used to store task results (tombstones). Disabled by default.
CELERY_ACCEPT_CONTENT = ['application/json']
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TASK_SERIALIZER = 'json'
CELERY_IGNORE_RESULT = False
CELERY_TASK_TRACK_STARTED = True

# CELERY_REDIS_RETRY_ON_TIMEOUT = True
# CELERY_RESULT_EXPIRES = 2
# CELERY_TASK_DEFAULT_DELIVERY_MODE = 'transient'
# CELERY_TASK_COMPRESSION = 'gzip'
# CELERY_TASK_PUBLISH_RETRY = False

DATA_UPLOAD_MAX_NUMBER_FIELDS = 100240
DATA_UPLOAD_MAX_MEMORY_SIZE = 5242880

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
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
                key_order=['timestamp', 'level', 'logger', 'event'],
                sort_keys=False
            ),
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "plain_console",
        },
        "json_file": {
            "class": "logging.handlers.WatchedFileHandler",
            "filename": "log/json.log",
            "formatter": "json_formatter",
        },
        "flat_line_file": {
            "class": "logging.handlers.WatchedFileHandler",
            "filename": "log/flat_line.log",
            "formatter": "key_value",
        },
    },
    "loggers": {
        '': {
            "handlers": ["console", "flat_line_file", "json_file"],
            "level": "WARNING",
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

structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S.%f"),  # (fmt="iso"),
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    context_class=structlog.threadlocal.wrap_dict(dict),
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)



@receiver(bind_extra_request_metadata)
def clean_metadata(request, logger, **kwargs):
    # logger.bind(user_email=getattr(request.user, 'email', ''))
    logger.unbind('request_id', 'ip', 'user_id')


