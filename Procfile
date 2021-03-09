web: uvicorn capital.asgi:application
web: celery -A capital worker -l info
web: celery -A capital beat -l info --scheduler django_celery_beat.schedulers:DatabaseScheduler
