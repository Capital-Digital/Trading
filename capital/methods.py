from django.utils import timezone
from datetime import timedelta
import structlog

log = structlog.get_logger(__name__)


def get_datetime_now(delta=None, string=False):
    #
    # Return a Python datetime object TZ aware if USE_TZ=True
    #

    args = dict(minute=0, second=0, microsecond=0)

    # Create a datetime object
    dt = timezone.now().replace(**args)

    if delta:
        dt = dt - timedelta(hours=delta)

    if string:
        dt = dt.strftime("%Y-%m-%d %H:%M")

    return dt
