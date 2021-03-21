from django.utils import timezone
from datetime import timedelta, datetime
import structlog
import pytz

log = structlog.get_logger(__name__)

datetime_directive_s = "%Y-%m-%d %H:%M:%S"
datetime_directive_ms = "%Y-%m-%d %H:%M:%S.%f"
datetime_directive_binance_order = '%Y-%m-%dT%H:%M:%S.%fZ'


# Return a Python datetime object TZ aware if USE_TZ=True
def get_datetime(hour=None, minute=None, delta=None, string=False, timestamp=False, ms=False):

    # Create datetime object and set minute or hour
    if hour:
        dt = timezone.now().replace(hour=hour, minute=0, microsecond=0)
    elif minute:
        dt = timezone.now().replace(minute=minute, microsecond=0)
    else:
        dt = timezone.now()

    # Remove a duration in seconds
    if delta:
        dt = dt - timedelta(seconds=delta)

    # Return object in the appropriate format
    if string:
        if ms:
            return dt.strftime(datetime_directive_ms)
        else:
            return dt.strftime(datetime_directive_s)
    elif timestamp:
        return int(dt.timestamp())
    else:
        return dt


# Convert a string to a datetime object
def convert_string_to_date(string, directive):
    return pytz.utc.localize(datetime.strptime(string, directive))
