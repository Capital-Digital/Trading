import sys
from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class TradingConfig(AppConfig):
    name = 'trading'
    verbose_name = _('trading')

    def ready(self):
        from . import signals
