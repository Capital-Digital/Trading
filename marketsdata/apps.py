from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class MarketsdataConfig(AppConfig):
    name = 'marketsdata'
    verbose_name = _('marketsdata')

    def ready(self):
        from . import signals
