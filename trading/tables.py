import django_tables2 as tables
from trading.models import Order


class CloseOrderTable(tables.Table):
    class Meta:
        model = Order
        fields = ('clientid', 'dt_create', 'dt_update')
