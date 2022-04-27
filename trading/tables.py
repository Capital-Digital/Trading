import django_tables2 as tables
from trading.models import Order


class OrderTable(tables.Table):
    class Meta:
        model = Order
        fields = ('clientid', 'market__symbol', 'market__type', 'dt_create', 'dt_update')
