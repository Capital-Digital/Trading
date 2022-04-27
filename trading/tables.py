import django_tables2 as tables
from trading.models import Order


class OrderTable(tables.Table):
    id = tables.Column(attrs={"td": {"class": "my-class"}})

    class Meta:
        model = Order
        fields = ('clientid', 'market__symbol', 'market__type', 'status', 'side', 'action', 'amount', 'price',
                  'cost', 'dt_create', 'dt_update')
