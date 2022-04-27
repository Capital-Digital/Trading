import django_tables2 as tables
from trading.models import Order


class OrderTable(tables.Table):
    price = tables.Column(verbose_name='Limit')
    dt_create = tables.DateTimeColumn(format='M d Y, h:m:s')
    dt_update = tables.DateTimeColumn(format='M d Y, h:m:s')

    def render_cost(self, **kwargs):
        return round(kwargs['value'], 2)

    class Meta:
        model = Order
        fields = ('clientid', 'market__symbol', 'market__type', 'status', 'side', 'action', 'amount', 'price',
                  'cost', 'dt_create', 'dt_update')
        exclude = ('ID',)
