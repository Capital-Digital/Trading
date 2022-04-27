import django_tables2 as tables
from trading.models import Order


class OrderTable(tables.Table):
    price = tables.Column(verbose_name='Limit')

    def render_dt_create(self, **kwargs):
        return round(kwargs['value'], 2)

    class Meta:
        model = Order
        fields = ('clientid', 'market__symbol', 'market__type', 'status', 'side', 'action', 'amount', 'price',
                  'cost', 'dt_create', 'dt_update')
        exclude = ('ID',)
