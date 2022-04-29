import django_tables2 as tables
from trading.models import Order, Asset


class OrderTable(tables.Table):

    price = tables.Column(verbose_name='Price limit')

    dt_created = tables.DateTimeColumn(format='M d Y, h:i:s')
    dt_modified = tables.DateTimeColumn(format='M d Y, h:i:s')
    dt_created = tables.Column(verbose_name='Creation')
    dt_modified = tables.Column(verbose_name='Last update')

    def render_cost(self, **kwargs):
        return round(kwargs['value'], 2)

    class Meta:
        model = Order
        fields = ('clientid', 'market__symbol', 'market__type', 'status', 'side', 'action', 'amount', 'price',
                  'cost', 'dt_created', 'dt_modified')
        exclude = ('ID',)


class AssetTable(tables.Table):

    currency = tables.Column(verbose_name='Asset')
    wallet = tables.Column(verbose_name='Wallet')
    total = tables.Column(verbose_name='Total')
    free = tables.Column(verbose_name='Free')
    used = tables.Column(verbose_name='Reserved')

    dt_modified = tables.DateTimeColumn(format='M d Y, h:i:s')
    dt_modified = tables.Column(verbose_name='Last update')

    def render_total(self, **kwargs):
        return round(kwargs['value'], 3)

    def render_free(self, **kwargs):
        return round(kwargs['value'], 3)

    def render_used(self, **kwargs):
        return round(kwargs['value'], 3)

    class Meta:
        model = Asset
        fields = ('currency', 'wallet', 'total', 'free', 'used', 'dt_modified')
        exclude = ('ID',)


class PositionTable(tables.Table):

    market = tables.Column(verbose_name='Market')
    amount = tables.Column(verbose_name='Amount')
    side = tables.Column(verbose_name='Side')
    unrealized_pnl = tables.Column(verbose_name='Unrealized PnL')
    notional_value = tables.Column(verbose_name='Notional value')

    dt_modified = tables.DateTimeColumn(format='M d Y, h:i:s')
    dt_modified = tables.Column(verbose_name='Last update')

    def render_notional_value(self, **kwargs):
        return round(kwargs['value'], 3)

    def render_unrealized_pnl(self, **kwargs):
        return round(kwargs['value'], 3)

    class Meta:
        model = Asset
        fields = ('market', 'quantity', 'side', 'unrealized_pnl', 'notional_value', 'dt_modified')
        exclude = ('ID',)

