import django_tables2 as tables
from trading.models import Order, Asset


class OrderTable(tables.Table):

    price = tables.Column(verbose_name='Price limit')
    average = tables.Column(verbose_name='Price avg')
    filled = tables.Column(verbose_name='Filled')

    dt_created = tables.DateTimeColumn(format='M d Y, h:i:s')
    dt_modified = tables.DateTimeColumn(format='M d Y, h:i:s')
    dt_created = tables.Column(verbose_name='Creation')
    dt_modified = tables.Column(verbose_name='Last update')

    def render_cost(self, **kwargs):
        return round(kwargs['value'], 2)

    def render_average(self, **kwargs):
        return round(kwargs['value'], 3)

    class Meta:
        model = Order
        fields = ('clientid', 'market__symbol', 'market__type', 'status', 'side', 'action', 'amount', 'price',
                  'cost', 'filled', 'average', 'dt_created', 'dt_modified')
        exclude = ('ID',)


class PercentColumn(tables.Column):
    def render(self, value):
        return str(round(value * 100, 1)) + '%'


class AssetTable(tables.Table):

    currency = tables.Column(verbose_name='Asset')
    wallet = tables.Column(verbose_name='Wallet')
    total = tables.Column(verbose_name='Total')
    free = tables.Column(verbose_name='Free')
    used = tables.Column(verbose_name='Reserved')
    total_value = tables.Column(verbose_name='Value')
    weight = PercentColumn(verbose_name='Weight')

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
        fields = ('currency', 'wallet', 'total', 'free', 'used', 'total_value', 'weight', 'dt_modified')
        exclude = ('ID',)


class PositionTable(tables.Table):

    market__symbol = tables.Column(verbose_name='Market')
    size = tables.Column(verbose_name='Size')
    side = tables.Column(verbose_name='Side')
    entry_price = tables.Column(verbose_name='Entry price')
    liquidation_price = tables.Column(verbose_name='Liquidation price')
    last = tables.Column(verbose_name='Last price')
    unrealized_pnl = tables.Column(verbose_name='Unrealized PnL')
    notional_value = tables.Column(verbose_name='Notional value')

    dt_modified = tables.DateTimeColumn(format='M d Y, h:i:s')
    dt_modified = tables.Column(verbose_name='Last update')

    def render_entry_price(self, **kwargs):
        return round(kwargs['value'], 1)

    def render_liquidation_price(self, **kwargs):
        return round(kwargs['value'], 1)

    def render_last(self, **kwargs):
        return round(kwargs['value'], 1)

    def render_notional_value(self, **kwargs):
        return round(kwargs['value'], 1)

    def render_unrealized_pnl(self, **kwargs):
        return round(kwargs['value'], 2)

    class Meta:
        model = Asset
        fields = ('market__symbol', 'size', 'notional_value', 'side',  'unrealized_pnl', 'entry_price',
                  'liquidation_price', 'last', 'dt_modified')


class ReturnTable(tables.Table):
    ret_1h = tables.Column('1h')
    ret_24h = tables.Column('24h')
    ret_7d = tables.Column('7d')
    Datetime = tables.Column()

    class Meta:
        sequence = ('Datetime', 'ret_1h', 'ret_24h', 'ret_7d')
