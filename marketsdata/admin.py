from django.contrib import admin
from datetime import timedelta
from .models import Exchange, Market, Candle, Currency, CurrencyType, OrderBook
import structlog
import locale
from celery import chain, group
from marketsdata import tasks

locale.setlocale(locale.LC_ALL, '')
log = structlog.get_logger(__name__)


@admin.register(Exchange)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'supported_market_types', 'get_status', "timeout", "rate_limit", "updated_at", 'status_at',
                    'get_currencies', 'get_markets', 'precision_mode', 'start_date', 'limit_ohlcv',)
    readonly_fields = ('options', 'status', 'url', 'status_at', 'eta', 'version', 'api', 'countries',
                       'urls', 'credit',
                       'has', 'timeframes',
                       'precision_mode', 'credentials')
    actions = ['insert_candles_history', 'update_exchange_currencies', 'update_exchange_markets', 'update_market_price']
    save_as = True
    save_on_top = True

    ###########
    # Columns #
    ###########

    def get_status(self, obj):
        return True if obj.status == 'ok' else False

    get_status.boolean = True
    get_status.short_description = "Online"

    def get_currencies(self, obj):
        return Currency.objects.filter(exchange=obj).count()

    get_currencies.short_description = "Currencies"

    def get_markets(self, obj):
        return Market.objects.filter(exchange=obj).count()

    get_markets.short_description = "Markets"

    ##########
    # Action #
    ##########

    def insert_candles_history(self, request, queryset):

        # Create a Celery task that handle retransmissions and run it
        group([tasks.insert_candle_history.s(exid=exchange.exid) for exchange in queryset])()

    insert_candles_history.short_description = "Insert candles history"

    def update_exchange_currencies(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        gp = group(tasks.update_exchange_currencies.s(exchange) for exchange in exchanges)
        result = gp.delay()

    update_exchange_currencies.short_description = "Update currencies"

    def update_exchange_markets(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        gp = group(tasks.update_exchange_markets.s(exchange) for exchange in exchanges)
        result = gp.delay()

    update_exchange_markets.short_description = "Update markets"

    def update_market_prices(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        gp = group(tasks.update_market_prices.s(exchange) for exchange in exchanges)
        result = gp.delay()

    update_market_prices.short_description = "Update market price"


@admin.register(Currency)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('code', 'exchanges','get_types', 'get_stable_coin')
    readonly_fields = ('exchange', 'code', )
    list_filter = ('exchange', 'stable_coin', 'code',)
    save_as = True
    save_on_top = True

    ###########
    # Columns #
    ###########

    def exchanges(self, obj):
        return [ex.exid for ex in obj.exchange.all()]

    exchanges.short_description = 'Exchanges'

    def get_types(self, obj):
        return [t.type for t in obj.type.all()]

    get_types.short_description = 'Type'

    def get_stable_coin(self, obj):
        return obj.stable_coin

    get_stable_coin.boolean = False
    get_stable_coin.short_description = 'Stable coin'


@admin.register(CurrencyType)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('type',)


@admin.register(OrderBook)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name',)
    save_as = True
    save_on_top = True


@admin.register(Market)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'exchange', 'type', 'derivative', 'active', 'is_updated', 'candles_number',
                    'margined', 'contract_value', 'contract_value_currency',)
    readonly_fields = ('symbol', 'exchange', 'type', 'type_ccxt', 'derivative', 'active', 'quote', 'base', 'margined',
                       'contract_value_currency', 'listing_date', 'delivery_date', 'contract_value', 'amount_min',
                       'amount_max',
                       'price_min', 'price_max', 'cost_min', 'cost_max', 'order_book', 'config', 'limits', 'precision',
                       'taker', 'maker', 'response')
    list_filter = ('exchange', 'type', 'derivative', 'active',
                   ('quote', admin.RelatedOnlyFieldListFilter),
                   ('base', admin.RelatedOnlyFieldListFilter)
                   )
    ordering = ('-active', 'symbol',)
    actions = ['insert_candles_history']
    save_as = True
    save_on_top = True

    ###########
    # Columns #
    ###########

    # Return the number of candles
    def candles_number(self, obj):
        return Candle.objects.filter(market=obj).count()

    candles_number.short_description = 'Candles'

    # Return the datetime of the most recent candle
    def latest(self, obj):
        if obj.is_populated():
            latest = obj.get_candle_latest_dt() - timedelta(hours=0)
            return latest.strftime("%m/%d/%Y %H:%M:%S")

    latest.short_description = 'Last update UTC'

    # Return True if the market is up to date
    def is_updated(self, obj):
        return obj.is_updated()

    is_updated.boolean = True
    is_updated.short_description = 'Updated'

    # Return Limit price min
    def get_limit_price_min(self, obj):
        return obj.limits['price']['min']

    get_limit_price_min.short_description = "Price min"

    # Return Precision amount
    def get_precision_amount(self, obj):
        return obj.precision['amount']

    get_precision_amount.short_description = "Precision amount"

    # Return Precision price
    def get_precision_price(self, obj):
        return obj.precision['price']

    get_precision_price.short_description = "Precision price"

    ##########
    # Action #
    ##########

    def insert_candles_history(self, request, queryset):
        for market in queryset:
            # Create a Celery task that handle retransmissions and run it
            task = tasks.insert_candle_history.s(exid=market.exchange.exid,
                                                 type=market.type,
                                                 derivative=market.derivative,
                                                 symbol=market.symbol
                                                 )
            task.delay()

    insert_candles_history.short_description = "Insert candles history"


@admin.register(Candle)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('get_dt', 'market', 'exchange', 'get_type', 'close', 'get_vol', 'get_vol_avg')
    readonly_fields = ('exchange', 'market', 'dt', 'close', 'volume', 'dt_created', 'volume_avg',)
    list_filter = ('exchange', 'market__type', 'market__derivative',
                   ('market__quote', admin.RelatedOnlyFieldListFilter),
                   ('market__base', admin.RelatedOnlyFieldListFilter)
                   )
    ordering = ('-dt',)

    ###########
    # Columns #
    ###########

    def get_vol(self, obj):
        if obj.volume:
            return f'{int(obj.volume):n}'

    get_vol.short_description = 'Volume'

    def get_vol_avg(self, obj):
        if obj.volume_avg:
            return f'{int(obj.volume_avg):n}'

    get_vol_avg.short_description = 'Volume average'

    def get_type(self, obj):
        return obj.market.type

    get_type.short_description = 'Type'

    def get_dt(self, obj):
        return obj.dt

    get_dt.short_description = 'Datetime UTC'
