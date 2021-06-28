from django.contrib import admin
from datetime import timedelta
from .models import Exchange, Market, Candle, Currency, CurrencyType, OrderBook
import structlog
import locale
from celery import group
from marketsdata import tasks
import time

locale.setlocale(locale.LC_ALL, '')
log = structlog.get_logger(__name__)


@admin.register(Exchange)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'get_status', "timeout", "rate_limit", "updated_at",
                    'get_currencies', 'get_markets', 'get_markets_not_updated', 'get_markets_not_populated',
                    'precision_mode', 'start_date',
                    'limit_ohlcv',)
    readonly_fields = ('options', 'status', 'url', 'status_at', 'eta', 'version', 'api', 'countries',
                       'urls', 'rate_limits', 'credit', 'credit_max',
                       'has', 'timeframes',
                       'precision_mode', 'credentials')
    actions = ['insert_candles_history_since_launch', 'insert_candles_history_recent', 'update_exchange_currencies',
               'update_exchange_markets', 'update_market_price', 'update_exchange_status', 'update_prices',
               'update_top_markets']
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

    def get_markets_not_updated(self, obj):
        not_upd = [m.symbol for m in Market.objects.filter(exchange=obj)
                   if (m.active and m.is_populated() and not m.excluded
                       and (not m.is_updated() and not m.derivative == 'future'))]
        return len(not_upd)

    get_markets_not_updated.short_description = "Not updated"

    def get_markets_not_populated(self, obj):
        not_upd = [m.symbol for m in Market.objects.filter(exchange=obj) if m.active
                   and not m.is_populated() and not m.excluded]
        return len(not_upd)

    get_markets_not_populated.short_description = "Not populated"

    ##########
    # Action #
    ##########

    def insert_candles_history_since_launch(self, request, queryset):
        #
        # Create a Celery task that handle retransmissions and run it
        #
        res = group([tasks.insert_candle_history.s(exid=exchange.exid)
                    .set(queue='slow') for exchange in queryset]).apply_async()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Insert complete')

        else:
            log.error('Insert failed :(')

    insert_candles_history_since_launch.short_description = "Insert candles history since launch"

    def insert_candles_history_recent(self, request, queryset):
        #
        # Download only the latest history since the last candle received
        #
        res = group([tasks.run.s(exchange.exid).set(queue='slow')
                     for exchange in queryset]).apply_async()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Insert recent complete')

        else:
            log.error('Insert recent failed :(')

    insert_candles_history_recent.short_description = "Insert candles history recent"

    def update_exchange_currencies(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        gp = group(tasks.update_currencies.s(exchange) for exchange in exchanges)
        result = gp.delay()

    update_exchange_currencies.short_description = "Update currencies"

    def update_prices(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        res = group(tasks.update_prices.s(exchange) for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update prices complete')

        else:
            log.error('Update prices failed :(')

    update_prices.short_description = "Update prices"

    def update_top_markets(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        res = group(tasks.update_top_markets.s(exchange) for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update top markets complete')

        else:
            log.error('Update top markets failed :(')

    update_top_markets.short_description = "Update top markets"

    def update_exchange_markets(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        res = group(tasks.update_markets.s(exchange) for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update currencies complete')

        else:
            log.error('Update currencies failed :(')

    update_exchange_markets.short_description = "Update markets"

    def update_market_prices(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        res = group(tasks.update_prices.s(exchange) for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update market complete')

        else:
            log.error('Update market failed :(')

    update_market_prices.short_description = "Update market price"

    def update_exchange_status(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]

        # Create a groups and execute task
        gp = group(tasks.update_status.s(exchange) for exchange in exchanges)
        result = gp.delay()

    update_exchange_status.short_description = "Update status"


@admin.register(Currency)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('code', 'exchanges', 'get_types', 'get_stable_coin')
    readonly_fields = ('exchange', 'code', 'type', 'stable_coin')  # Modify config.ini to change type
    list_filter = ('exchange', 'type', 'stable_coin', 'code',)
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
                    'margined', 'contract_value', 'contract_value_currency')
    readonly_fields = ('symbol', 'exchange', 'top', 'excluded', 'type', 'ccxt_type_response', 'default_type',
                       'derivative', 'active', 'quote', 'base', 'funding_rate', 'margined',
                       'contract_value_currency', 'listing_date', 'delivery_date', 'contract_value', 'amount_min',
                       'amount_max',
                       'price_min', 'price_max', 'cost_min', 'cost_max', 'order_book', 'config', 'limits', 'precision',
                       'taker', 'maker', 'response')
    list_filter = ('exchange', 'type', 'derivative', 'active', 'excluded', 'default_type',
                   ('margined', admin.RelatedOnlyFieldListFilter),
                   ('quote', admin.RelatedOnlyFieldListFilter),
                   ('base', admin.RelatedOnlyFieldListFilter)
                   )
    ordering = ('excluded', '-active', 'symbol',)
    actions = ['insert_candles_history_since_launch', 'insert_candles_history_recent']
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

    def insert_candles_history_since_launch(self, request, queryset):
        #
        # Download all history since exchange launch
        #
        for market in queryset.order_by('symbol'):
            # Create a Celery task that handle retransmissions and run it
            tasks.insert_candle_history.s(exid=market.exchange.exid,
                                          type=market.type,
                                          derivative=market.derivative,
                                          symbol=market.symbol
                                          ).apply_async(queue='slow')

            if market.exchange.exid == 'bitmex':
                time.sleep(5)

    insert_candles_history_since_launch.short_description = "Insert candles history since launch"

    def insert_candles_history_recent(self, request, queryset):
        #
        # Download only the latest history since the last candle received
        #
        for market in queryset.order_by('symbol'):

            if not market.is_populated():
                continue

            # Create a Celery task that handle retransmissions and run it
            tasks.insert_candle_history.s(exid=market.exchange.exid,
                                          type=market.type,
                                          derivative=market.derivative,
                                          symbol=market.symbol,
                                          start=market.get_candle_datetime_last()
                                          ).apply_async(queue='slow')

    insert_candles_history_recent.short_description = "Insert candles history recent"


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
