from django.contrib import admin
from datetime import timedelta, datetime
from .models import Exchange, Market, Candle, Currency, Tickers, CoinPaprika, Candles
import structlog
import locale
from celery import group
from marketsdata import tasks
import time
from celery import chain

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
    actions = ['fetch_candle_history', 'update_currencies',
               'update_markets', 'update_price', 'update_status', 'update_prices',
               'flag_top_markets']
    save_as = True
    save_on_top = True

    ###########
    # Columns #
    ###########

    def get_status(self, obj):
        return True if obj.status == 'ok' else False

    get_status.boolean = True
    get_status.short_description = "Status"

    def get_currencies(self, obj):
        return Currency.objects.filter(exchange=obj).count()

    get_currencies.short_description = "Currencies"

    def get_markets(self, obj):
        return Market.objects.filter(exchange=obj).count()

    get_markets.short_description = "Markets"

    def get_markets_not_updated(self, obj):
        not_upd = [m.symbol for m in Market.objects.filter(exchange=obj,
                                                           trading=True,
                                                           updated=False
                                                           )]
        return len(not_upd)

    get_markets_not_updated.short_description = "Not updated"

    def get_markets_not_populated(self, obj):
        not_upd = [m.symbol for m in Market.objects.filter(exchange=obj,
                                                           trading=True
                                                           ) if not m.is_populated()]
        return len(not_upd)

    get_markets_not_populated.short_description = "Not populated"

    ##########
    # Action #
    ##########

    # Fetch markets history
    def fetch_candle_history(self, request, queryset):

        exids = [i.exid for i in queryset]
        res = group(tasks.fetch_candle_history.s(exid).set(queue='default') for exid in exids)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Fetch markets history complete')

        else:
            log.error('Fetch markets history error')

    fetch_candle_history.short_description = "Fetch candles history"

    # Fetch markets history
    def update_currencies(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]
        res = group(tasks.currencies.s(exchange).set(queue='default') for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update currencies complete')

        else:
            log.error('Update currencies failed')

    update_currencies.short_description = "Update currencies"

    # Update prices
    def update_prices(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]
        res = group(tasks.prices.s(exchange).set(queue='default') for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update prices complete')

        else:
            log.error('Update prices failed')

    update_prices.short_description = "Update prices"

    # Update markets
    def update_markets(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]
        res = group(tasks.markets.s(exchange).set(queue='default') for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update markets complete')

        else:
            log.error('Update markets failed')

    update_markets.short_description = "Update markets"

    # Update price
    def update_prices(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]
        res = group(tasks.prices.s(exchange).set(queue='default') for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update prices complete')

        else:
            log.error('Update prices failed')

    update_prices.short_description = "Update price"

    # Update status
    def update_status(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]
        res = group(tasks.status.s(exchange).set(queue='default') for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update status complete')

        else:
            log.error('Update status failed')

    update_status.short_description = "Update status"


@admin.register(Currency)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('code', 'exchanges', 'get_stable_coin')
    readonly_fields = ('exchange', 'code', 'stable_coin')
    list_filter = ('exchange', 'stable_coin', 'code',)
    save_as = True
    save_on_top = True

    ###########
    # Columns #
    ###########

    def exchanges(self, obj):
        return [ex.exid for ex in obj.exchange.all()]

    exchanges.short_description = 'Exchanges'

    def get_stable_coin(self, obj):
        return obj.stable_coin

    get_stable_coin.boolean = False
    get_stable_coin.short_description = 'Stable coin'


@admin.register(Market)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'exchange', 'type', 'trading', 'updated', 'margined', 'contract_value',
                    'contract_currency')
    readonly_fields = ('symbol', 'exchange', 'top', 'trading', 'updated', 'type', 'wallet', 'contract_type', 'status',
                       'quote', 'base', 'funding_rate', 'margined',
                       'contract_currency', 'listing_date', 'delivery_date', 'contract_value', 'amount_min',
                       'amount_max',
                       'price_min', 'price_max', 'cost_min', 'cost_max', 'order_book', 'config', 'limits', 'precision',
                       'taker', 'maker', 'response')
    list_filter = ('exchange', 'type', 'contract_type', 'status', 'trading', 'wallet',
                   ('margined', admin.RelatedOnlyFieldListFilter),
                   ('quote', admin.RelatedOnlyFieldListFilter),
                   ('base', admin.RelatedOnlyFieldListFilter)
                   )
    ordering = ('-trading', 'symbol',)
    actions = ['fetch_history', ]
    save_as = True
    save_on_top = True

    ###########
    # Columns #
    ###########

    # Return the datetime of the most recent candle
    def latest(self, obj):
        if obj.is_populated():
            latest = obj.get_candle_latest_dt() - timedelta(hours=0)
            return latest.strftime("%m/%d/%Y %H:%M:%S")

    latest.short_description = 'Last update UTC'

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


@admin.register(CoinPaprika)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('currency', 'year', 'semester', 'count_records', 'latest_timestamp')
    readonly_fields = ('name', 'currency', 'year', 'semester', 'dt_created', 'data')
    list_filter = ('year', 'semester', 'currency__code',)
    ordering = ('-year', '-semester', 'currency',)
    save_as = True

    def count_records(self, obj):
        if obj.data:
            return len(obj.data)

    count_records.short_description = 'Records'

    def latest_timestamp(self, obj):
        if obj.data:
            log.info('Display {0}'.format(obj.currency.code))
            return # obj.data[-1]['timestamp'][:16]

    latest_timestamp.short_description = 'Latest'


@admin.register(Candles)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('market', 'year', 'semester', 'count_records', 'latest_timestamp')
    readonly_fields = ('market', 'year', 'semester', 'dt_created', 'data')
    list_filter = ('year', 'semester', 'market__quote__code', 'market__base__code',)
    ordering = ('-year', '-semester', 'market',)
    save_as = True
    actions = ['update_candles', ]

    def count_records(self, obj):
        if obj.data:
            return len(obj.data)

    count_records.short_description = 'Records'

    def latest_timestamp(self, obj):
        if obj.data:
            return obj.data[-1][0][:16]

    latest_timestamp.short_description = 'Latest'

    ##########
    # Action #
    ##########


@admin.register(Tickers)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('market', 'year', 'semester', 'count_records', 'latest_timestamp')
    readonly_fields = ('market', 'year', 'semester', 'dt_created', 'data')
    list_filter = ('year', 'semester', 'market__base__code')
    ordering = ('-year', '-semester', 'market',)

    def count_records(self, obj):
        if obj.data:
            return len(obj.data)

    count_records.short_description = 'Records'

    def latest_timestamp(self, obj):
        if obj.data:
            return # obj.data[-1][0][:16]

    latest_timestamp.short_description = 'Latest'
