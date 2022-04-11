from django.contrib import admin
from datetime import timedelta, datetime
from marketsdata.models import Exchange, Market, Candle, Currency, Tickers, Candles
import structlog
import locale
from celery import group
from marketsdata.tasks import *
import time
from celery import chain

locale.setlocale(locale.LC_ALL, '')
log = structlog.get_logger(__name__)


@admin.register(Exchange)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'get_status', "timeout", "rate_limit", "updated_at",
                    'get_currencies', 'get_markets', 'precision_mode', 'start_date', 'limit_ohlcv',)
    readonly_fields = ('options', 'status', 'url', 'status_at', 'eta', 'version', 'api', 'countries',
                       'urls', 'rate_limits', 'credit', 'credit_max', 'has', 'timeframes', 'precision_mode',
                       'credentials')
    actions = ['update_status', 'update_properties', 'update_currencies', 'update_markets', 'update_prices',
               'update_strategies']
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

    ##########
    # Action #
    ##########

    # Update status
    def update_status(self, request, queryset):
        for exchange in queryset:
            update_ex_status.delay(exchange.exid)

    update_status.short_description = "Update status"

    # Update properties
    def update_properties(self, request, queryset):
        for exchange in queryset:
            update_ex_properties.delay(exchange.exid)

    update_properties.short_description = "Update properties"

    # Update currencies
    def update_currencies(self, request, queryset):
        for exchange in queryset:
            update_ex_currencies.delay(exchange.exid)

    update_currencies.short_description = "Update currencies"

    # Update markets
    def update_markets(self, request, queryset):
        for exchange in queryset:
            update_ex_markets.delay(exchange.exid)

    update_markets.short_description = "Update markets"

    # Update prices
    def update_prices(self, request, queryset):
        for exchange in queryset:
            update_ticker.delay(exchange.exid)

    update_prices.short_description = "Update prices"

    # Update strategies
    def update_strategies(self, request, queryset):
        for exchange in queryset:
            update_strategies.delay(exchange.exid, signal=False)

    update_strategies.short_description = "Update strategies"

    # Fetch markets history
    def fetch_candle_history(self, request, queryset):
        for exchange in queryset:
            pass

    fetch_candle_history.short_description = "Fetch candles history"


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
                    'contract_currency', 'dt_created')
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


@admin.register(Candles)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('market', 'year', 'semester', 'count_records', 'latest_timestamp')
    readonly_fields = ('market', 'year', 'semester', 'dt_created', 'data')
    list_filter = ('year', 'semester',
                   ('market__quote', admin.RelatedOnlyFieldListFilter),
                   ('market__base', admin.RelatedOnlyFieldListFilter)
                   )
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
