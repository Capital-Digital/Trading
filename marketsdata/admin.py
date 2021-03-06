from django.contrib import admin
from datetime import timedelta, datetime
from marketsdata.models import Exchange, Market, Candle, Currency, Tickers, Candles
import structlog
import locale
from celery import group
from marketsdata.tasks import *
from strategy.tasks import *
from trading.tasks import *
import time
from celery import chain

locale.setlocale(locale.LC_ALL, '')
log = structlog.get_logger(__name__)


@admin.register(Exchange)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'get_status', "timeout", "rate_limit", "updated_at", "get_df_latest_index",
                    'get_currencies', 'get_markets', 'precision_mode', 'start_date', 'limit_ohlcv',)
    readonly_fields = ('options', 'status', 'url', 'status_at', 'eta', 'version', 'api', 'countries',
                       'urls', 'rate_limits', 'credit', 'credit_max', 'has', 'timeframes', 'precision_mode',
                       'credentials')
    actions = ['action_update_status', 'action_update_properties', 'action_update_currencies', 'action_update_markets',
               'action_preload_dataframe', 'action_update_dataframe', 'action_update_prices',
               'action_update_strategies', 'action_rebalance_accounts']
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

    def get_df_latest_index(self, obj):
        return obj.is_data_updated()
        # if hasattr(obj, 'data'):
        #     if isinstance(obj.data, pd.DataFrame):
        #         log.info('Last row'.format(list(obj.data.index[-1])))
        #         return list(obj.data.index[-1])

    get_df_latest_index.short_description = "Last index"

    ##########
    # Action #
    ##########

    # Update status
    def action_update_status(self, request, queryset):
        for exchange in queryset:
            update_status.delay(exchange.exid)

    action_update_status.short_description = "Update status"

    # Update properties
    def action_update_properties(self, request, queryset):
        for exchange in queryset:
            update_properties.delay(exchange.exid)

    action_update_properties.short_description = "Update properties"

    # Update currencies
    def action_update_currencies(self, request, queryset):
        for exchange in queryset:
            update_currencies.delay(exchange.exid)

    action_update_currencies.short_description = "Update currencies"

    # Update markets
    def action_update_markets(self, request, queryset):
        for exchange in queryset:
            update_markets.delay(exchange.exid)

    action_update_markets.short_description = "Update markets"

    # Update prices
    def action_update_prices(self, request, queryset):
        for exchange in queryset:
            for wallet in exchange.get_wallets():
                update_prices.delay(exchange.exid, wallet)

    action_update_prices.short_description = "Update prices"

    # Preload dataframes
    def action_preload_dataframe(self, request, queryset):
        for exchange in queryset:
            preload_dataframe.delay(exchange.exid)

    action_preload_dataframe.short_description = "Preload dataframes"

    # Update dataframes
    def action_update_dataframe(self, request, queryset):
        for exchange in queryset:
            update_dataframe.delay(exchange.exid, tickers=False)

    action_update_dataframe.short_description = "Update dataframes"

    # Update strategies
    def action_update_strategies(self, request, queryset):
        for exchange in queryset:
            bulk_update_strategies.delay(exchange.exid, False)

    action_update_strategies.short_description = "Update strategies"

    # Rebalance accounts
    def action_rebalance_accounts(self, request, queryset):
        for exchange in queryset:
            for strategy in Strategy.objects.filter(exchange=exchange, production=True):
                bulk_rebalance.delay(strategy.id, reload=True)

    action_rebalance_accounts.short_description = "Rebalance accounts"

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
