from django.contrib import admin
from datetime import timedelta
from .models import Exchange, Market, Candle, Currency, OrderBook
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
    actions = ['insert_full_ohlcv', 'insert_recent_ohlcv', 'update_currencies',
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

    def insert_full_ohlcv(self, request, queryset):

        # Retrieve listing data from CMC
        mcap = tasks.get_mcap()

        exids = [exchange.exid for exchange in queryset]
        chains = [tasks.insert_ohlcv_bulk(exid, mcap, recent=None) for exid in exids]
        res = group(*chains).delay()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Insert full OHLCV complete')

        else:
            log.error('Insert full OHLCV failed')

    insert_full_ohlcv.short_description = "Insert full OHLCV"

    def insert_recent_ohlcv(self, request, queryset):

        # Retrieve listing data from CMC
        print('CMC')
        mcap = tasks.get_mcap()
        print('CMC is', mcap)

        for exchange in queryset:
            markets = Market.objects.filter(exchange=exchange).order_by('symbol')
            chains = [tasks.insert_ohlcv.si(exchange.exid,
                                            market.wallet,
                                            market.symbol,
                                            mcap,
                                            True
                                            ).set(queue='slow') for market in markets]
            res = chain(*chains).delay()
            while not res.ready():
                time.sleep(0.5)

            if res.successful():
                log.info('Insert recent OHLCV complete')

            else:
                log.error('Insert recent OHLCV failed')

        # exids = [exchange.exid for exchange in queryset]
        # groups = [tasks.insert_ohlcv_bulk.s(exid, recent=True) for exid in exids]
        # res = group(*groups).delay()
        #
        # while not res.ready():
        #     time.sleep(0.5)
        #
        # if res.successful():
        #     log.info('Insert recent OHLCV complete')
        #
        # else:
        #     log.error('Insert recent OHLCV failed')

    insert_recent_ohlcv.short_description = "Insert recent OHLCV"

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

    def flag_top_markets(self, request, queryset):
        exchanges = [exchange.exid for exchange in queryset]
        res = group(tasks.top_markets.s(exchange).set(queue='default') for exchange in exchanges)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Flag top markets complete')

        else:
            log.error('Flag top markets failed')

    flag_top_markets.short_description = "Flag top markets"

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


@admin.register(OrderBook)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name',)
    save_as = True
    save_on_top = True


@admin.register(Market)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('symbol', 'exchange', 'type', 'trading', 'updated', 'candles_number',
                    'margined', 'contract_value', 'contract_currency')
    readonly_fields = ('symbol', 'exchange', 'top', 'trading', 'updated', 'type',
                       'wallet', 'contract_type', 'status',
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
        res = group(tasks.insert_ohlcv.s(market.exchange.exid,
                                         market.wallet,
                                         market.symbol,
                                         recent=None
                                         ).set(queue='slow') for market in queryset)()

    insert_candles_history_since_launch.short_description = "Insert candles history since launch"

    def insert_candles_history_recent(self, request, queryset):
        res = group(tasks.insert_ohlcv.s(market.exchange.exid,
                                         market.wallet,
                                         market.symbol,
                                         recent=True
                                         ).set(queue='slow') for market in queryset)()

        while not res.ready():
            time.sleep(0.5)

        if res.successful():
            log.info('Update currencies complete')

        else:
            log.error('Update currencies failed')

    insert_candles_history_recent.short_description = "Insert candles history recent"


@admin.register(Candle)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('get_dt', 'market', 'exchange', 'get_type', 'close', 'get_vol', 'get_vol_avg', 'volume_mcap')
    readonly_fields = ('exchange', 'market', 'dt', 'close', 'volume', 'dt_created', 'volume_avg', 'mcap', 'volume_mcap')
    list_filter = ('exchange', 'market__type', 'market__contract_type',
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

