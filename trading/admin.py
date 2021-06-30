from prettyjson import PrettyJSONWidget
from django.contrib import admin
from trading.models import Account, Fund, Position, Order, Transfer
from trading import tasks
import structlog
from celery import chain, group
from django.contrib.admin import SimpleListFilter
from django.contrib.postgres.fields import JSONField
from prettyjson import PrettyJSONWidget
import json
from pygments import highlight, formatters, lexers
from django.utils.safestring import mark_safe

log = structlog.get_logger(__name__)


@admin.register(Account)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'exchange', 'trading', 'updated', 'valid_credentials', 'strategy',
                    'get_limit_price_tolerance', 'limit_order','updated_at',)
    readonly_fields = ('valid_credentials', 'user')
    actions = ['update_credentials', 'update_fund', 'update_positions', 'fetch_order_open_all', 'rebalance']
    save_as = True
    save_on_top = True

    # Columns

    def get_limit_price_tolerance(self, obj):
        return str(round(obj.limit_price_tolerance * 100, 2)) + '%'

    get_limit_price_tolerance.short_description = 'Limit Price Tolerance'

    def get_fund(self, obj):
        return int(Fund.objects.filter(account=obj).latest('dt_create').total)

    get_fund.short_description = "Total"

    # Actions

    def rebalance(self, request, queryset):

        chains = [chain(
            tasks.rebalance.s(account.strategy.id, account.id).set(queue='slow')
        ) for account in queryset]

        result = group(*chains).delay()

    rebalance.short_description = "Rebalance"

    def update_credentials(self, request, queryset):
        for account in queryset:
            account.update_credentials()

    update_credentials.short_description = "Update credentials"

    def update_fund(self, request, queryset):
        for account in queryset:
            tasks.create_fund(account.id)

    update_fund.short_description = "Update fund"

    def update_positions(self, request, queryset):
        for account in queryset:
            tasks.update_positions(account.id)

    update_positions.short_description = "Update positions"

    def fetch_order_open_all(self, request, queryset):
        for account in queryset:
            tasks.fetch_order_open(account)

    fetch_order_open_all.short_description = "Fetch all open orders"


@admin.register(Fund)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('dt', 'account', 'exchange', 'balance', )
    readonly_fields = ('account', 'exchange', 'balance', 'dt_create', 'dt', 'total', 'free', 'used', 'margin_assets')
    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        ('exchange', admin.RelatedOnlyFieldListFilter)
    )

    # def get_balance(self, obj):
    #     return f'{int(round(sum([v for k, v in obj.balance.items()]), 2)):n}'
    #
    # get_balance.short_description = 'Balance'


@admin.register(Order)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('id', 'account', 'market', 'action', 'status', 'side', 'amount', 'cost',
                    'price', 'price_strategy', 'distance', 'filled',  'dt_create', 'dt_update')

    readonly_fields = ('orderid', 'account', 'market', 'status',  'type', 'amount', 'side', 'params',
                       'cost', 'filled', 'average', 'remaining', 'timestamp', 'max_qty', 'trades',
                       'last_trade_timestamp', 'price', 'price_strategy', 'distance', 'fee', 'datetime',
                       'response',
                       'route', 'segments')
    actions = ['place_order', 'refresh', 'cancel_order']

    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        ('market', admin.RelatedOnlyFieldListFilter),
        'status',
        'type',
    )

    # Columns
    #
    # def get_strategy(self, obj):
    #     return obj.account.strategy.name  # .split(' ')[2].split('.')[0]
    #
    # get_strategy.short_description = 'Strategy'

    # def get_exchange(self, obj):
    #     return obj.account.exchange.name  # .split(' ')[2].split('.')[0]
    #
    # get_exchange.short_description = 'Exchange'

    # Actions

    def place_order(self, request, queryset):
        for order in queryset:
            order.place()
            log.info('Order placed')

    place_order.short_description = 'Place order'

    def refresh(self, request, queryset):
        for order in queryset:
            order.refresh()

    refresh.short_description = 'Refresh order'

    def cancel_order(self, request, queryset):
        for order in queryset:
            order.cancel()

    cancel_order.short_description = 'Cancel order'


@admin.register(Position)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('account', 'exchange', 'market', 'get_side', 'size', 'get_asset','get_notional_value',
                    'settlement',
                    'get_value_usd', 'get_initial_margin', 'leverage', 'get_contract_value',
                    'get_contract_value_curr', 'get_liquidation_price',
                    'last',
                    'entry_price', 'realized_pnl', 'unrealized_pnl', 'margin_mode',
                    'dt_update',)
    readonly_fields = ('account', 'exchange', 'market', 'side', 'size', 'asset', 'value_usd', 'settlement',
                       'notional_value',
                       'entry_price', 'initial_margin', 'maint_margin', 'order_initial_margin', 'user',
                       'last', 'hedge', 'liquidation_price',
                       'realized_pnl', 'unrealized_pnl',
                       'margin_mode', 'leverage', 'dt_update', 'dt_create', 'instrument_id', 'created_at',
                       'response', 'response_2', 'max_qty')
    actions = ['refresh_position', 'close_position']
    list_filter = (
        ('exchange', admin.RelatedOnlyFieldListFilter),
        ('account', admin.RelatedOnlyFieldListFilter),
        ('market', admin.RelatedOnlyFieldListFilter)
    )

    def get_side(self, obj):
        return True if obj.side == 'buy' else False
    get_side.boolean = True
    get_side.short_description = 'Side'

    def get_asset(self, obj):
        if obj.size:
            return obj.asset if obj.asset else 'Cont'
    get_asset.short_description = 'Asset'

    def get_notional_value(self, obj):
        if obj.notional_value:
            return round(obj.notional_value, 2)
    get_notional_value.short_description = 'Notional Value'

    def get_initial_margin(self, obj):
        if obj.initial_margin:
            return round(obj.initial_margin, 2)
    get_initial_margin.short_description = 'Initial Margin'

    def get_value_usd(self, obj):
        if obj.value_usd:
            return round(obj.value_usd, 2)
    get_value_usd.short_description = 'Dollar Value'

    def get_contract_value(self, obj):
        if obj.market:
            return obj.market.contract_value
    get_contract_value.short_description = 'Contract value'

    def get_contract_value_curr(self, obj):
        if obj.market:
            return obj.market.contract_value_currency
    get_contract_value_curr.short_description = 'Contract currency'

    def get_liquidation_price(self, obj):
        if obj.liquidation_price:
            return round(obj.liquidation_price, 2)
    get_liquidation_price.short_description = 'Liquidation'

    # Action

    def close_position(self, request, queryset):
        for position in queryset:
            position.close()

    close_position.short_description = 'Close position'

    def refresh_position(self, request, queryset):
        accounts = [position.account for position in queryset]
        for account in accounts:
            account.refresh_positions()

    refresh_position.short_description = 'Refresh position'


@admin.register(Transfer)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('transferid', 'account', 'exchange', 'currency', 'amount', 'from_wallet', 'to_wallet', 'status')
    readonly_fields = ('transferid', 'account', 'exchange', 'currency', 'amount', 'from_wallet', 'to_wallet', 'status',
                       'response', 'datetime', 'timestamp', 'user')

    list_filter = (
        ('exchange', admin.RelatedOnlyFieldListFilter),
        ('account', admin.RelatedOnlyFieldListFilter),
        ('currency', admin.RelatedOnlyFieldListFilter)
    )
