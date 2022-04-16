from prettyjson import PrettyJSONWidget
from django.contrib import admin
from marketsdata.tasks import update_account
from trading.models import Account, Fund, Position, Order, Transfer
from trading.tasks import *
import structlog
from celery import chain, group
from django.contrib.admin import SimpleListFilter
from django.contrib.postgres.fields import JSONField
from prettyjson import PrettyJSONWidget
import json
from pygments import highlight, formatters, lexers
from django.utils.safestring import mark_safe
from django.db.models import Avg

log = structlog.get_logger(__name__)


@admin.register(Account)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'user', 'exchange', 'quote', 'active', 'trading', 'valid_credentials',
                    'get_limit_price_tolerance', 'updated_at',)
    readonly_fields = ('valid_credentials', 'user')
    actions = ['check_credentials', 'rebalance_account', 'market_sell_spot', 'market_close_positions']
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

    def market_sell_spot(self, request, queryset):
        for account in queryset:
            log.info('Market sell', account=account.name)
            market_sell.delay(account.id)

    market_sell_spot.short_description = "Market sell"

    def market_close_positions(self, request, queryset):
        for account in queryset:
            log.info('Market close', account=account.name)
            market_close.delay(account.id)

    market_close_positions.short_description = "Market close"

    def rebalance_account(self, request, queryset):
        for account in queryset:
            update_account.delay(account.id, signal=False)

    rebalance_account.short_description = "Rebalance"

    def check_credentials(self, request, queryset):
        for account in queryset:
            check_account_cred.delay(account.id)

    check_credentials.short_description = "Check credentials"


@admin.register(Fund)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('dt', 'account', 'exchange', 'balance', )
    readonly_fields = ('account', 'exchange', 'balance', 'dt_create', 'dt', 'total', 'free', 'used', 'margin_assets')
    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        ('exchange', admin.RelatedOnlyFieldListFilter)
    )


@admin.register(Order)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('clientid', 'account', 'dt_create', 'market', 'action', 'status', 'side', 'amount',
                    'get_cost', 'get_price', 'filled', 'dt_update', 'orderid')

    readonly_fields = ('clientid', 'account', 'market', 'status',  'action', 'type', 'amount', 'side', 'params',
                       'cost', 'filled', 'average', 'remaining', 'timestamp', 'max_qty', 'trades',
                       'last_trade_timestamp', 'price', 'fee', 'datetime',
                       'response', 'orderid', 'user', )
    actions = ['place_order', 'refresh', 'cancel_order']

    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        ('market', admin.RelatedOnlyFieldListFilter),
        'status',
        'type',
    )

    # Columns

    def get_cost(self, obj):
        if obj.cost:
            return round(obj.cost, 2)

    get_cost.short_description = 'Cost'

    def get_price(self, obj):
        if obj.price:
            return round(obj.price, 3)

    get_price.short_description = 'Price'

    # Actions

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
            return obj.market.contract_currency
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
