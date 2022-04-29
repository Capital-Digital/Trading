from prettyjson import PrettyJSONWidget
from django.contrib import admin
from trading.models import Account, Fund, Position, Order, Transfer, Asset
from trading.tasks import *
import structlog
from celery import chain, group
from django.contrib.admin import SimpleListFilter
from django.contrib.postgres.fields import JSONField
from prettyjson import PrettyJSONWidget
from django.db.models import Count, Sum, F, Q, JSONField
import json
from pygments import highlight, formatters, lexers
from django.utils.safestring import mark_safe
from django.db.models import Avg

log = structlog.get_logger(__name__)


@admin.register(Account)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'pseudonym', 'owner', 'exchange', 'quote', 'active', 'valid_credentials', )
    readonly_fields = ('valid_credentials', )
    actions = ['check_credentials', 'rebalance_account', 'market_sell_spot', 'market_close_positions', 'fetch_assets']
    save_as = True
    save_on_top = True

    # Actions

    def fetch_assets(self, request, queryset):
        for account in queryset:
            for wallet in account.exchange.get_wallets():
                fetch_assets.delay(account.id, wallet)

    fetch_assets.short_description = "Fetch assets"

    def market_sell_spot(self, request, queryset):
        for account in queryset:
            market_sell.delay(account.id)

    market_sell_spot.short_description = "Market sell"

    def market_close_positions(self, request, queryset):
        for account in queryset:
            market_close.delay(account.id)

    market_close_positions.short_description = "Market close"

    def rebalance_account(self, request, queryset):
        for account in queryset:
            rebalance.delay(account.id, reload=True, release=True)

    rebalance_account.short_description = "Rebalance"

    def check_credentials(self, request, queryset):
        for account in queryset:
            check_credentials.delay(account.id)

    check_credentials.short_description = "Check credentials"


@admin.register(Asset)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('get_owner', 'account', 'exchange', 'currency', 'wallet', 'get_total', 'get_free', 'get_used',
                    'dt_modified',
                    )
    readonly_fields = ('account', 'exchange', 'currency', 'wallet', 'total', 'free', 'used', 'dt_created',
                       'dt_modified', 'dt_response', )
    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        ('exchange', admin.RelatedOnlyFieldListFilter)
    )
    save_on_top = True

    # Columns

    def get_owner(self, obj):
        return obj.account.owner

    get_owner.short_description = 'Owner'

    def get_total(self, obj):
        return round(obj.total, 3)

    get_total.short_description = 'Total'

    def get_free(self, obj):
        return round(obj.free, 3)

    get_free.short_description = 'Free'

    def get_used(self, obj):
        return round(obj.used, 3)

    get_used.short_description = 'Reserved'


@admin.register(Fund)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('account', 'exchange')
    readonly_fields = ('account', 'historical_balance', 'dt_create', 'owner')
    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        ('exchange', admin.RelatedOnlyFieldListFilter)
    )
    save_on_top = True


@admin.register(Order)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('clientid', 'account', 'dt_modified', 'market', 'action', 'status', 'side', 'amount',
                    'get_cost', 'get_price', 'filled',)

    readonly_fields = ('clientid', 'account', 'market', 'status',  'action', 'type', 'amount', 'side',
                       'cost', 'filled', 'average', 'remaining', 'timestamp', 'max_qty', 'trades',
                       'last_trade_timestamp', 'price', 'fee', 'datetime', 'response', 'orderid', 'owner', 'sender')
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
            send_fetch_orderid.delay(order.account.id, order.orderid)

    refresh.short_description = 'Update order'

    def cancel_order(self, request, queryset):
        for order in queryset:
            send_cancel_order.delay(order.account.id, order.orderid)

    cancel_order.short_description = 'Cancel order'


@admin.register(Position)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('account', 'exchange', 'market', 'get_side', 'size', 'get_asset','get_notional_value',
                    'settlement',
                    'get_value_usd', 'get_initial_margin', 'leverage', 'get_contract_value',
                    'get_contract_value_curr', 'get_liquidation_price',
                    'last',
                    'entry_price', 'realized_pnl', 'unrealized_pnl', 'margin_mode',)
    readonly_fields = ('account', 'exchange', 'market', 'side', 'size', 'asset', 'value_usd', 'settlement',
                       'notional_value',
                       'entry_price', 'initial_margin', 'maint_margin', 'order_initial_margin',
                       'last', 'hedge', 'liquidation_price',
                       'realized_pnl', 'unrealized_pnl',
                       'margin_mode', 'leverage', 'instrument_id',
                       'response', 'max_qty')
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
                       'response', 'datetime', 'timestamp', 'owner')

    list_filter = (
        ('exchange', admin.RelatedOnlyFieldListFilter),
        ('account', admin.RelatedOnlyFieldListFilter),
        ('currency', admin.RelatedOnlyFieldListFilter)
    )
