from prettyjson import PrettyJSONWidget
from django.contrib import admin
from trading.models import Account, Fund, Position, Order, Asset, Stat
from trading.tasks import *
import structlog
from celery import chain, group
from django.contrib.admin import SimpleListFilter
from django.contrib.postgres.fields import JSONField
from prettyjson import PrettyJSONWidget
from django.db.models import Count, Sum, F, Q, JSONField, Avg
import json
from pygments import highlight, formatters, lexers
from django.utils.safestring import mark_safe

log = structlog.get_logger(__name__)


@admin.register(Account)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'pseudonym', 'owner', 'exchange', 'busy', 'quote', 'active', 'valid_credentials',)
    readonly_fields = ('valid_credentials',)
    actions = ['check_credentials', 'rebalance_account', 'fetch_assets',
               'fetch_positions', 'market_sell_spot', 'market_close_positions', 'transfer_to_spot']
    save_as = True
    save_on_top = True

    # Actions

    def fetch_assets(self, request, queryset):
        for account in queryset:
            for wallet in account.exchange.get_wallets():
                fetch_assets.delay(account.id, wallet)

    fetch_assets.short_description = "Fetch assets"

    def fetch_positions(self, request, queryset):
        for account in queryset:
            fetch_positions.delay(account.id)

    fetch_positions.short_description = "Fetch positions"

    def rebalance_account(self, request, queryset):
        for account in queryset:
            rebalance.delay(account.id, reload=True, release=True)

    rebalance_account.short_description = "Rebalance"

    def check_credentials(self, request, queryset):
        for account in queryset:
            check_credentials.delay(account.id)

    check_credentials.short_description = "Check credentials"

    def market_sell_spot(self, request, queryset):
        for account in queryset:
            market_sell.delay(account.id)

    market_sell_spot.short_description = "Market sell assets"

    def market_close_positions(self, request, queryset):
        for account in queryset:
            market_close.delay(account.id)

    market_close_positions.short_description = "Market close positions"

    def transfer_to_spot(self, request, queryset):
        for account in queryset:
            transfer_to_spot.delay(account.id)

    transfer_to_spot.short_description = "Transfer assets to spot"


@admin.register(Asset)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('get_owner', 'account', 'exchange', 'currency', 'wallet', 'get_total', 'get_free', 'get_used',
                    'total_value', 'get_weight', 'dt_modified',
                    )
    readonly_fields = ('account', 'exchange', 'currency', 'wallet', 'total', 'free', 'used', 'total_value', 'weight',
                       'dt_created',
                       'dt_modified', 'dt_response',)
    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        'wallet',
        ('exchange', admin.RelatedOnlyFieldListFilter)
    )
    save_on_top = True

    # Columns

    def get_owner(self, obj):
        return obj.account.owner

    get_owner.short_description = 'Owner'

    def get_total(self, obj):
        if obj.total:
            return round(obj.total, 3)

    get_total.short_description = 'Total'

    def get_free(self, obj):
        if obj.free:
            return round(obj.free, 3)

    get_free.short_description = 'Free'

    def get_used(self, obj):
        if obj.used:
            return round(obj.used, 3)

    get_used.short_description = 'Reserved'

    def get_weight(self, obj):
        if obj.weight:
            print(obj.weight)
            print(str(obj.weight * 100) + '%')
            return str(obj.weight * 100) + '%'

    get_weight.short_description = 'Weight'


@admin.register(Stat)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('account', 'exchange', 'strategy', 'order_execution_success_rate', 'order_execution_time_avg',
                    'trade_total_value',
                    'order_executed', 'positions_notional_value', 'assets_return', 'dt_modified')
    readonly_fields = ('account', 'order_execution_success_rate', 'order_execution_time_avg', 'trade_total_value',
                       'order_executed', 'account_value', 'positions_notional_value', 'assets_value_history',
                       'assets_return', 'dt_modified', 'dt_created')


@admin.register(Order)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('clientid', 'account', 'strategy', 'market', 'action', 'status', 'side', 'amount',
                    'get_cost', 'get_price', 'filled', 'dt_created', 'dt_modified', )

    readonly_fields = ('clientid', 'account', 'strategy', 'market', 'status', 'action', 'type', 'amount', 'side',
                       'cost', 'filled', 'average', 'remaining', 'timestamp', 'max_qty', 'trades',
                       'last_trade_timestamp', 'price', 'fee', 'datetime', 'response', 'orderid', 'owner', 'sender',
                       'dt_created', 'dt_modified', )
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
    list_display = ('account', 'exchange', 'market', 'get_side', 'size', 'get_notional_value',
                    'settlement', 'get_initial_margin', 'leverage', 'get_liquidation_price',
                    'last',
                    'entry_price', 'realized_pnl', 'unrealized_pnl', 'margin_mode',)
    readonly_fields = ('account', 'exchange', 'market', 'side', 'size', 'settlement',
                       'notional_value', 'entry_price', 'initial_margin', 'maint_margin', 'order_initial_margin',
                       'last', 'liquidation_price',
                       'realized_pnl', 'unrealized_pnl',
                       'margin_mode', 'leverage', 'instrument_id',
                       'response', 'max_qty', 'dt_modified', 'dt_created')
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


@admin.register(Fund)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('account', 'exchange')
    readonly_fields = ('account', 'historical_balance', 'dt_create', 'owner')
    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        ('exchange', admin.RelatedOnlyFieldListFilter)
    )
    save_on_top = True