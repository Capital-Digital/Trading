from prettyjson import PrettyJSONWidget
from django.contrib import admin
from trading.models import Account, Fund, Position, Order
from trading.tasks import *
import structlog
from django.contrib.admin import SimpleListFilter
from django.contrib.postgres.fields import JSONField
from prettyjson import PrettyJSONWidget
import json
from pygments import highlight, formatters, lexers
from django.utils.safestring import mark_safe

log = structlog.get_logger(__name__)


@admin.register(Account)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'exchange',  'trading', 'valid_credentials', 'strategy', 'type', 'derivative',
                    'margined', 'get_limit_price_tolerance', 'limit_order','updated_at',)
    readonly_fields = ('valid_credentials', 'position_mode')
    actions = ['set_credentials', 'get_position_mode', 'create_fund', 'refresh_orders', 'update_positions',
               'update_allocation', 'fetch_all_open_orders']
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

    def set_credentials(self, request, queryset):
        for account in queryset:
            account.set_credentials()

    set_credentials.short_description = "Set credentials"

    def get_position_mode(self, request, queryset):
        for account in queryset:
            account.get_position_mode()

    get_position_mode.short_description = "Get position mode"

    def refresh_orders(self, request, queryset):
        for account in queryset:
            account.refresh_orders()

    refresh_orders.short_description = "Refresh orders"

    def create_fund(self, request, queryset):
        for account in queryset:
            create_fund(account)

    create_fund.short_description = "Create fund"

    def update_positions(self, request, queryset):
        for account in queryset:
            account.update_positions()

    update_positions.short_description = "Update positions"

    def update_allocation(self, request, queryset):
        for account in queryset:
            account.update()

    update_allocation.short_description = "Update allocation"

    def fetch_all_open_orders(self, request, queryset):
        for account in queryset:
            orders_fetch_all_open(account.name)

    fetch_all_open_orders.short_description = "Fetch open orders"


@admin.register(Fund)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('dt', 'account', 'exchange', 'get_balance', )
    readonly_fields = ('account', 'exchange', 'balance', 'dt_create', 'dt', 'total', 'free', 'used', 'derivative')
    list_filter = (
        ('account', admin.RelatedOnlyFieldListFilter),
        ('exchange', admin.RelatedOnlyFieldListFilter)
    )

    def get_balance(self, obj):
        return f'{int(round(sum([v for k, v in obj.balance.items()]), 2)):n}'

    get_balance.short_description = 'Balance'


@admin.register(Order)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('orderId', 'account', 'market', 'status', 'side', 'cost', 'trades',
                    'type', 'price', 'price_strategy', 'filled', 'contract_val',
                    'dt_create', 'dt_update')

    readonly_fields = ('account', 'market', 'status', 'api', 'orderId', 'clientOrderId', 'type', 'amount', 'side',
                       'cost', 'remaining', 'timestamp', 'max_qty', 'trades',
                       'price', 'price_strategy', 'contract_val', 'fee', 'response', 'filled', 'average',
                       'api', 'last_trade_timestamp', 'leverage', 'datetime')
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
    list_display = ('account', 'exchange', 'market', 'get_side', 'last', 'liquidation_price', 'size',
                    'size_available', 'value_usd', 'sub_account_equity', 'margin_ratio',
                    'entry_price', 'margin', 'margin_maint_ratio', 'realized_pnl', 'unrealized_pnl', 'margin_mode',
                    'leverage', 'dt_update',)
    readonly_fields = ('account', 'exchange', 'market', 'side', 'size', 'value_usd', 'entry_price', 'last',
                       'liquidation_price',
                       'size_available',
                       'margin', 'margin_maint_ratio', 'realized_pnl', 'unrealized_pnl',
                       'margin_mode', 'leverage', 'dt_update', 'dt_create', 'instrument_id', 'created_at',
                       'sub_account_equity', 'margin_ratio', 'response', 'max_qty')
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
