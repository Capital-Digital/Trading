import ccxt
from django.db import models
from django.contrib.postgres.fields import JSONField
from django.utils import timezone
from django.db.models import Q
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from capital.methods import *
from strategy.models import Strategy, Allocation
from marketsdata.models import Exchange, Market, Currency
from trading.error import *
import structlog
from datetime import timedelta, datetime
from pprint import pprint
from decimal import Decimal
import numpy as np
import pandas as pd

log = structlog.get_logger(__name__)

datetime_directives_std = '%Y-%m-%dT%H:%M:%S.%fZ'


class Account(models.Model):
    objects = models.Manager()
    name = models.CharField(max_length=100, null=True, blank=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='account', blank=True, null=True)
    strategy = models.ForeignKey(Strategy, on_delete=models.SET_NULL, related_name='account', null=True)
    type = models.CharField(max_length=20, choices=[('spot', 'spot'), ('derivative', 'derivative')], blank=True)
    derivative = models.CharField(max_length=20, choices=[('perpetual', 'perpetual'), ('delivery', 'delivery')],
                                  blank=True)
    margined = models.ForeignKey(Currency, on_delete=models.DO_NOTHING, related_name='account_margined',
                                 blank=True, null=True)
    limit_order, valid_credentials, trading = [models.BooleanField(null=True, default=None) for i in range(3)]
    limit_price_tolerance = models.DecimalField(default=0, max_digits=4, decimal_places=3)
    position_mode = models.CharField(max_length=20, choices=[('dual', 'dual'), ('hedge', 'hedge')], blank=True)
    email = models.EmailField(max_length=100, blank=True)
    api_key, api_secret = [models.CharField(max_length=100, blank=True) for i in range(2)]
    password = models.CharField(max_length=100, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)

    class Meta:
        verbose_name_plural = "Accounts"

    def __str__(self):
        return self.name

    # Check crendentials and update field
    def update_credentials(self):

        try:
            client = self.exchange.get_ccxt_client(self)
            client.checkRequiredCredentials()

        except ccxt.AuthenticationError as e:
            self.valid_credentials = False

        except Exception as e:
            self.valid_credentials = False

        else:
            self.valid_credentials = True

        finally:
            self.save()

    # Return latest funds object
    def get_fund_latest(self):

        if self.is_fund_updated():
            return self.funds.latest('dt')

        else:
            log.warning('Fund object is not updated')
            return self.funds.latest('dt_create')
            # raise Exception('Fund object is not updated')

    # Construct a table
    def get_table(self):

        funds = self.get_fund_latest()
        instructions = self.strategy.get_instructions()
        d = pd.DataFrame()

        for default_type, dic1 in funds.total.items():
            for code, dic2 in dic1.items():

                # Loop through fund
                for field, value in dic2.items():
                    # Create multilevel columns
                    columns = pd.MultiIndex.from_product([[default_type], [field]], names=['type', 'indicator'])
                    indexes = pd.MultiIndex.from_tuples([(code, default_type)], names=['code', 'type'])

                    # Construct dataframe and normalize rows
                    df = pd.DataFrame(value, index=indexes, columns=columns)
                    d = pd.concat([df, d], axis=0).groupby(level=[0, 1]).mean()

            # Loop through instructions
            for k, v in instructions.items():
                for code in list(v.keys()):
                    # Create a new row AND/OR a column with instruction weight
                    d.loc[code, ('account', 'target %')] = v[code]['weight']

            # Create a column with target quantity
            total_usd = d.xs('value', axis=1, level=1, drop_level=False).sum().sum()
            d[('account', 'target $')] = d[('account', 'target %')] * total_usd

        # Sort dataframe
        d.sort_index(axis=1, inplace=True)
        d.sort_index(axis=0, inplace=True)

        return d

    # Return positions
    def get_positions(self):

        positions = self.positions.all()
        if positions.exists():
            lst = []
            for p in positions:
                lst.append(p)
            return lst
        else:
            return []

    # Return long positions
    def get_positions_long(self):

        positions = self.positions.all().filter(side='buy')
        if positions.exists():
            lst = []
            for p in positions:
                lst.append(p)
            return lst
        else:
            return []

    # Return short positions
    def get_positions_short(self):

        positions = self.positions.all().filter(side='sell')
        if positions.exists():
            lst = []
            for p in positions:
                lst.append(p)
            return lst
        else:
            return []

    # return datetime of latest order
    def get_order_latest_dt(self, market):

        orders = Order.objects.filter(account=self, market=market)
        if orders.exists():
            return orders.latest('dt_create').dt

    # Select currencies that need to be traded without margin
    def bases_alloc_no_margin(self):

        dt = self.strategy.get_latest_alloc_dt()
        allocations = Allocation.objects.filter(strategy=self.strategy, dt=dt)
        if allocations.exists():
            lst = []
            for a in allocations:
                if not a.margin or (a.margin and a.side == 'buy'):
                    lst.append(a)
            return lst
        else:
            return []

    # Select bases (margin short) from the new allocations
    def get_allocations_short(self):

        dt = self.strategy.get_latest_alloc_dt()
        allocations = Allocation.objects.filter(strategy=self.strategy, dt=dt)
        if allocations.exists():
            lst = []
            for a in allocations:
                if a.margin and a.side == 'sell':
                    lst.append(a)
            return lst

    # Transfer funds between accounts
    def transfer_fund(self):

        if self.exchange.exid == 'okex':
            from . import okex
            okex.transfer_funds(self)

    # Return True if fund object is stamped at 0
    def is_fund_updated(self):
        if self.funds.latest('dt_create').dt == get_datetime(minute=0):
            return True
        else:
            return False

    # Return True if position has been updated recently
    def is_positions_updated(self):

        return True if all([position.is_updated() for position in self.position.all()]) else False

    # Return True if account exchange is compatible with strategy
    def is_compatible(self):

        # Fire an exception if one currency from the strategy isn't available on exchange
        def check(strategy):

            # set type to future/swap if strategy.margin else all types
            bases, quote, margin = strategy.get_markets(flat=True)
            tp = ['swap', 'future', 'futures'] if margin else ['swap', 'spot', 'future', 'futures', None]

            # try to select all bases from the strategy
            for base in bases:
                try:
                    Market.objects.get(exchange=self.exchange, type__in=tp, base__code=base)
                except ObjectDoesNotExist:
                    mk = 'future market' if margin else 'spot market'
                    raise SettingError('{1} {0} is not available on {2}'.format(mk,
                                                                                        base,
                                                                                        self.exchange.exid))
                except MultipleObjectsReturned:
                    pass

        if self.strategy.master:
            for strategy in self.strategy.sub_strategies.all():
                check(strategy)
        else:
            check(self.strategy)
        return True

    # Create an order object (to open, add, remove or close a position)
    def order_create(self, market, market_type, type, side, size):

        from trading.methods import format_decimal
        market = Market.objects.get(symbol=market, type=market_type, exchange=self.exchange)

        # Format decimal
        size = format_decimal(size, market.precision['amount'], self)

        defaults = dict(
            account=self,
            api=True,
            market=market,
            size=str(size),
            status='created',
            price_strategy=market.get_last_price(),
            type=type
        )

        # Set price
        if type == 'limit':
            price = market.get_last_price()
            price = format_decimal(price, market.precision['price'], self)
            defaults['price'] = str(price)

        log.info('Create order object')
        pprint(defaults)
        Order.objects.create(**defaults)


class Fund(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='funds', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='funds', null=True)
    dt = models.DateTimeField(null=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)
    balance, total, free, used, derivative = [JSONField(null=True) for i in range(5)]

    class Meta:
        verbose_name_plural = "Funds"
        ordering = ['-dt_create']
        get_latest_by = 'dt_create'

    def __str__(self):
        return str(self.id)


class Order(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='order', null=True)
    market = models.ForeignKey(Market, on_delete=models.SET_NULL, related_name='order', null=True)
    orderId, clientOrderId, status, type = [models.CharField(max_length=150, null=True) for i in range(4)]
    amount, filled, remaining, max_qty = [models.CharField(max_length=10, null=True) for i in range(4)]
    side = models.CharField(max_length=10, null=True, choices=(('buy', 'buy'), ('sell', 'sell')))
    cost = models.FloatField(null=True)
    trades = models.CharField(max_length=10, null=True)
    average, price, price_strategy, leverage = [models.FloatField(null=True) for i in range(4)]
    fee = JSONField(null=True)
    response = JSONField(null=True)
    datetime, last_trade_timestamp = [models.DateTimeField(null=True) for i in range(2)]
    timestamp = models.BigIntegerField(null=True)
    dt_update = models.DateTimeField(auto_now=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        verbose_name_plural = "Orders"

    def __str__(self):
        return self.type


class Position(models.Model):
    objects = models.Manager()
    market = models.ForeignKey(Market, on_delete=models.SET_NULL, related_name='positions', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='positions', null=True)
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='positions', null=True)
    last, liquidation_price = [models.FloatField(null=True) for i in range(2)]
    size = models.CharField(max_length=100, null=True)
    entry_price = models.FloatField(null=True)
    max_qty = models.FloatField(null=True)
    margin, margin_maint_ratio, margin_ratio = [models.FloatField(null=True) for i in range(3)]
    realized_pnl, unrealized_pnl, value_usd = [models.FloatField(null=True) for i in range(3)]
    instrument_id, side = [models.CharField(max_length=150, null=True) for i in range(2)]
    margin_mode = models.CharField(max_length=10, null=True, choices=(('isolated', 'isolated'),
                                                                      ('crossed', 'crossed')))
    leverage = models.DecimalField(null=True, max_digits=5, decimal_places=2)
    leverage_max = models.DecimalField(null=True, max_digits=5, decimal_places=2)
    created_at = models.DateTimeField(null=True)
    response = JSONField(null=True)
    dt_update = models.DateTimeField(auto_now=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        verbose_name_plural = "Positions"

    def __str__(self):
        return self.exchange.exid

    # Return margin ratio
    def get_margin_ratio(self):
        if self.is_updated():
            if self.account.is_fund_updated():
                fund = self.account.fund.latest('dt_create')
                return (self.margin * float(self.leverage)) / fund.equity
            else:
                log.error('Cannot calculate margin ratio, fund is not updated')
        else:
            log.error('Cannot calculate margin ratio, position is not updated')

    # Return True if a position has been updated recently
    def is_updated(self):
        return True if (timezone.now() - self.dt_update).seconds < 60 * 5 else False

    # Create an order to create a new position
    def close(self):
        log.bind(account=self.account.name)

        log.info('Create position')

        type_order = 'open_long' if self.side == 'long' else 'open_short'
        self.account.create_order(self.market, self.size, type_order)

    # Create an order to add contracts to a position
    def add(self, size):
        log.bind(account=self.account.name)

        log.info('Add contracts to position')

        type_order = 'open_long' if self.side == 'long' else 'open_short'
        self.account.create_order(self.market, size, type_order)

    # Create an order to remove contracts to a position
    def remove(self, size):
        log.bind(account=self.account.name)

        log.info('Remove contracts to position')

        type_order = 'close_long' if self.side == 'long' else 'close_short'
        self.account.create_order(self.market, size, type_order)

    # Create an order to close an open position
    def close(self):
        log.bind(account=self.account.name)

        log.info('Close position')

        type_order = 'close_long' if self.side == 'long' else 'close_short'
        self.account.create_order(self.market, self.size, type_order)
