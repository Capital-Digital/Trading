import ccxt
from django.db import models
from django.contrib.postgres.fields import JSONField
from django.core.validators import MaxValueValidator, MinValueValidator
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
    valid_credentials = models.BooleanField(null=True, default=None)
    trading = models.BooleanField(null=True, blank=False, default=False)
    limit_order = models.BooleanField(null=True, blank=False, default=True)
    limit_price_tolerance = models.DecimalField(default=0, max_digits=4, decimal_places=3)
    position_mode = models.CharField(max_length=20, choices=[('dual', 'dual'), ('hedge', 'hedge')], blank=True)
    email = models.EmailField(max_length=100, blank=True)
    api_key, api_secret = [models.CharField(max_length=100, blank=True) for i in range(2)]
    password = models.CharField(max_length=100, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    leverage = models.DecimalField(
        default=1,
        max_digits=2, decimal_places=1,
        validators=[
            MaxValueValidator(2),
            MinValueValidator(0)
        ]
    )

    class Meta:
        verbose_name_plural = "Accounts"

    def __str__(self):
        return self.name

    # Construct a dataframe with account balance and instructions
    def create_dataframe_funds(self):

        funds = self.get_fund_latest()
        instructions = self.strategy.get_instructions()
        df_account = pd.DataFrame()

        # Loop through funds
        for default_type, dic1 in funds.total.items():

            for code, dic2 in dic1.items():
                for field, value in dic2.items():
                    # Create multilevel columns
                    columns = pd.MultiIndex.from_product([['account'], [field]])
                    indexes = pd.MultiIndex.from_tuples([(code, default_type)], names=['code', 'type'])

                    # Construct dataframe and normalize rows
                    df = pd.DataFrame(value, index=indexes, columns=columns)
                    df_account = pd.concat([df, df_account], axis=0).groupby(level=[0, 1]).mean()

        # Get a dataframe with positions
        positions = self.create_dataframe_positions()

        # If multiple position on a same code raise exception
        if len(list(positions.index.get_level_values(0))) > 2:
            raise Exception('You should normalize size of positions before creating a dataframe')

        # Sum quantity and value of positions with the same code and default_type
        for index, position in positions.groupby(level=[0, 2]).sum().iterrows():
            # Add position size and position value
            df_account.loc[(index[0], index[1]), ('position', 'quantity')] = position['size']
            df_account.loc[(index[0], index[1]), ('position', 'value')] = position['value']

        # Create exposure columns (exposure=account+positions)
        df_account[('exposure', 'quantity')] = df_account.xs('quantity', axis=1, level=1).sum(axis=1)
        df_account[('exposure', 'value')] = df_account.xs('value', axis=1, level=1).sum(axis=1)

        # Create a column for instruction weight
        for default_type, dic1 in funds.total.items():
            for k, v in instructions.items():
                inst_codes = list(v.keys())
                for code in inst_codes:
                    df_account.loc[(code, default_type), ('target', 'percent')] = v[code]['weight']

                    # Select latest price
                    if code != self.exchange.dollar_currency:
                        price = Market.objects.get(base__code=code,
                                                   quote__code=self.exchange.dollar_currency,
                                                   exchange=self.exchange,
                                                   default_type__in=['spot', None]
                                                   ).get_candle_price_last()
                    else:
                        price = 1

                    df_account.loc[(code, default_type), 'price'] = price

        # Create a column with target value
        account_value = df_account[('account', 'value')].sum()
        df_account['target', 'value'] = df_account[('target', 'percent')] * account_value * float(self.leverage)

        # Create a column with target quantity (quantity=value/price)
        df_account[('target', 'quantity')] = df_account[('target', 'value')] / df_account['price']

        # Finally calculate delta
        for code, row in df_account.groupby(level=0):
            exposure = df_account.loc[code, ('exposure', 'quantity')].sum()  # sum all default_type qty of the same coin
            delta = exposure - df_account[('target', 'quantity')]
            df_account.loc[code, ('target', 'delta')] = delta
            df_account.loc[code, ('target', 'delta_usd')] = delta * df_account.loc[code, 'price'].mean()

        # Sort dataframe
        df_account.sort_index(axis=1, inplace=True)
        df_account.sort_index(axis=0, inplace=True)

        return df_account

    # Construct a dataframe is account open positions
    def create_dataframe_positions(self):

        df = pd.DataFrame()
        positions = self.get_positions()

        for position in positions:

            default_type = position.market.default_type
            type = position.market.type
            margined = position.market.margined.code
            symbol = position.market.symbol
            code = position.market.base.code
            size = float(position.size) if position.side == 'buy' else -float(position.size)
            value = float(position.value_usd) if position.side == 'buy' else -float(position.value_usd)
            # Create multilevel columns
            indexes = pd.MultiIndex.from_tuples([(code,
                                                  position.market.quote.code,
                                                  default_type,
                                                  symbol,
                                                  type,
                                                  position.market.derivative,
                                                  margined
                                                  )], names=['code',
                                                             'quote',
                                                             'default_type',
                                                             'symbol',
                                                             'type',
                                                             'derivative',
                                                             'margined'])
            # Construct dataframe and normalize rows
            d = pd.DataFrame([[position.side, size, value]], index=indexes, columns=['side', 'size', 'value'])
            df = pd.concat([df, d], axis=0)

        return df

    # Create a new order object
    def create_order(self, market, action, amount):

        from trading import methods
        print('amount before', amount)
        # Check amount limits min & max conditions
        amount = methods.limit_amount(market, amount)

        print('amount after', amount)
        if not amount:
            return

        # Format decimal
        amount = methods.format_decimal(counting_mode=self.exchange.precision_mode,
                                        precision=market.precision['amount'],
                                        n=amount
                                        )

        if action in ['open_long', 'close_short', 'buy']:
            side = 'buy'
        elif action in ['open_short', 'close_long', 'sell']:
            side = 'sell'

        defaults = dict(
            account=self,
            action=action,
            market=market,
            type='limit' if self.limit_order else 'market',
            side=side,
            amount=str(amount),
            status='created'
        )

        # Limit price order
        if self.limit_order:
            if self.exchange.has['createLimitOrder']:

                price = market.get_candle_price_last()

                # Add or remove tolerance
                if side == 'buy':
                    price = price + price * float(self.limit_price_tolerance)
                elif side == 'sell':
                    price = price - price * float(self.limit_price_tolerance)

                # Check price limits and format decimal
                if methods.limit_price(market, price):
                    price = methods.format_decimal(counting_mode=self.exchange.precision_mode,
                                                   precision=market.precision['price'],
                                                   n=price
                                                   )
                    defaults['price'] = str(price)
                else:
                    raise Exception('Price limits conditions error')
            else:
                raise Exception('Limit order not supported')

        # Market order
        else:
            if self.exchange.has['createMarketOrder']:

                # Set price to check cost limit conditions
                price = market.get_candle_price_last()

            else:
                raise Exception('Market order not supported')

        # Finally check cost and create object
        if methods.limit_cost(market, float(amount), float(price)):

            order = Order.objects.create(**defaults)

            print('Create order object:')
            pprint(defaults)

            return order.id

        else:
            pass

    # Check crendentials and update field
    def update_credentials(self):

        try:
            client = self.exchange.get_ccxt_client(self)
            client.checkRequiredCredentials()

        except ccxt.AuthenticationError as e:
            print('NOK')
            self.valid_credentials = False

        except Exception as e:
            print('NOK')
            self.valid_credentials = False

        else:
            print('OK')
            self.valid_credentials = True

        finally:
            self.save()

    # Return a list with orderid of open orders
    def get_pending_order_ids(self):

        from trading import tasks
        orders = Order.objects.filter(account=self, status='open')

        if orders.exists():
            return [order.orderid for order in orders.all()]

    # Return latest funds object
    def get_fund_latest(self):

        if self.is_fund_updated():
            return self.funds.latest('dt')

        else:

            from trading.tasks import create_fund
            create_fund(self.name)
            return self.funds.latest('dt')

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
    orderid = models.CharField(max_length=150, null=True)  # order exchange's ID
    status, type = [models.CharField(max_length=150, null=True) for i in range(2)]
    amount, filled, remaining, max_qty = [models.CharField(max_length=10, null=True) for i in range(4)]
    side = models.CharField(max_length=10, null=True, choices=(('buy', 'buy'), ('sell', 'sell')))
    cost = models.FloatField(null=True)
    trades = models.CharField(max_length=10, null=True)
    action = models.CharField(max_length=20, null=True)
    average, price, price_strategy = [models.FloatField(null=True, blank=True) for i in range(3)]
    fee = JSONField(null=True)
    response = JSONField(null=True)
    datetime, last_trade_timestamp = [models.DateTimeField(null=True) for i in range(2)]
    timestamp = models.BigIntegerField(null=True)
    dt_update = models.DateTimeField(auto_now=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        verbose_name_plural = "Orders"

    def __str__(self):
        return str(self.pk)


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
