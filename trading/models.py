import ccxt
from django.db import models
from django.conf import settings
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
    name = models.CharField(max_length=100, null=True, blank=False)
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
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True
    )

    class Meta:
        verbose_name_plural = "Accounts"

    def __str__(self):
        return self.name

    # Construct a dataframe with wallet balances and allocations
    # Wallets specify a list of wallet's funds need to be updated
    def create_df_account(self):

        log.info('Create account dataframe', account=self.name)

        funds = self.get_fund_latest()
        allocations = self.strategy.get_allocations()
        df_account = pd.DataFrame()

        # Loop through funds
        for i in ['total', 'free', 'used']:
            for default_type, dic1 in getattr(funds, i).items():

                for code, dic2 in dic1.items():
                    for field, value in dic2.items():
                        # Create multilevel columns
                        columns = pd.MultiIndex.from_product([['wallet'], [i + '_' + field]],
                                                             names=['level_1', 'level_2'])
                        indexes = pd.MultiIndex.from_tuples([(code, default_type)], names=['code', 'wallet'])

                        # Construct dataframe and normalize rows
                        df = pd.DataFrame(value, index=indexes, columns=columns)
                        df_account = pd.concat([df, df_account], axis=0).groupby(level=[0, 1]).mean()

        # Get a dataframe with positions
        positions = self.create_df_positions()

        if not positions.empty:

            # Sum quantity and dollar_value of positions with same code and wallet
            for index, position in positions.groupby(level=[0, 2]).sum().iterrows():
                # Add position size and position value
                df_account.loc[(index[0], index[1]), ('position', 'quantity')] = position['quantity']
                df_account.loc[(index[0], index[1]), ('position', 'value')] = position['total_value']

        else:
            # No position open
            for index, position in df_account.iterrows():
                df_account.loc[index, ('position', 'quantity')] = np.nan
                df_account.loc[index, ('position', 'value')] = np.nan
                df_account.loc[index, ('wallet', 'position_value')] = np.nan

        # Sum wallet and position to get total exposure per wallet en per coin
        d = df_account.loc[:, df_account.columns.get_level_values(0).isin({"position", "wallet"})]
        qty = d.loc[:, d.columns.get_level_values(1).isin({"quantity", "total_quantity"})].sum(axis=1)
        val = d.loc[:, d.columns.get_level_values(1).isin({"value", "total_value"})].sum(axis=1)

        # Create exposure columns
        df_account[('exposure', 'quantity')] = qty
        df_account[('exposure', 'value')] = val

        # Create a column for allocations weight
        for default_type, dic1 in funds.total.items():
            for k, v in allocations.items():
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
        total = df_account[('wallet', 'total_value')]
        df_account['target', 'value'] = df_account[('target', 'percent')] * total.sum() * float(self.leverage)

        log.info('Account value is {0}'.format(round(total.sum(), 2)))

        # Create a column with target quantity (quantity=value/price)
        df_account[('target', 'quantity')] = df_account[('target', 'value')] / df_account['price']
        df_account[('target', 'quantity')] = df_account[('target', 'quantity')].fillna(0)

        # Calculate delta
        for code, row in df_account.groupby(level=0):
            exposure = df_account.loc[code, ('exposure', 'quantity')].sum()  # sum exposure of a coin in all wallets
            delta = exposure - df_account[('target', 'quantity')]
            df_account.loc[code, ('delta', 'quantity')] = delta
            df_account.loc[code, ('delta', 'value')] = delta * df_account.loc[code, 'price'].mean()

        # Group rows by code and sum total exposure
        val_total = df_account.groupby(['code']).sum()['exposure']['value']
        for i, r in df_account.iterrows():
            df_account.loc[i, ('exposure', 'value_total')] = val_total[list(i)[0]]

        # Sort dataframe
        df_account.sort_index(axis=1, inplace=True)
        df_account.sort_index(axis=0, inplace=True)

        return df_account

    # Construct a dataframe is account open positions
    def create_df_positions(self):

        from trading import methods
        df = pd.DataFrame()
        positions = self.get_positions()
        cash = self.get_value_stablecoins()
        coins = self.get_coins()

        if positions:
            for position in positions:

                default_type = position.market.default_type
                type = position.market.type
                margined = position.market.margined.code
                symbol = position.market.symbol
                code = position.market.base.code
                size = float(position.size)
                quantity = methods.contract_to_amount(position.market, size)  # Convert position size to currency
                dollar_value = position.value_usd
                asset = position.asset.code if position.asset else 'Cont'
                side = position.side
                leverage = position.leverage
                settlement = position.settlement.code
                initial_margin = position.initial_margin
                pnl = position.unrealized_pnl
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
                                                                 'wallet',
                                                                 'symbol',
                                                                 'type',
                                                                 'derivative',
                                                                 'margined'])
                # Construct dataframe and normalize rows
                d = pd.DataFrame([[side, size, asset, quantity, settlement, dollar_value, initial_margin, leverage,
                                   pnl]],
                                 index=indexes,
                                 columns=['side', 'size', 'asset', 'quantity', 'settlement', 'total_value',
                                          'initial_margin', 'leverage', 'PnL']
                                 )

                # Check if position is potentially a hedge
                if d['side'][0] == 'sell':
                    if code in coins:
                        d['hedge'] = True
                    else:
                        d['hedge'] = False

                df = pd.concat([df, d], axis=0)

        # Iterate through coins in account and positions
        for coin in coins:
            balance_value = self.get_value_coin(coin)
            for index, row in df.loc[:, :].iterrows():
                if index[0] == coin:

                    df.sort_index(axis=0, inplace=True)
                    df.loc[index, 'balance_value'] = balance_value

                    # Determine synthetic cash of every short position
                    if row['hedge']:

                        value = abs(row['total_value'])
                        synthetic_cash = min(value, balance_value)
                        df.loc[index, 'synthetic_cash'] = synthetic_cash
                        balance_value -= synthetic_cash

                        # Determine USD value of initial margin
                        if index[6] in self.exchange.get_stablecoins():
                            position = Position.objects.get(account=self,
                                                            market__symbol=index[3],
                                                            market__default_type=index[2]
                                                            )
                            df.loc[index, 'hedge_margin_value'] = synthetic_cash / position.leverage

        return df

    # Create a new order object
    def create_order(self, market, instruction, side, amount, price, params=None):

        if self.limit_order:
            order_type = 'limit'
        else:
            order_type = 'market'
            price = None

        defaults = dict(
            account=self,
            action=instruction,
            market=market,
            price=price,
            type=order_type,
            side=side,
            amount=str(amount),
            status='created'
        )

        # Set parameters
        if params:
            defaults['params'] = params

        order = Order.objects.create(**defaults)

        if order:
            log.info('Object created with id {0}'.format(order.id))
            return order.id

        else:
            pprint(defaults)
            raise Exception('Error while creating order object')

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

    # Return coins
    def get_coins(self):

        funds = self.get_fund_latest()
        coins = []

        for wallet in funds.total.keys():
            coins.extend(list(funds.total[wallet].keys()))

        return coins

    # Return value of stablecoins
    def get_value_stablecoins(self):

        funds = self.get_fund_latest()
        stablecoins = [c.code for c in Currency.objects.filter(exchange=self.exchange, stable_coin=True)]
        cash = []

        for wallet, coins in funds.total.items():
            for code, values in coins.items():
                if code in stablecoins:
                    cash.append(values['value'])

        return sum(cash)

    # Return coin value
    def get_value_coin(self, code):

        funds = self.get_fund_latest()
        values = []

        for wallet, coins in funds.total.items():
            for key, value in coins.items():
                if key == code:
                    values.append(value['value'])

        return sum(values)

    # Return a list with orderid of open orders
    def get_pending_order_ids(self):

        from trading import tasks
        orders = Order.objects.filter(account=self, status='open')

        if orders.exists():
            return [order.orderid for order in orders]

    # Return latest funds object
    def get_fund_latest(self):

        if self.is_fund_updated():
            return self.funds.latest('dt')

        else:
            from trading.tasks import create_fund
            create_fund(self.id)
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

    # Return True if fund object is stamped at 0
    def is_fund_updated(self):
        dt = timezone.now().replace(minute=0, second=0, microsecond=0)
        if self.funds.latest('dt_create').dt == dt:
            return True
        else:
            return False


class Fund(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='funds', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='funds', null=True)
    dt = models.DateTimeField(null=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)
    balance, total, free, used, margin_assets = [JSONField(null=True) for i in range(5)]
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True
    )

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
    amount, filled, remaining, max_qty = [models.FloatField(max_length=10, null=True) for i in range(4)]
    side = models.CharField(max_length=10, null=True, choices=(('buy', 'buy'), ('sell', 'sell')))
    cost = models.FloatField(null=True)
    route_type = models.CharField(max_length=10, null=True)
    action = models.CharField(max_length=20, null=True)
    average, price, price_strategy = [models.FloatField(null=True, blank=True) for i in range(3)]
    fee, trades, params, response = [JSONField(null=True) for i in range(4)]
    datetime, last_trade_timestamp = [models.DateTimeField(null=True) for i in range(2)]
    timestamp = models.BigIntegerField(null=True)
    dt_update = models.DateTimeField(auto_now=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True
    )

    class Meta:
        verbose_name_plural = "Orders"

    def __str__(self):
        return str(self.pk)


class Position(models.Model):
    objects = models.Manager()
    market = models.ForeignKey(Market, on_delete=models.SET_NULL, related_name='positions', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='positions', null=True)
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='positions', null=True)
    settlement = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='positions', null=True)
    last, liquidation_price = [models.FloatField(null=True) for i in range(2)]
    size = models.CharField(max_length=100, null=True)
    asset = models.ForeignKey(Currency, on_delete=models.SET_NULL, related_name='position_asset', null=True)
    hedge = models.FloatField(null=True)
    entry_price = models.FloatField(null=True)
    max_qty = models.FloatField(null=True)
    notional_value, initial_margin, maint_margin, order_initial_margin = [models.FloatField(null=True) for i in
                                                                          range(4)]
    realized_pnl, unrealized_pnl, value_usd = [models.FloatField(null=True) for i in range(3)]
    instrument_id, side = [models.CharField(max_length=150, null=True) for i in range(2)]
    margin_mode = models.CharField(max_length=10, null=True, choices=(('isolated', 'isolated'),
                                                                      ('crossed', 'crossed')))
    # leverage = models.DecimalField(null=True, max_digits=5, decimal_places=2)
    leverage = models.IntegerField(null=True)
    created_at = models.DateTimeField(null=True)
    response = JSONField(null=True)
    dt_update = models.DateTimeField(auto_now=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True
    )

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
