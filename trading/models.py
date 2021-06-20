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

import json

log = structlog.get_logger(__name__)

datetime_directives_std = '%Y-%m-%dT%H:%M:%S.%fZ'


class Account(models.Model):
    objects = models.Manager()
    name = models.CharField(max_length=100, null=True, blank=False)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='account', blank=True, null=True)
    strategy = models.ForeignKey(Strategy, on_delete=models.SET_NULL, related_name='account', null=True)
    valid_credentials = models.BooleanField(null=True, default=None)
    trading = models.BooleanField(null=True, blank=False, default=False)
    updated = models.BooleanField(null=True, blank=False)
    limit_order = models.BooleanField(null=True, blank=False, default=True)
    limit_price_tolerance = models.DecimalField(default=0, max_digits=4, decimal_places=3)
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
    def create_df_account(self, prices):

        # Get an asset price
        def get_price_candle(code):

            # Select latest price
            if code not in self.exchange.get_stablecoins():
                return Market.objects.get(base__code=code,
                                          quote__code=self.exchange.dollar_currency,
                                          exchange=self.exchange,
                                          default_type__in=['spot', None]
                                          ).get_candle_price_last()
            else:
                return 1

        # Get an asset price from WS streams
        def get_price_ws(code):
            if code not in self.exchange.get_stablecoins():
                return float(prices['spot'][code]['ask'])
            else:
                return 1

        log.info('Create dataframe account')

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

                # Select margin_assets for the wallet and add max_withdrawal
                margin_assets = funds.margin_assets[default_type]
                if margin_assets:
                    for asset in margin_assets:
                        coin = asset['asset']
                        withdrawal = float(asset['maxWithdrawAmount'])
                        df_account.sort_index(axis=0, inplace=True)
                        df_account.loc[(coin, default_type), ('withdrawal', 'max_quantity')] = withdrawal
                        df_account.loc[(coin, default_type), ('withdrawal', 'max_value')] = withdrawal * get_price_candle(coin)

        # Get a dataframe with positions
        positions = self.create_df_positions()

        if not positions.empty:

            # Sum quantity and dollar_value of positions with same code and wallet
            for index, position in positions.groupby(level=[0, 2]).sum().iterrows():
                # Add position size and position value
                df_account.loc[(index[0], index[1]), ('position', 'quantity')] = position['quantity']
                df_account.loc[(index[0], index[1]), ('position', 'value')] = position['value']

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

                    # Select latest and hourly prices
                    price = get_price_candle(code)
                    price_ws = get_price_ws(code)

                    df_account.loc[(code, default_type), ('price', 'hourly')] = price
                    df_account.loc[(code, default_type), ('price', 'ask')] = price_ws

        # Create a column with target value
        total = df_account[('wallet', 'total_value')]
        df_account['target', 'value'] = df_account[('target', 'percent')] * total.sum() * float(self.leverage)

        # Create a column with target quantity (quantity=value/price)
        df_account[('target', 'quantity')] = df_account[('target', 'value')] / df_account[('price', 'ask')]
        df_account[('target', 'quantity')] = df_account[('target', 'quantity')].fillna(0)

        # Calculate delta
        for code, row in df_account.groupby(level=0):
            exposure = df_account.loc[code, ('exposure', 'quantity')].sum()  # sum exposure of a coin in all wallets
            delta = exposure - df_account[('target', 'quantity')]
            df_account.loc[code, ('delta', 'quantity')] = delta
            df_account.loc[code, ('delta', 'value')] = delta * df_account.loc[code, ('price', 'ask')].mean()

        # Group rows by code and sum total exposure
        val_total = df_account.groupby(['code']).sum()['exposure']['value']
        for i, r in df_account.iterrows():
            df_account.loc[i, ('exposure', 'value_total')] = val_total[list(i)[0]]

        # Sort dataframe
        df_account.sort_index(axis=1, inplace=True)
        df_account.sort_index(axis=0, inplace=True)

        log.info('Create dataframe account OK', value=round(total.sum(), 2))

        return df_account

    # Construct a dataframe is account open positions
    def create_df_positions(self):

        log.info('Create dataframe positions')

        from trading import methods
        df = pd.DataFrame()
        positions = self.positions.all()
        codes = self.get_codes()
        hedge_total = self.get_hedge_total()

        if positions:
            for position in positions:
                default_type = position.market.default_type
                type = position.market.type
                margined = position.market.margined.code
                symbol = position.market.symbol
                code = position.market.base.code
                size = float(position.size)
                quantity = methods.contract_to_amount(position.market, size)  # Convert position size to currency
                value = position.value_usd
                asset = position.asset.code if position.asset else 'Cont'
                side = position.side
                leverage = position.leverage
                settlement = position.settlement.code
                # initial_margin = position.initial_margin
                pnl = position.unrealized_pnl

                # Calculate initial_margin as value/leverage
                # to prevent margin > value in some cases
                initial_margin = value / leverage

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
                d = pd.DataFrame([[side, size, asset, quantity, settlement, value, initial_margin, leverage,
                                   pnl]],
                                 index=indexes,
                                 columns=['side', 'size', 'asset', 'quantity', 'settlement', 'value',
                                          'initial_margin_value', 'leverage', 'PnL']
                                 )

                df = pd.concat([df, d], axis=0)

        # Iterate through coins in account and positions
        for code in codes:
            balance = self.get_balance(code)
            hedge = self.get_hedge(code)

            for index, row in df.loc[:, :].iterrows():
                if code == index[0]:

                    df.sort_index(axis=0, inplace=True)

                    if row['side'] == 'sell':

                        # df.loc[index, 'hedge_ratio'] = hedge_ratio
                        df.loc[index, 'hedge_code'] = hedge
                        df.loc[index, 'hedge_total'] = hedge_total
                        df.loc[index, 'balance'] = balance

                        # If position is USD margined determine margin allocated to hedge
                        if index[6] in self.exchange.get_stablecoins():

                            # Position value
                            short = abs(df.loc[index, 'value'])

                            # Total shorted value
                            shorts = self.get_shorts(code)

                            # Determine ratio of hedge among shorts
                            ratio = hedge / shorts

                            # Determine value of a position allocated to hedge
                            hedge_position = short * ratio

                            # Determine margin allocated to hedge for that position
                            margin = hedge_position / df.loc[index, 'leverage']

                            df.loc[index, 'hedge_margin'] = margin
                            df.loc[index, 'hedge_position'] = hedge_position

        log.info('Create dataframe positions OK')

        return df

    # Return a list of codes in all wallets
    def get_codes(self):

        funds = self.get_fund_latest()
        codes = []

        for wallet in funds.total.keys():
            codes.extend(list(funds.total[wallet].keys()))

        return list(set(codes))

    # Return a list of stablecoins in all wallets
    def get_codes_stable(self):
        return [code for code in self.get_codes() if Currency.objects.get(code=code).stable_coin]

    # Return a list of codes with opened positions
    def get_positions_codes(self):
        return list(set(position.market.base.code for position in self.positions.all()))

    # Return absolute value of a code or a list of codes
    def get_balance(self, code):

        funds = self.get_fund_latest()
        values = []

        for wallet, coins in funds.total.items():
            for key, value in coins.items():
                if isinstance(code, list):
                    if key in code:
                        values.append(value['value'])
                else:
                    if key == code:
                        values.append(value['value'])

        return sum(values)

    # Return absolute value of short positions opened for a code
    def get_shorts(self, code):

        positions = self.positions.all()
        shorts = []

        for position in positions:
            if position.side == 'sell':
                if position.market.base.code == code:
                    shorts.append(abs(position.value_usd))

        return sum(shorts)

    # Return hedge for a specific code
    def get_hedge(self, code):

        balance = self.get_balance(code)
        shorts = self.get_shorts(code)
        return min(balance, shorts)

    # Return hedge ratio for a specific code
    def get_hedge_ratio(self, code):

        balance = self.get_balance(code)
        shorts = self.get_shorts(code)
        return shorts / balance

    # Return hedge for all currencies
    def get_hedge_total(self):

        total = []
        for code in self.get_positions_codes():
            total.append(self.get_hedge(code))

        return sum(total)

    # Return value above which a short position isn't a hedge but a short
    def get_hedge_threshold(self, code):
        return self.get_balance(code) - self.get_hedge(code)

    # Create a new order object
    def create_order(self, route, segment):

        market = Market.objects.get(exchange=self.exchange,
                                    symbol=segment.market.symbol,
                                    default_type=segment.market.wallet
                                    )

        if self.limit_order:
            order_type = 'limit'
            price = segment.trade.price
        else:
            order_type = 'market'
            price = None

        defaults = dict(
            account=self,
            action=segment.type.action,
            market=market,
            price=price,
            type=order_type,
            side=segment.trade.side,
            amount=segment.trade.amount,
            status='created',
            segments=route.to_json()
        )

        # Set parameters
        if not pd.isna(segment.trade.params):
            defaults['params'] = eval(segment.trade.params)

        order = Order.objects.create(**defaults)

        if order:
            return order.id

        else:
            pprint(defaults)
            raise Exception('Error while creating order object')

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

    # Return True if fund object is stamped at 0
    def is_fund_updated(self):
        dt = timezone.now().replace(minute=0, second=0, microsecond=0)
        if self.funds.latest('dt_create').dt == dt:
            return True
        else:
            return False

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


class Fund(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='funds', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='funds', null=True)
    dt = models.DateTimeField(null=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)
    balance, total, free, used, margin_assets, positions = [JSONField(null=True) for i in range(6)]
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
    fee, trades, params, response, route, segments = [JSONField(null=True) for i in range(6)]
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
    response_2 = JSONField(null=True)
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


class Transfer(models.Model):
    objects = models.Manager()
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='transfer', null=True)
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='transfer', null=True)
    currency = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='transfer', null=True)
    amount = models.FloatField(null=True)
    response = JSONField(null=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)
    from_wallet, to_wallet = [models.CharField(max_length=50, null=True) for i in range(2)]
    transferid = models.BigIntegerField(null=True)
    status = models.BooleanField(default=None, null=True)
    datetime = models.DateTimeField(null=True)
    timestamp = models.FloatField(null=True)

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True
    )

    class Meta:
        verbose_name_plural = "Transfers"

    def __str__(self):
        return str(self.transferid)
