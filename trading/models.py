import ccxt
from django.db import models
from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.utils import timezone
from django.db.models import Q
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from capital.methods import *
from strategy.models import Strategy, Allocation
from marketsdata.models import Exchange, Market, Currency
from trading.error import *
from trading.methods import *
import structlog
from datetime import timedelta, datetime
from pprint import pprint
from decimal import Decimal
import numpy as np
import pandas as pd
import traceback
import sys
from timeit import default_timer as timer

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

    # Return a dictionary with balance of a specific wallet
    def get_usdt_balance(self, wallet, key='total'):

        client = self.exchange.get_ccxt_client(self)
        client.options['defaultType'] = wallet
        response = client.fetchBalance()
        response[key] = {k: v for k, v in response[key].items() if v > 0}
        return self.convert_balance(response[key])

    def convert_balance(self, bal):
        def convert_value(key, value):
            price = Currency.objects.get(code=key).get_latest_price(self.exchange)
            return value * price

        return dict(map(lambda x: (x[0], convert_value(x[0], x[1])), bal.items()))

    # Returns a Pandas Series with target USDT
    def get_target_usdt(self):
        dic = dict()
        for wallet in self.exchange.get_wallets():
            dic[wallet] = self.get_usdt_balance(wallet)
        total = sum_wallet_balances(dic)
        weights = self.strategy.get_target()
        print(total)
        print(weights)
        return total * weights

    # Returns a Pandas Series with target quantity
    def get_target(self):
        target = self.get_target_usdt()
        for code in target:
            target[code] /= Currency.objects.get(code=code).get_latest_price(self.exchange)
        return target


    ##############################################################################################
    # Construct a dataframe with wallets balance, positions, exposure and delta
    def create_dataframes(self, target=None):

        try:

            start = timer()
            funds = self.get_fund_latest()
            allocations = self.strategy.get_allocations()
            df = pd.DataFrame()

            # Insert wallets balances
            for i in ['total', 'free', 'used']:
                for wallet, dic1 in getattr(funds, i).items():
                    for code, dic2 in dic1.items():
                        for field, value in dic2.items():

                            # Create columns for total, free and used quantities
                            if field == 'quantity':

                                cols = pd.MultiIndex.from_product([['wallet'], [i + '_quantity']], names=['level_1',
                                                                                                          'level_2'])
                                indexes = pd.MultiIndex.from_tuples([(code, wallet)], names=['code', 'wallet'])
                                tmp = pd.DataFrame(value, index=indexes, columns=cols)
                                df = pd.concat([df, tmp], axis=0).groupby(level=[0, 1]).mean()

            # Insert dollar value of open positions (sum USDT and BUSD margined)
            positions = self.create_positions_df()
            if not positions.empty:
                for index, position in positions.groupby(['code', 'wallet']).sum().iterrows():
                    price = get_price_hourly(self.exchange, index[0], self.strategy.exchange.dollar_currency)
                    df.loc[(index[0], index[1]), ('position', 'quantity')] = position.quantity
                    df.loc[(index[0], index[1]), ('position', 'value')] = position.quantity * price
            else:
                df[('position', 'value')] = np.nan
                df[('position', 'quantity')] = np.nan

            # Insert allocations weights
            for value in allocations.values():
                for code in list(value.keys()):
                    df.loc[code, ('target', 'percent')] = value[code]['weight']

            # Fill nan percents with 0
            df[('target', 'percent')] = df.target.percent.fillna(0)

            # Insert prices
            for code in df.index.get_level_values(0):
                # df.loc[code, ('price', 'ws')] = get_price_ws(self.exchange, code, prices) spot
                df.loc[code, ('price', 'hourly')] = get_price_hourly(self.exchange,
                                                                     code,
                                                                     self.strategy.exchange.dollar_currency)

            # Insert wallets balances in dollar
            print(df.to_string())
            for index, row in df.iterrows():
                df.loc[index, ('wallet', 'total_value')] = row.wallet.total_quantity * row.price.hourly
                df.loc[index, ('wallet', 'free_value')] = row.wallet.free_quantity * row.price.hourly
                df.loc[index, ('wallet', 'used_value')] = row.wallet.used_quantity * row.price.hourly

            # Insert exposure value (balance + positions) and total
            df[('exposure', 'value')] = pd.concat([df.position.value, df.wallet.total_value], axis=1).sum(axis=1)
            df[('exposure', 'quantity')] = pd.concat([df.position.quantity, df.wallet.total_quantity], axis=1).sum(axis=1)
            df[('exposure', 'total_value')] = df.exposure.value.groupby('code').transform('sum')
            df[('exposure', 'total_quantity')] = df.exposure.quantity.groupby('code').transform('sum')

            # Insert max_withdrawal
            if not self.strategy.all_pairs:
                for wallet in funds.total.keys():
                    margin_assets = funds.margin_assets[wallet]
                    if margin_assets:
                        for asset in margin_assets:
                            coin = asset['asset']
                            withdrawal = float(asset['maxWithdrawAmount'])
                            df.sort_index(axis=0, inplace=True)
                            df.loc[(coin, wallet), ('withdrawal', 'quantity')] = withdrawal

            # Insert account balance
            balance = df.wallet.total_value.sum()
            df['account', 'balance'] = balance

            # Restore previously saved target value and quantity to avoid instability
            # at the end of the rebalancing with a lot of orders with small amount
            if target is None:
                # Calculate target
                df[('target', 'value')] = df.target.percent * balance * float(self.leverage)
                df[('target', 'quantity')] = df.target.value / df.price.hourly

            else:
                # Else restore previously saved target value and balance (avoid ping-pong orders)
                inter = df.index.intersection(target.index)
                df.loc[inter, ('target', 'value')] = target.loc[inter, 'value']
                df.loc[inter, ('target', 'quantity')] = target.loc[inter, 'quantity']

            # Insert delta
            df[('delta', 'value')] = df.exposure.total_value - df.target.value
            df[('delta', 'value')] = df[('delta', 'value')].round(2)
            df[('delta', 'quantity')] = df.exposure.total_quantity - df.target.quantity

            df.sort_index(axis=1, inplace=True)
            df.sort_index(axis=0, inplace=True)

            end = timer()
            log.info('Create dataframe in {0} sec'.format(round(end - start, 2)))

        except Exception as e:
            log.exception('create_dataframes() failed: {0} {1}'.format(type(e).__name__, str(e)))
            log.error('Traceback', traceback=traceback.format_exc())

        else:

            return df, positions

    # Construct a dataframe with open positions
    def create_positions_df(self):

        # Convert dollar to currency
        def size_to_currency(size):

            # price = get_price_ws(self.exchange, code, prices)
            # price = get_price_hourly(self.exchange, code)

            if margined != position.market.base.code:
                return size
            else:
                return notional_value  # already in currency if coin-margined

        # Convert PnL to currency
        def pnl_to_currency(pnl):

            if margined != position.market.base.code:
                return pnl / get_price_hourly(self.exchange, code, self.strategy.exchange.dollar_currency)
            else:
                return pnl  # already in currency if coin-margined

        # Convert to dollar
        def to_dollar(amount):

            if margined != position.market.base.code:
                return amount
            else:
                # price = get_price_ws(self.exchange, prices, margined)
                price = get_price_hourly(self.exchange, code, self.strategy.exchange.dollar_currency)
                return amount * price

        from trading import methods
        df = pd.DataFrame()
        positions = self.positions.all()
        codes = self.get_codes()
        hedge_total = self.get_hedge_total()

        if positions:
            for position in positions:

                # Select market
                wallet = position.market.wallet
                type = position.market.type
                margined = position.market.margined.code
                symbol = position.market.symbol
                code = position.market.base.code

                # Select position stats
                side = position.side
                notional_value = position.notional_value
                size = float(position.size)
                asset = position.asset.code if position.asset else 'Cont'
                leverage = position.leverage
                pnl = position.unrealized_pnl

                # Convert to currency
                quantity = size_to_currency(size)
                pnl_qty = pnl_to_currency(pnl)
                net_qty = quantity + pnl_qty
                abs_qty = abs(net_qty)

                # Convert to stable
                stable_value = to_dollar(notional_value)
                pnl_value = to_dollar(pnl)
                net_value = stable_value + pnl_value
                abs_value = abs(net_value)

                # Calculate initial_margin
                initial_margin = stable_value / leverage

                # # Calculate net value and net quantity
                # value_net = abs(value) + pnl_value
                # quantity_net = abs(quantity) + quantity_pnl

                # Create multilevel columns
                indexes = pd.MultiIndex.from_tuples([(code,
                                                      position.market.quote.code,
                                                      wallet,
                                                      symbol,
                                                      type,
                                                      position.market.contract_type,
                                                      margined
                                                      )], names=['code',
                                                                 'quote',
                                                                 'wallet',
                                                                 'symbol',
                                                                 'type',
                                                                 'contract_type',
                                                                 'margined'])
                # Construct dataframe and normalize rows
                d = pd.DataFrame([[side, size, asset, quantity, notional_value, initial_margin,
                                   leverage,
                                   stable_value, pnl_value, net_value, abs_value,
                                   pnl_qty, net_qty, abs_qty]],
                                 index=indexes,
                                 columns=['side', 'size', 'asset', 'quantity', 'notional_value', 'initial_margin',
                                          'leverage',
                                          'stable_value', 'pnl_value', 'net_value', 'abs_value',
                                          'pnl_qty', 'net_qty', 'abs_qty']
                                 )

                df = pd.concat([df, d], axis=0)
        else:
            log.info('No position found')
            return pd.DataFrame()

        # Iterate through coins in account and positions
        for code in codes:
            hedge = self.get_hedge(code)
            balance = self.get_balance(code)
            hedge_ = hedge

            # Sort dataframe with USDT-margined position at the top
            df.sort_index(level='margined', ascending=False, axis=0, inplace=True)

            for index, row in df.loc[:, :].iterrows():
                if code == index[0]:
                    if row['side'] == 'sell':

                        df.sort_index(axis=0, inplace=True)
                        df.loc[index, 'hedge_code'] = hedge
                        df.loc[index, 'hedge_total'] = hedge_total
                        df.loc[index, 'balance'] = balance

                        # Assign hedge to USDT-margined position first
                        if hedge_:
                            hedge_position = min(hedge_, row.abs_value)
                            hedge_ -= hedge_position

                            df.loc[index, 'hedge_position'] = hedge_position
                            df.loc[index, 'hedge_position_ratio'] = hedge_position / row.abs_value

                            # If position is USD margined determine margin allocated to hedge
                            if index[6] in self.exchange.get_stablecoins():
                                hedge_position_margin = hedge_position / row.leverage
                                df.loc[index, 'hedge_position_margin'] = hedge_position_margin
                            else:
                                hedge_position_margin = 0

                            # Estimate capacity used by the position
                            df.loc[index, 'hedge_capacity_used'] = hedge_position + hedge_position_margin

        df.sort_index(axis=0, inplace=True)
        # df.sort_index(axis=1, inplace=True)

        return df

    # Return a list of codes in all wallets
    def get_codes(self, greater_than=None):

        funds = self.get_fund_latest()
        codes = []

        for wallet, dic in funds.total.items():
            if greater_than is not None:
                for code in dic.keys():
                    if funds.total[wallet][code]['value'] > greater_than:
                        codes.append(code)
            else:
                lst = list(funds.total[wallet].keys())
                codes.extend(lst)

        return list(set(codes))

    # Return a list of stablecoins in all wallets
    def get_codes_stable(self):
        codes = [code for code in self.get_codes() if Currency.objects.get(code=code).stable_coin]
        return codes

    # Return a list of codes with opened positions
    def get_positions_codes(self):
        return list(set(position.market.base.code for position in self.positions.all()))

    # Return absolute value of a code or a list of codes
    def get_balance(self, code):

        funds = self.get_fund_latest()
        values = []

        for wallet, coins in funds.total.items():
            for key, value in coins.items():
                if key == code:
                    # price = get_price_ws(self.exchange, code, prices)
                    price = get_price_hourly(self.exchange, code, self.strategy.exchange.dollar_currency)
                    value = value['quantity'] * price
                    values.append(value)

        return sum(values)

    # Return absolute value of short positions opened for a code
    def get_shorts(self, code):

        positions = self.positions.all()
        shorts = []

        for position in positions:
            if position.side == 'sell':
                if position.market.base.code == code:
                    if position.market.margined == position.exchange.dollar_currency:
                        value = abs(position.notional_value + position.unrealized_pnl)
                    else:
                        # price = get_price_ws(self.exchange, position.market.margined.code, prices)
                        price = get_price_hourly(self.exchange,
                                                 position.market.margined.code,
                                                 self.strategy.exchange.dollar_currency
                                                 )
                        value = abs(position.notional_value + position.unrealized_pnl) * price
                    shorts.append(value)

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
    def get_max_hedge(self, code):
        return self.get_balance(code) - self.get_hedge(code)

    # Create a new order object
    def create_order(self, route, segment):

        segment = route[segment]
        market = Market.objects.get(exchange=self.exchange,
                                    symbol=segment.market.symbol,
                                    wallet=segment.market.wallet
                                    )

        # Set order type and price
        if self.limit_order:
            order_type = 'limit'
            price = segment.trade.price
        else:
            order_type = 'market'
            price = None

        # Select size
        amount = segment.trade.order_qty
        if market.type == 'derivative':
            if market.contract_type == 'perpetual':
                if market.margined == market.base:
                    amount = segment.trade.cont

        defaults = dict(
            account=self,
            action=segment.type.action,
            market=market,
            price=price,
            type=order_type,
            side=segment.trade.side,
            amount=amount,
            status='created',
            segments=route.to_json()
        )

        # Set parameters
        if not pd.isna(segment.trade.params):
            params = eval(segment.trade.params)
            if 'quoteOrderQty' in params:
                defaults['amount'] = None
            defaults['params'] = params

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

    @property
    def latest_balance(self):
        return self.get_fund_latest().balance

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
    balance, total, free, used, margin_assets, positions = [models.JSONField(null=True) for i in range(6)]
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
    average, price, price_strategy, distance = [models.FloatField(null=True, blank=True) for i in range(4)]
    fee, trades, params, response, route, segments = [models.JSONField(null=True) for i in range(6)]
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
    response = models.JSONField(null=True)
    response_2 = models.JSONField(null=True)
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
    response = models.JSONField(null=True)
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
