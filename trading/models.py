import ccxt
from django.db import models
from django.db.models import Sum
from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.utils import timezone
from django.db.models import Q
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from capital.methods import *
from strategy.models import Strategy
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
import collections
import math
from picklefield.fields import PickledObjectField
import warnings
import random
import string

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

import json

log = structlog.get_logger(__name__)

datetime_directives_std = '%Y-%m-%dT%H:%M:%S.%fZ'


class Account(models.Model):
    objects = models.Manager()
    name = models.CharField(max_length=100, null=True, blank=False)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='account', blank=True, null=True)
    strategy = models.ForeignKey(Strategy, related_name='account', on_delete=models.SET_NULL, blank=True, null=True)
    quote = models.CharField(max_length=10, null=True, choices=(('USDT', 'USDT'), ('BUSD', 'BUSD')), default='USDT')

    balances = PickledObjectField(null=True)
    balances_dt = models.DateTimeField(null=True)
    params = models.JSONField(null=True, blank=True)
    valid_credentials = models.BooleanField(null=True, default=None)
    active = models.BooleanField(null=True, blank=False, default=False)
    order_type = models.CharField(max_length=10, null=True, choices=(('limit', 'limit'), ('market', 'market')),
                                  default='limit')
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

    # Fetch coins and create balances dataframe
    def get_balances_qty(self):

        client = self.exchange.get_ccxt_client(self)
        log.info('Get balances qty start')

        # Reset attribute
        self.balances = pd.DataFrame()

        # Iterate through exchange's wallets
        for wallet in self.exchange.get_wallets():

            client.options['defaultType'] = wallet
            response = client.fetchBalance()
            for key in ['total', 'free', 'used']:

                # Exclude LBTC from dictionary (staking or earning account)
                dic = {k: v for k, v in response[key].items() if v > 0 and k != 'LDBTC'}

                if dic:
                    columns = pd.MultiIndex.from_product([[wallet], [key], ['quantity']])
                    tmp = pd.DataFrame(index=dic.keys(),
                                       data=dic.values(),
                                       columns=columns
                                       )
                    self.balances = tmp if not hasattr(self, 'balances') else pd.concat([self.balances, tmp])
                    self.balances = self.balances.groupby(level=0).last()
                else:
                    self.balances[(wallet, key, 'quantity')] = np.nan

        self.save()

        log.info('Get balances qty done')

    # Insert bid/ask of spot markets
    def insert_spot_prices(self, codes):

        for code in codes:

            if 'price' in self.balances.columns.get_level_values(0).tolist():
                if 'spot' in self.balances.price.columns.get_level_values(0).tolist():
                    if code in self.balances.price.spot.bid.dropna().index.tolist():
                        continue

            # log.info('Insert spot prices for {0}'.format(code))

            try:
                currency = Currency.objects.get(code=code)

            except ObjectDoesNotExist:

                log.error('Spot market {0}/{1} not found'.format(code, self.quote))

                self.balances.loc[code, ('price', 'spot', 'bid')] = np.nan
                self.balances.loc[code, ('price', 'spot', 'ask')] = np.nan

            else:
                for key in ['bid', 'ask']:
                    p = currency.get_latest_price(self.exchange, self.quote, key)
                    self.balances.loc[code, ('price', 'spot', key)] = p

        self.save()

    # Insert bid/ask of future markets
    def insert_futu_prices(self, codes):

        for code in codes:

            if 'price' in self.balances.columns.get_level_values(0).tolist():
                if 'future' in self.balances.price.columns.get_level_values(0).tolist():
                    if code in self.balances.price.future.bid.dropna().index.tolist():
                        continue

            # log.info('Insert future prices for {0}'.format(code))

            try:
                market = Market.objects.get(base__code=code,
                                            quote__code=self.quote,
                                            type='derivative',
                                            contract_type='perpetual',
                                            exchange=self.exchange)

            except ObjectDoesNotExist:

                log.error('Future market {0}/{1} not found'.format(code, self.quote))

                self.balances.loc[code, ('price', 'future', 'bid')] = np.nan
                self.balances.loc[code, ('price', 'future', 'ask')] = np.nan

            else:
                for key in ['bid', 'ask']:
                    self.balances.loc[code, ('price', 'future', key)] = market.get_latest_price(key)

        self.save()

    # Convert quantity in dollar in balances dataframe
    def calculate_balances_value(self):

        codes = self.balances.spot.total.quantity.index.tolist()
        self.insert_spot_prices(codes)
        self.insert_futu_prices(codes)

        log.info('Calculate balances value')

        # Iterate through wallets, free, used and total quantities
        for wallet in self.exchange.get_wallets():
            for tp in ['free', 'total', 'used']:
                for coin in self.balances[wallet][tp]['quantity'].dropna().index.tolist():

                    if coin == self.quote:
                        price = 1
                    else:
                        price = self.balances.price.spot['bid'][coin]

                    # Calculate value
                    value = price * self.balances[wallet][tp]['quantity'][coin]
                    self.balances.loc[coin, (wallet, tp, 'value')] = value

        # Drop dust coins
        mask = self.balances.loc[:, self.balances.columns.get_level_values(2) == 'value'] > 1
        self.balances = self.balances.loc[(mask == True).any(axis=1)]
        self.save()

        log.info('Calculate balances value complete')

    # Fetch and update open positions in balances dataframe
    def get_positions_value(self):

        log.info('Get positions start')

        # Get client
        client = self.exchange.get_ccxt_client(self)
        client.options['defaultType'] = 'future'

        #  and query all futures positions
        response = client.fapiPrivateGetPositionRisk()
        opened = [i for i in response if float(i['positionAmt']) != 0]
        closed = [i for i in response if float(i['positionAmt']) == 0]

        if opened:

            for position in opened:
                market = Market.objects.get(exchange=self.exchange, response__id=position['symbol'], type='derivative')
                code = market.base.code

                quantity = float(position['positionAmt'])
                self.balances.loc[code, ('position', 'open', 'quantity')] = quantity
                self.balances.loc[code, ('position', 'open', 'side')] = 'buy' if quantity > 0 else 'sell'
                self.balances.loc[code, ('position', 'open', 'value')] = quantity * float(position['markPrice'])
                self.balances.loc[code, ('position', 'open', 'leverage')] = float(position['leverage'])
                self.balances.loc[code, ('position', 'open', 'unrealized_pnl')] = float(position['unRealizedProfit'])
                self.balances.loc[code, ('position', 'open', 'liquidation')] = float(position['liquidationPrice'])

            # Insert prices
            codes = self.balances.position.open.quantity.dropna().index.tolist()
            self.insert_futu_prices(codes)
            self.insert_spot_prices(codes)

        self.save()
        log.info('Get positions done')

    # Return account total value
    def account_value(self):
        wallets = []
        for level in list(set(self.balances.columns.get_level_values(0))):

            # Exclude positions
            if level in ['spot', 'future']:
                # Sum value of all coins
                wallets.append(self.balances[level].total.value.sum())

        # Sum value of all wallet
        return sum(wallets)

    # Create columns with targets
    def get_target(self):

        try:

            # Insert percentage
            target_pct = self.strategy.load_targets()

            for coin, pct in target_pct.items():
                self.balances.loc[coin, ('account', 'target', 'percent')] = pct

                log.info('Target percent {0}: {1}'.format(coin, round(pct * 100, 2)))

            # Determine values
            value = self.account_value() * target_pct
            for coin, val in value.items():
                self.balances.loc[coin, ('account', 'target', 'value')] = val

            # Determine quantities
            for coin, val in value.items():

                # Insert prices of new coins
                if coin not in self.balances.price.spot.bid.dropna().index.tolist():
                    self.insert_prices(coin)

                # Price found ?
                if not np.isnan(self.balances.price.spot.bid[coin]):

                    # Calculate quantity
                    qty = val / self.balances.price.spot.bid[coin]
                    self.balances.loc[coin, ('account', 'target', 'quantity')] = qty

                else:
                    log.error('Can not calculate target value of {0}, price not found'.format(coin))

        except AttributeError as e:
            self.trading = False
            raise Exception('Unable to get targets weights {0}'.format(e.__class__.__name__))

        except ValueError as e:
            self.trading = False
            raise Exception('Unable to get targets weights {0}'.format(e.__class__.__name__))

        finally:

            self.save()

    ##################################

    # Calculate net exposure and delta
    def get_delta(self):

        log.info('Calculate delta')
        target = self.balances.account.target.quantity.dropna()
        acc_value = self.account_value()

        #  Select total quantities of wallets and open positions
        df = self.balances.loc[:, (self.balances.columns.get_level_values(2) == 'quantity')]
        mask = df.columns.get_level_values(1).isin(['total', 'open'])
        df = df.loc[:, mask]
        df = df.dropna(axis=1, how='all')  # drop wallet with nan

        # Sum sum to determine exposure per coin
        self.balances.loc[:, ('account', 'current', 'exposure')] = df.sum(axis=1)

        # Calculate percentage for each coin
        for coin, exp in self.balances.account.current.exposure.items():
            bid = self.balances.price.spot.bid[coin]
            if 'position' in self.balances.columns.get_level_values(0):
                pos_value = self.balances.position.open.value.dropna().sum()
            else:
                pos_value = 0
            percent = (exp * bid) / (acc_value - pos_value)
            self.balances.loc[coin, ('account', 'current', 'percent')] = percent

            log.info('Target percent {0}: {1}'.format(coin, round(exp * 100, 2)))

        # Calculate value allocated to each coin
        for coin, exp in self.balances.account.current.exposure.items():
            bid = self.balances.price.spot.bid[coin]
            self.balances.loc[coin, ('account', 'current', 'value')] = exp * bid

        # Iterate through target coins and calculate delta
        for coin in target.index.values.tolist():

            # Coins already in account ?
            if coin in df.index.values.tolist():
                qty = self.balances.loc[coin, ('account', 'current', 'exposure')]
                self.balances.loc[coin, ('account', 'target', 'delta')] = qty - target[coin]

            # Coins not in account ?
            else:
                self.balances.loc[coin, ('account', 'target', 'delta')] = -target[coin]

        # Iterate through coins in account and calculate delta
        for coin in df.index.values.tolist():

            # Coin not in target ?
            if coin not in target.index.values.tolist():
                qty = self.balances.loc[coin, ('account', 'current', 'exposure')]
                self.balances.loc[coin, ('account', 'target', 'delta')] = qty
                self.balances.loc[coin, ('account', 'target', 'quantity')] = 0
                self.balances.loc[coin, ('account', 'target', 'percent')] = 0
                self.balances.loc[coin, ('account', 'target', 'value')] = 0

        self.save()
        log.info('Get delta done')

    # Return a list of codes to sell
    def codes_to_sell(self):
        delta = self.balances.account.target.delta
        return [i for i in delta.loc[delta > 0].index.values.tolist() if i != self.quote]

    # Return a list of codes to buy
    def codes_to_buy(self):
        delta = self.balances.account.target.delta
        return [i for i in delta.loc[delta < 0].index.values.tolist() if i != self.quote]

    # Return a Series with codes/quantity to sell spot
    def to_sell_spot(self):

        # Select codes to sell in spot
        codes = self.codes_to_sell()
        spot = self.balances.spot.free.quantity[codes].dropna()

        # Select corresponding target
        target = self.balances.account.target.quantity[spot.index]

        # Determine delta
        delta = spot - target

        # Select minimum between spot and delta
        return spot.append(delta).groupby(level=0).min()

    # Return a Series with codes/quantity to close short
    def to_close_short(self):
        codes = self.codes_to_buy()
        if 'position' in self.balances.columns.get_level_values(0):
            qty = self.balances.position.open.quantity.dropna()
            opened_short = qty[qty < 0].index.tolist()
            to_close = [c for c in codes if c in opened_short]
            delta = self.balances.account.target.delta

            # Determine the min values between the size of the opened short and delta quantity
            s1 = abs(qty[to_close])
            s2 = abs(delta[to_close])
            s3 = s1.append(s2).groupby(level=0).min()

            return s3

        else:
            return pd.Series()

    # Return a Series with codes/quantity to buy spot
    def to_buy_spot(self):
        codes = self.codes_to_buy()
        return abs(self.balances.account.target.delta[codes])

    # Return a Series with codes/value to buy spot
    def to_buy_spot_value(self):
        qty = self.to_buy_spot()
        price = self.balances.price.spot.bid[qty.index]
        return qty * price

    # Return a Series with codes/quantity to open short
    def to_open_short(self):
        codes = self.codes_to_sell()
        if codes:
            target = self.balances.account.target.quantity.dropna()
            to_short = [i for i in target.loc[target < 0].index.values.tolist()]
            open_short = list(set(codes) & set(to_short))
            return abs(target[open_short])
        else:
            return pd.Series()

    # Return a Series with codes/value to open short
    def to_open_short_value(self):
        qty = self.to_open_short()
        price = self.balances.price.spot.bid[qty.index]
        return qty * price

    # Determine order size based on available resources
    def size_order(self, code, quantity, action):

        log.info(' ')
        log.info('Size order {0}'.format(code))

        # Determine wallet
        if action in ['buy_spot', 'sell_spot']:
            wallet = 'spot'
        if action in ['open_short', 'close_short']:
            wallet = 'future'

        # Determine side
        if action in ['buy_spot', 'close_short']:
            side = 'buy'
            key = 'ask'
        if action in ['open_short', 'sell_spot']:
            side = 'sell'
            key = 'bid'

        offset = 0

        others = Order.objects.filter(account=self,
                                      market__wallet=wallet,
                                      market__base__code=code,
                                      action=action,
                                      status__in=['preparation', 'new', 'open']
                                      )

        if others.exists():
            size = others.aggregate(Sum('size'))['size__sum']
            filled = others.aggregate(Sum('filled'))['filled__sum']
            offset = size - filled

            log.info('{0} order(s) object found (offset {1})'.format(len(others), offset))

            for other in others:
                log.info('client_id -> {0}'.format(other.clientid))
                log.info('order_id -> {0}'.format(other.orderid))
                log.info('status -> {0}'.format(other.status))
                log.info('size -> {0}'.format(other.size))
                log.info(' ')

        # Select price
        price = self.balances.price[wallet][key][code]

        # Determine order value and size when USDT resources are released
        if action == 'sell_spot':
            order_size = quantity - offset  # offset qty of open/filled order
            order_value = order_size * price

        elif action == 'close_short':
            order_size = quantity - offset
            order_value = order_size * price

        else:

            # Determine order value and size when USDT resources are allocated
            if action == 'buy_spot':
                available = self.balances.spot.free.quantity[self.quote]
            elif action == 'open_short':
                total = self.balances.future.total.quantity[self.quote]
                open_value = abs(self.balances.position.open.value.dropna()).sum()
                available = max(0, total - open_value)

            if not pd.isna(available):

                value = math.trunc(quantity * price)
                order_value = min(available, value)
                order_size = order_value / price

                # Offset order size with size from another order
                order_size -= offset
                order_value = order_size * price

                log.info('resources available in {1} : {0}'.format(round(available, 2), wallet))
                log.info('buy size {0}'.format(round(order_size, 4)))
                log.info('buy value {0}'.format(round(order_value, 2)))
                log.info('offset {0}'.format(round(offset, 4)))

            else:
                order_size = 0
                order_value = 0

        return dict(order_size=order_size,
                    order_value=order_value,
                    price=price,
                    action=action,
                    code=code,
                    side=side,
                    wallet=wallet
                    )

    # Prepare dictionary key:value for an order
    def prep_order(self, wallet, code, order_size, order_value, price, action, side):

        # Select market
        markets = Market.objects.filter(base__code=code,
                                        quote__code=self.quote,
                                        exchange=self.exchange)

        if wallet == 'spot':
            market = markets.get(type='spot')
        else:
            market = markets.get(type='derivative', contract_type='perpetual')

        # Format decimal
        size = format_decimal(counting_mode=self.exchange.precision_mode,
                              precision=market.precision['amount'],
                              n=order_size)

        # Test amount limits MIN and MAX
        if limit_amount(market, size):

            # Test cost limits MIN and MAX
            cost = order_value
            min_notional = limit_cost(market, cost)
            reduce_only = False

            # If cost limit not satisfied and close short set reduce_only = True
            if not min_notional:
                if market.exchange.exid == 'binance':
                    if market.type == 'derivative':
                        if market.margined.code == 'USDT':
                            if action == 'close_short':
                                reduce_only = True

                # Else return
                if not reduce_only:
                    log.info('Cost not satisfied for {2} {1} {0}'.format(wallet, market.base.code, size))
                    return False, dict()

            # Generate order_id
            alphanumeric = 'abcdefghijklmnopqrstuvwABCDEFGHIJKLMNOPQRSTUVWWXYZ01234689'
            clientid = ''.join((random.choice(alphanumeric)) for x in range(5))

            Order.objects.create(
                account=self,
                market=market,
                clientid=clientid,
                type=self.order_type,
                filled=0,
                side=side,
                action=action,
                amount=size,
                status='preparation',
                sender='app'
            )

            # Determine resources used (code and quantity)
            if action in ['buy_spot', 'open_short']:
                code_res = self.quote
                used_qty = order_value
            else:
                code_res = code
                used_qty = size

            # Update free and used resources in balances df
            self.balances.loc[code_res, (wallet, 'used', 'quantity')] += used_qty
            self.balances.loc[code_res, (wallet, 'used', 'value')] += order_value
            self.balances.loc[code_res, (wallet, 'free', 'quantity')] -= used_qty
            self.balances.loc[code_res, (wallet, 'free', 'value')] -= order_value
            self.save()

            log.info(' ')
            log.info('Prepare order {0}'.format(code))
            log.info('wallet {0}'.format(wallet))
            log.info('order size {0}'.format(size))
            log.info('order value {0}'.format(order_value))
            log.info('clientid {0}'.format(clientid))
            log.info('action {0}'.format(action))
            log.info('resource used {0}'.format(used_qty))
            log.info('resource code {0}'.format(code_res))
            log.info(' ')

            return True, dict(account_id=self.id,
                              action=action,
                              code=code,
                              clientid=clientid,
                              order_type='limit',
                              price=price,
                              reduce_only=reduce_only,
                              side=side,
                              size=size,
                              symbol=market.symbol,
                              wallet=market.wallet
                              )

        else:
            log.info('Condition not satisfied for {2} {1} {0}'.format(wallet, market.base.code, size))
            return False, dict()

    # Update order object
    def update_order_object(self, wallet, response):
        #
        if response:

            orderid = response['id']
            status = response['info']['status'].lower()
            clientid = response['info']['clientOrderId']

            try:
                # Object with orderID exists ?
                order = Order.objects.get(account=self, orderid=orderid)

            except ObjectDoesNotExist:

                try:
                    # Object with clientID exists ?
                    order = Order.objects.get(account=self, clientid=clientid)

                except ObjectDoesNotExist:

                    symbol = response['symbol']
                    market = Market.objects.get(exchange=self.exchange, wallet=wallet, symbol=symbol)

                    log.info('Create user order')

                    # Create object
                    Order.objects.create(
                        account=self,
                        sender='user',
                        market=market,
                        clientid=orderid,
                        status=status
                    )

                    log.warning('Cancel user order')
                    from trading.tasks import send_cancel_order
                    send_cancel_order.delay(self.id, orderid)
                    return

                else:
                    # Update orderID
                    order.orderid = orderid

            finally:

                # Select attributes
                code = order.market.base.code
                wallet = order.market.wallet
                action = order.action

                # Get traded amount
                filled_prev = order.filled
                filled_total = response['filled']

                # Determine new trade
                if filled_total > filled_prev:
                    filled_new = filled_total - filled_prev

                    if action in ['sell_spot', 'close_short']:
                        log.info('New resources available')
                    elif action == 'buy_spot':
                        log.info('Coins bought')
                    elif action == 'open_short':
                        log.info('Contract sold')

                else:
                    filled_new = 0

                log.info('code {0} ({1})'.format(code, wallet))
                log.info('status {0}'.format(status))
                log.info('action {0}'.format(action))
                log.info('filled new {0}'.format(filled_new))
                log.info('filled total {0}'.format(filled_total))

                # Update attributes
                order.status = status
                order.filled = filled_total
                order.response = response
                order.save()

                return filled_new

        else:
            log.info('Empty response from exchange {0}'.format(wallet))

    # Update balances after new trade
    def update_balances(self, action, wallet, code, qty_filled):

        if qty_filled:

            log.info('')
            log.info('Update balances')
            log.info('***************')
            log.info('action {0}'.format(action))
            log.info('-> {0} {1}'.format(code, wallet))

            # Determine key
            if action in ['buy_spot', 'close_short']:
                key = 'ask'
            if action in ['open_short', 'sell_spot']:
                key = 'bid'

            # Determine price
            if wallet == 'spot':
                price = self.balances.price.spot[key]
            else:
                price = self.balances.price.future[key]

            # Calculate trade value
            val_filled = qty_filled * price

            # Determine amounts to offset
            if action in ['sell_spot', 'open_short']:
                qty_filled = -qty_filled
                val_filled = -val_filled

            old = self.balances.copy()

            # Update position and free margin
            if action in ['open_short', 'close_short']:

                log.info('Filled new {0}'.format(qty_filled))
                log.info('Filled value {0}'.format(val_filled))
                log.info('')
                log.info('Position open before')
                log.info(self.balances.position.open)

                self.balances.loc[code, ('position', 'open', 'quantity')] += qty_filled
                self.balances.loc[code, ('position', 'open', 'value')] += val_filled

                log.info('')
                log.info('Position open after')
                log.info(self.balances.position.open)

                log.info('')
                log.info('Future total quantity before')
                log.info(self.balances.future.total.quantity)

                self.balances.loc[self.quote, ('future', 'total', 'quantity')] += val_filled
                self.balances.loc[self.quote, ('future', 'free', 'quantity')] += val_filled
                self.balances.loc[self.quote, ('future', 'used', 'quantity')] += val_filled

                log.info('')
                log.info('Future total quantity after')
                log.info(self.balances.future.total.quantity)

            else:

                log.info('')
                log.info('Spot total quantity before')
                log.info(self.balances.spot.total.quantity)

                # Or update spot
                self.balances.loc[code, (wallet, 'total', 'quantity')] += qty_filled
                self.balances.loc[code, (wallet, 'free', 'quantity')] += qty_filled
                self.balances.loc[code, (wallet, 'used', 'quantity')] += qty_filled

                log.info('')
                log.info('Spot total quantity after')
                log.info(self.balances.spot.total.quantity)

    # Update balances after a transfer
    def update_balances_after_transfer(self, source, dest, quantity):

        log.info('')
        log.info('Update balances dataframe')

        # Update source wallet
        self.balances.loc[self.quote, (source, 'total', 'quantity')] -= quantity
        self.balances.loc[self.quote, (source, 'total', 'value')] -= quantity
        self.balances.loc[self.quote, (source, 'free', 'quantity')] -= quantity
        self.balances.loc[self.quote, (source, 'free', 'value')] -= quantity

        # Update destination
        self.balances.loc[self.quote, (dest, 'total', 'quantity')] += quantity
        self.balances.loc[self.quote, (dest, 'total', 'value')] += quantity
        self.balances.loc[self.quote, (dest, 'free', 'quantity')] += quantity
        self.balances.loc[self.quote, (dest, 'free', 'value')] += quantity

    # Sell spot
    def sell_spot_all(self):
        from trading.tasks import send_create_order
        log.info('')
        log.info('Sell spot')
        log.info('*********')
        for code, quantity in self.to_sell_spot().items():
            kwargs = self.size_order(code, quantity, 'sell_spot')
            valid, order = self.prep_order(**kwargs)
            if valid:
                args = order.values()
                send_create_order.delay(*args)

    # Close short
    def close_short_all(self):
        log.info('')
        log.info('Close short')
        log.info('***********')
        from trading.tasks import send_create_order
        opened_short = self.to_close_short()
        for code, quantity in opened_short.items():
            kwargs = self.size_order(code, quantity, 'close_short')
            valid, order = self.prep_order(**kwargs)
            if valid:
                args = order.values()
                send_create_order.delay(*args)

    # Buy spot
    def buy_spot_all(self):
        log.info('')
        log.info('Buy spot')
        log.info('********')
        from trading.tasks import send_create_order
        for code, quantity in self.to_buy_spot().items():
            kwargs = self.size_order(code, quantity, 'buy_spot')
            valid, order = self.prep_order(**kwargs)
            if valid:
                args = order.values()
                send_create_order.delay(*args)

    # Open short
    def open_short_all(self):
        log.info('')
        log.info('Open short')
        log.info('**********')
        from trading.tasks import send_create_order
        for code, quantity in self.to_open_short().items():
            kwargs = self.size_order(code, quantity, 'open_short')
            valid, order = self.prep_order(**kwargs)
            if valid:
                args = order.values()
                send_create_order.delay(*args)

    # Market sell spot account
    def market_sell(self):
        #
        for code, amount in self.balances.spot.free.quantity.T.items():
            if code != self.quote:
                if not np.isnan(amount):

                    log.info('Sell {0}'.format(code))

                    price = self.balances.price.spot.bid
                    value = amount * price
                    valid, order = self.prep_order('spot', code, amount, value, price, 'sell_spot', 'sell')

                    if valid:
                        order['order_type'] = 'market'
                        args = order.values()

                        from trading.tasks import send_create_order
                        send_create_order.delay(*args, then_rebalance=False)

    # Market close position
    def market_close(self):
        #
        for code, value in self.balances.position.open.T.items():
            amount = abs(value['quantity'])
            if not np.isnan(amount):

                log.info('Close position {0}'.format(code))

                price = self.balances.price.spot.bid
                value = amount * price
                valid, order = self.prep_order('spot', code, amount, value, price, 'close_short', 'sell')

                if valid:
                    order['order_type'] = 'market'
                    args = order.values()

                    from trading.tasks import send_create_order
                    send_create_order.delay(*args, then_rebalance=False)

    # Return True if balances df is updated
    def is_fresh_balances(self):
        dt = self.balances_dt
        if dt:
            if dt == dt_aware_now(0):
                return True
            else:
                print(dt)
                print(dt_aware_now(0))
                return False
        else:
            return False


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
    clientid = models.CharField(max_length=150, null=True)  # order exchange's ID
    sender = models.CharField(max_length=10, null=True, choices=(('app', 'app'), ('user', 'user')))
    status, type = [models.CharField(max_length=150, null=True) for i in range(2)]
    amount, remaining, max_qty = [models.FloatField(max_length=10, null=True) for i in range(3)]
    filled = models.FloatField(max_length=10, null=True, default=0)
    side = models.CharField(max_length=10, null=True, choices=(('buy', 'buy'), ('sell', 'sell')))
    cost = models.FloatField(null=True)
    action = models.CharField(max_length=20, null=True)
    average, price = [models.FloatField(null=True, blank=True) for i in range(2)]
    fee, trades, params, response = [models.JSONField(null=True) for i in range(4)]
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
