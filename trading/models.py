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
from billiard.process import current_process

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
    def get_assets_balances(self):
        #
        log.info('Get assets balance')

        # Reset attribute
        self.balances = pd.DataFrame()
        self.save()

        client = self.exchange.get_ccxt_client(self)

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

        # Timestamp index name
        dt = datetime.now()
        now = dt.strftime(datetime_directive_s)
        self.balances.index.set_names(now, inplace=True)

        self.save()

        log.info('Get assets balance complete')

    # Fetch and update open positions in balances dataframe
    def get_open_positions(self):

        log.info('Get open positions')

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

        self.save()
        log.info('Get open positions complete')

    # Insert bid/ask of spot markets
    def get_spot_prices(self, update=False):
        #
        action = 'Update' if update else 'Get'
        log.info(' ')
        log.info('{0} spot prices'.format(action))

        codes = self.balances.spot.total.quantity.index.tolist()
        for code in codes:

            try:
                currency = Currency.objects.get(code=code)

            except ObjectDoesNotExist:

                # log.error('Spot market {0}/{1} not found'.format(code, self.quote))

                self.balances.loc[code, ('price', 'spot', 'bid')] = np.nan
                self.balances.loc[code, ('price', 'spot', 'ask')] = np.nan

            else:
                try:
                    bid, ask = currency.get_latest_price(self.exchange, self.quote, ['bid', 'ask'])
                except TypeError as e:
                    log.error('{0}'.format(str(e)))
                    raise Exception('Unable to select spot price of {0}/{1}'.format(currency.code, self.quote))
                else:
                    # Insert prices
                    self.balances.loc[code, ('price', 'spot', 'bid')] = bid
                    self.balances.loc[code, ('price', 'spot', 'ask')] = ask

        self.save()
        log.info('{0} spot prices complete'.format(action))

    # Insert bid/ask of future markets
    def get_futu_prices(self, update=False):
        #
        action = 'Update' if update else 'Get'
        log.info('{0} future prices'.format(action))

        codes = self.balances.spot.total.quantity.index.tolist()
        for code in codes:
            market, flip = self.exchange.get_perp_market(code, self.quote)
            price = market.get_latest_price('last') if market else np.nan
            self.balances.loc[code, ('price', 'future', 'last')] = price

        self.save()
        log.info('{0} future prices complete'.format(action))

    # Convert quantity in dollar in balances dataframe
    def calculate_assets_value(self):
        #
        log.info('Calculate assets value')

        # Iterate through wallets, free, used and total quantities
        for wallet in self.exchange.get_wallets():
            for tp in ['free', 'total', 'used']:
                for coin, value in self.balances[wallet][tp]['quantity'].items():

                    if coin == self.quote:
                        price = 1
                    else:
                        price = self.balances.price.spot['bid'][coin]

                    # Calculate value
                    value = price * value
                    self.balances.loc[coin, (wallet, tp, 'value')] = value

        # Select coins with more than $1
        nodust = self.balances.loc[:, (['spot', 'future'], 'total', 'value')].sum(axis=1) > 1
        nodust = nodust[nodust].index.tolist()
        self.balances = self.balances.loc[nodust, :]

        # add strategy coins and quote if missing
        codes = self.strategy.get_codes()
        codes.append(self.quote)
        for code in list(set(codes)):
            if code not in self.balances.index.tolist():
                self.balances.loc[code] = np.nan

        # Create missing value columns
        for i in ['total', 'free', 'used']:
            for wallet in ['spot', 'future']:
                if (wallet, i, 'value') not in self.balances.columns:
                    self.balances[(wallet, i, 'value')] = np.nan

        # reorder columns
        self.balances.sort_index(1, inplace=True)
        self.save()

        log.info('Calculate assets value complete')

    # Return account total value
    def account_value(self):

        if ('spot', 'total', 'value') in self.balances.columns:
            spot_val = self.balances.spot.total.value.sum()
            if np.isnan(spot_val):
                spot_val = 0
        else:
            spot_val = 0

        if ('future', 'total', 'value') in self.balances.columns:
            futu_val = self.balances.future.total.value.dropna().sum()
        else:
            futu_val = 0

        # Sum wallets
        return spot_val + futu_val

    # Create columns with targets
    def get_target(self):
        #
        log.info('Get target weights')

        try:

            # Insert percentage
            target_pct = self.strategy.load_targets()

            if self.quote == 'BUSD':

                # Set BUSD as strategy stablecoin
                i = target_pct.index.tolist()
                i = [self.quote if x == 'USDT' else x for x in i]
                target_pct.set_axis(i, inplace=True)

            for coin, pct in target_pct.items():
                self.balances.loc[coin, ('account', 'target', 'percent')] = pct

            # Determine values
            value = self.account_value() * target_pct

            for coin, val in value.items():
                self.balances.loc[coin, ('account', 'target', 'value')] = val

                # Determine quantity
                qty = val / self.balances.price.spot.bid[coin]
                self.balances.loc[coin, ('account', 'target', 'quantity')] = qty

        except AttributeError as e:
            self.trading = False
            raise Exception('Unable to get targets weights {0}'.format(e.__class__.__name__))

        except ValueError as e:
            self.trading = False
            raise Exception('Unable to get targets weights {0}'.format(e.__class__.__name__))

        finally:

            log.info('Get target weights complete')
            self.save()

    # Calculate net exposure and delta
    def calculate_delta(self):
        #
        log.info('Calculate delta')

        target = self.balances.account.target.quantity.dropna()
        acc_value = self.account_value()

        log.info('Total value of account is {0} {1}'.format(round(acc_value, 1), self.quote))

        #  Select columns with assets quantities
        mask = self.balances.columns.isin([('spot', 'total', 'quantity'),
                                           ('future', 'total', 'quantity'),
                                           ('position', 'open', 'quantity')])
        # Determine total exposure
        exposure = self.balances.loc[:, mask].dropna(axis=1, how='all').sum(axis=1)

        if 'position' in self.balances.columns.get_level_values(0):
            pos_value = self.balances.position.open.value.dropna().sum()
            exposure[self.quote] = max(0, exposure[self.quote] - abs(pos_value))

        self.balances.loc[:, ('account', 'current', 'exposure')] = exposure

        # Calculate percentage for each coin
        for coin, exp in self.balances.account.current.exposure.items():
            bid = self.balances.price.spot.bid[coin]
            exposure_value = exp * bid

            log.info('Total exposure of {0} is {1} {2}'.format(coin, round(exposure_value, 1), self.quote))

            percent = exposure_value / acc_value

            self.balances.loc[coin, ('account', 'current', 'percent')] = percent

        # Calculate value allocated to each coin
        for coin, exp in self.balances.account.current.exposure.items():
            bid = self.balances.price.spot.bid[coin]
            self.balances.loc[coin, ('account', 'current', 'value')] = exp * bid

        # Iterate through target coins and calculate delta
        for coin in target.index.tolist():

            # Coins already in account ?
            if coin in self.balances.index.tolist():
                qty = self.balances.loc[coin, ('account', 'current', 'exposure')]
                self.balances.loc[coin, ('account', 'target', 'delta')] = qty - target[coin]

            # Coins not in account ?
            else:
                self.balances.loc[coin, ('account', 'target', 'delta')] = -target[coin]

        # Iterate through coins in account and calculate delta
        for coin in self.balances.index.tolist():

            # Coin not in target ?
            if coin not in target.index.tolist():
                qty = self.balances.loc[coin, ('account', 'current', 'exposure')]
                self.balances.loc[coin, ('account', 'target', 'delta')] = qty
                self.balances.loc[coin, ('account', 'target', 'quantity')] = 0
                self.balances.loc[coin, ('account', 'target', 'percent')] = 0
                self.balances.loc[coin, ('account', 'target', 'value')] = 0

        log.info('Calculate delta complete')
        self.save()

    # Return a list of codes to sell
    def codes_to_sell(self):
        delta = self.balances.account.target.delta
        return [i for i in delta.loc[delta > 0].index.values.tolist() if i != self.quote]

    # Return a list of codes to buy
    def codes_to_buy(self):
        delta = self.balances.account.target.delta
        return [i for i in delta.loc[delta < 0].index.values.tolist() if i != self.quote]

    # Return True is account has asset in spot wallet
    def has_spot_asset(self, key, code=None):
        if ('spot', key, 'quantity') in self.balances.columns:
            if code:
                if self.balances.spot.total.value[code] > 1:
                    return True
                else:
                    return False
            else:
                return False

    # Return True is account has asset in future wallet
    def has_future_asset(self, code=None):
        if ('future', 'total', 'quantity') in self.balances.columns:
            if code:
                if code in self.balances.future.total.quantity.dropna().index:
                    return True
                else:
                    return False
            else:
                return False

    # Return True is account has opened short
    def has_opened_short(self, code=None):
        if ('position', 'open', 'quantity') in self.balances.columns:
            if code:
                if code in self.balances.position.open.quantity.dropna().index:
                    return True
                else:
                    return False
            else:
                return True
        else:
            return False

    # Return absolute positions value
    def position_abs_value(self):
        if self.has_opened_short:
            return abs(self.balances.position.open.value.dropna()).sum()
        else:
            return 0

    # Return free margin
    def free_margin(self):
        total = self.balances.future.total.quantity[self.quote]
        if np.isnan(total):
            total = 0
        return max(0, total - self.position_abs_value())

    # Validate order size and cost
    def validate_order(self, wallet, code, qty, cost, action=None):

        if wallet == 'spot':
            market, flip = self.exchange.get_spot_market(code, self.quote)
        else:
            market, flip = self.exchange.get_perp_market(code, self.quote)

        if market:

            # Format decimal
            size = format_decimal(counting_mode=self.exchange.precision_mode,
                                  precision=market.precision['amount'],
                                  n=qty)

            # Test amount limits MIN and MAX
            if limit_amount(market, size):

                # Test cost limits MIN and MAX
                min_notional = limit_cost(market, cost)
                reduce_only = False

                # If cost not satisfied and close short
                # set reduce_only = True
                if not min_notional:
                    if market.exchange.exid == 'binance':
                        if market.type == 'derivative':
                            if market.margined.code == self.quote:
                                if action == 'close_short':
                                    reduce_only = True

                    # Else return
                    if not reduce_only:
                        log.info(' ')
                        log.info('Cost not satisfied for {2} {1} {0}'.format(wallet, market.base.code, size))
                        return False, size, False

                return True, size, reduce_only

            else:
                log.info(' ')
                log.info('Condition not satisfied ({0} {1})'.format(round(size, 3), code))
                return False, size, False

        else:
            log.error('Unable to validate order of {0} {1}'.format(round(qty, 3), code))
            return False, qty, False

    # Create order object
    def create_object(self, wallet, code, side, action, qty):

        # Select market
        markets = Market.objects.filter(base__code=code, quote__code=self.quote, exchange=self.exchange)
        if wallet == 'spot':
            market = markets.get(type='spot')
        else:
            market = markets.get(type='derivative', contract_type='perpetual')

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
            amount=qty,
            status='preparation',
            sender='app'
        )

        return clientid

    # Update order object
    def update_order_object(self, wallet, response):
        #
        orderid = response['id']
        status = response['status'].lower()
        clientid = response['clientOrderId']

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
                    orderid=orderid,
                    status=status
                )

                log.warning('Cancel user order')
                from trading.tasks import send_cancel_order
                send_cancel_order.delay(self.id, orderid)
                # self.offset_order_cancelled()

                return

            else:
                # Set orderID assigned by exchange
                order.orderid = orderid

        finally:

            # Get traded amount
            filled_prev = order.filled
            filled_total = response['filled']

            # Determine new trade
            if filled_total > filled_prev:
                filled_new = filled_total - filled_prev
            else:
                filled_new = 0

            # Update attributes
            order.cost = response['cost']
            order.average = response['average']
            order.fee = response['fee']
            order.price = response['price']
            order.remaining = response['remaining']
            order.status = status
            order.filled = filled_total
            order.response = response
            order.save()

            log.info(' ')
            log.info('Update object')
            log.info('Order with clientID {0}'.format(order.clientid))
            log.info('Order with status {0}'.format(status))
            log.info('Order for code {0} ({1})'.format(order.market.base.code, order.market.wallet))
            log.info('Order to {0}'.format(order.action.replace('_', ' ')))
            log.info('Trade total {0}'.format(filled_total))
            log.info('Trade new {0}'.format(filled_new))

            return filled_new, order.average

    # Offset transfer
    def offset_transfer(self, source, destination, amount, transfer_id):

        log.bind(id=transfer_id)
        log.info(' ')
        log.info('Offset transfer')
        log.info('Offset transfer from {0} to {1}'.format(source, destination))
        log.info('Offset transfer amount is {0} {1}'.format(round(amount, 1), self.quote))

        offset = self.balances.copy()
        for col in offset.columns.get_level_values(0).unique().tolist():
            offset.loc[:, col] = np.nan

        # Offset in source
        offset.loc[self.quote, (source, 'free', 'quantity')] = -amount
        offset.loc[self.quote, (source, 'free', 'value')] = -amount
        offset.loc[self.quote, (source, 'total', 'quantity')] = -amount
        offset.loc[self.quote, (source, 'total', 'value')] = -amount

        # Offset in destination
        offset.loc[self.quote, (destination, 'free', 'quantity')] = amount
        offset.loc[self.quote, (destination, 'free', 'value')] = amount
        offset.loc[self.quote, (destination, 'total', 'quantity')] = amount
        offset.loc[self.quote, (destination, 'total', 'value')] = amount

        offset = offset.dropna(axis=0, how='all').dropna(axis=1, how='all')
        updated = self.balances.loc[offset.index, offset.columns].fillna(0) + offset
        self.balances.loc[offset.index, offset.columns] = updated
        self.save()

        log.info('Offset transfer complete')
        log.unbind('id')

    # Offset a new order
    def offset_order(self, code, action, qty, val, filled, average):

        log.info(' ')
        log.info('Offset order')
        log.info('Offset order to {0}'.format(action.replace('_', ' ')))

        offset = self.balances.copy()
        for col in offset.columns.get_level_values(0).unique().tolist():
            offset.loc[:, col] = np.nan

        # No trade yet
        if not filled:

            log.info('Offset used and free resources')
            log.info('Offset resources {0} {1}'.format(round(qty, 3), code))

            if action == 'buy_spot':

                # Offset order value from free and used quote
                offset.loc[self.quote, ('spot', 'free', 'quantity')] = -val
                offset.loc[self.quote, ('spot', 'free', 'value')] = -val
                offset.loc[self.quote, ('spot', 'used', 'quantity')] = val
                offset.loc[self.quote, ('spot', 'used', 'value')] = val

            if action == 'sell_spot':

                # Offset order quantity and value from free and used code
                offset.loc[code, ('spot', 'free', 'quantity')] = -qty
                offset.loc[code, ('spot', 'free', 'value')] = -val
                offset.loc[code, ('spot', 'used', 'quantity')] = qty
                offset.loc[code, ('spot', 'used', 'value')] = val

            if action == 'close_short':
                pass

            if action == 'open_short':

                margin_value = val / 20

                # Offset margin value from used and free quote
                offset.loc[self.quote, ('future', 'free', 'quantity')] = -margin_value
                offset.loc[self.quote, ('future', 'free', 'value')] = -margin_value
                offset.loc[self.quote, ('future', 'used', 'quantity')] = margin_value
                offset.loc[self.quote, ('future', 'used', 'value')] = margin_value

        else:

            # Determine trade value
            filled_value = filled * average

            log.info('Offset trade of {0} {1}'.format(round(filled, 3), code))
            log.info('Offset trade value of {0} {1}'.format(round(filled_value, 1), self.quote))

            if action == 'buy_spot':

                offset.loc[code, ('spot', 'free', 'quantity')] = filled
                offset.loc[code, ('spot', 'free', 'value')] = filled_value
                offset.loc[code, ('spot', 'total', 'quantity')] = filled
                offset.loc[code, ('spot', 'total', 'value')] = filled_value

                offset.loc[self.quote, ('spot', 'total', 'quantity')] = -filled_value
                offset.loc[self.quote, ('spot', 'total', 'value')] = -filled_value

                offset.loc[code, ('account', 'current', 'exposure')] = filled
                offset.loc[code, ('account', 'current', 'value')] = filled_value
                offset.loc[self.quote, ('account', 'current', 'exposure')] = -filled_value
                offset.loc[self.quote, ('account', 'current', 'value')] = -filled_value
                offset.loc[code, ('account', 'target', 'delta')] = filled

            if action == 'sell_spot':

                offset.loc[code, ('spot', 'free', 'quantity')] = -filled
                offset.loc[code, ('spot', 'free', 'value')] = -filled_value
                offset.loc[code, ('spot', 'total', 'quantity')] = -filled
                offset.loc[code, ('spot', 'total', 'value')] = -filled_value

                offset.loc[self.quote, ('spot', 'total', 'quantity')] = filled_value
                offset.loc[self.quote, ('spot', 'total', 'value')] = filled_value

                offset.loc[code, ('account', 'current', 'exposure')] = -filled
                offset.loc[code, ('account', 'current', 'value')] = -filled_value
                offset.loc[self.quote, ('account', 'current', 'exposure')] = filled_value
                offset.loc[self.quote, ('account', 'current', 'value')] = filled_value
                offset.loc[code, ('account', 'target', 'delta')] = -filled

            if action == 'close_short':

                # Offset position size and value
                offset.loc[code, ('position', 'open', 'quantity')] = filled
                offset.loc[code, ('position', 'open', 'value')] = filled_value

                offset.loc[code, ('account', 'current', 'exposure')] = filled
                offset.loc[code, ('account', 'current', 'value')] = filled_value
                offset.loc[code, ('account', 'target', 'delta')] = filled

            if action == 'open_short':

                # Offset position size and value
                offset.loc[code, ('position', 'open', 'quantity')] = -filled
                offset.loc[code, ('position', 'open', 'value')] = -filled_value

                offset.loc[code, ('account', 'current', 'exposure')] = -filled
                offset.loc[code, ('account', 'current', 'value')] = -filled_value
                offset.loc[code, ('account', 'target', 'delta')] = -filled

        offset = offset.dropna(axis=0, how='all').dropna(axis=1, how='all')
        updated = self.balances.loc[offset.index, offset.columns].fillna(0) + offset
        self.balances.loc[offset.index, offset.columns] = updated
        self.save()

        log.info('Offset complete')

    # Offset a cancelled order
    def offset_order_cancelled(self, code, side, qty, val, filled=0):
        pass

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
        if 'position' in self.balances.columns.get_level_values(0).tolist():

            for code in self.balances.position.open.quantity.dropna().index.tolist():

                log.info('Close position {0}'.format(code))

                amount = self.balances.position.open.quantity[code]
                side = 'buy' if amount < 0 else 'sell'
                amount = abs(amount)
                price = self.balances.price.spot.bid[code]
                value = amount * price
                valid, order = self.prep_order('future', code, amount, value, price, 'close_short', side)

                if valid:
                    order['order_type'] = 'market'
                    args = order.values()
                    from trading.tasks import send_create_order
                    send_create_order.delay(*args, then_rebalance=False)

        else:
            log.info('No position found')


class Fund(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='funds', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='funds', null=True)
    historical_balance = models.JSONField(null=True)
    dt = models.DateTimeField(null=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True,
        blank=True
    )

    class Meta:
        verbose_name_plural = "Funds"
        ordering = ['-dt_create']
        get_latest_by = 'dt_create'

    def __str__(self):
        return str(self.account.name)


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
    fee, trades, response = [models.JSONField(null=True) for i in range(3)]
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
        if self.clientid:
            return self.clientid
        else:
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
