import ccxt
from django.db import models
from django.conf import settings
from django.core.validators import MaxValueValidator, MinValueValidator
from django.utils import timezone
from django.db.models import Q, Avg, Sum
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
from gibberish import Gibberish
import json

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

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
    busy = models.BooleanField(null=True, blank=False, default=False)
    order_type = models.CharField(max_length=10, null=True, choices=(('limit', 'limit'), ('market', 'market')),
                                  default='limit')
    limit_price_tolerance = models.DecimalField(default=0, max_digits=4, decimal_places=3)
    email = models.EmailField(max_length=100, blank=True)
    api_key, api_secret = [models.CharField(max_length=100, blank=True) for i in range(2)]
    password = models.CharField(max_length=100, null=True, blank=True)
    leverage = models.DecimalField(
        default=1,
        max_digits=2, decimal_places=1,
        validators=[
            MaxValueValidator(2),
            MinValueValidator(0)
        ]
    )
    dt_created = models.DateTimeField(null=True)
    dt_modified = models.DateTimeField(null=True)
    pseudonym = models.CharField(max_length=30, null=True, blank=True)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True
    )

    class Meta:
        verbose_name_plural = "Accounts"

    def save(self, *args, **kwargs):
        if not self.pseudonym:
            self.pseudonym = pseudo_generator(1)[0].title()
        if not self.pk:
            self.dt_created = timezone.now()
        self.dt_modified = timezone.now()
        return super(Account, self).save(*args, **kwargs)

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

    # Check coins of the strategy and quote are present
    def add_missing_coin(self):

        codes = self.strategy.get_codes()
        codes.append(self.quote)
        for code in list(set(codes)):
            if code not in self.balances.index.tolist():
                self.balances.loc[code, ('spot', 'total', 'quantity')] = np.nan

    # Insert bid/ask of assets in spot wallet
    def get_spot_prices(self, update=False):

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
                    log.warning('Unable to select spot price of market {0}/{1}'.format(currency.code, self.quote))
                    if currency.code not in self.strategy.get_codes():
                        log.info('Asset {0} should be sold by the user in another market'.format(currency.code))
                else:
                    # Insert prices
                    self.balances.loc[code, ('price', 'spot', 'bid')] = bid
                    self.balances.loc[code, ('price', 'spot', 'ask')] = ask

        self.save()

    # Insert bid/ask of assets in future wallet
    def get_futu_prices(self, update=False):

        codes = self.balances.spot.total.quantity.index.tolist()
        for code in codes:
            market, flip = self.exchange.get_perp_market(code, self.quote)
            price = market.get_latest_price('last') if market else np.nan
            self.balances.loc[code, ('price', 'future', 'last')] = price

        self.save()

    # Convert quantity in dollar in balances dataframe
    def calculate_assets_value(self):

        # Iterate through wallets, free, used and total quantities
        for wallet in self.exchange.get_wallets():
            for tp in ['free', 'total', 'used']:
                for coin, value in self.balances[wallet][tp]['quantity'].dropna().items():

                    if coin == self.quote:
                        price = 1
                    else:
                        price = self.balances.price.spot['bid'][coin]

                    # Calculate value
                    value = price * value
                    self.balances.loc[coin, (wallet, tp, 'value')] = value

        self.save()

    # Drop dust coins
    def drop_dust_coins(self):

        # Keep assets with more than $10
        nodust = self.balances.loc[:, (['spot', 'future'], 'total', 'value')].sum(axis=1) > 10
        nodust = nodust[nodust].index.tolist()

        # Keep asset with an opened position
        if self.has_opened_short():
            posidx = self.balances.position.open.value.dropna().index.tolist()
        else:
            posidx = []

        # Keep assets from our strategy and quote
        strat = self.strategy.get_codes()
        strat.append(self.quote)

        keep = list(set(posidx + nodust + strat))
        self.balances = self.balances.loc[keep, :]
        self.save()

    # Check and reorder columns
    def check_columns(self):
        for i in ['total', 'free', 'used']:
            for wallet in ['spot', 'future']:
                if (wallet, i, 'value') not in self.balances.columns:
                    self.balances[(wallet, i, 'value')] = np.nan

        self.balances.sort_index(1, inplace=True)
        self.save()

    # Return account total value
    def assets_value(self):

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

    # Return positions pnl
    def positions_pnl(self):
        if self.has_opened_short():
            pos_val = self.balances.position.open.unrealized_pnl.dropna().sum()
        else:
            pos_val = 0
        return pos_val

    # Sum assets value with position PnL
    def account_value(self):
        if self.has_opened_short():
            return self.assets_value() + self.positions_pnl()
        else:
            return self.assets_value()

    # Create columns with targets
    def get_target(self):

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
            # if self.has_opened_short():
            #     value = (self.assets_value() + self.positions_pnl()) * target_pct
            # else:
            value = self.assets_value() * target_pct

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

            self.save()

    # Calculate net exposure and delta
    def calculate_delta(self):
        #
        if 'account' in self.balances.columns.get_level_values(0):

            # Refresh instance
            self.refresh_from_db()

            target = self.balances.account.target.quantity.dropna()
            assets_v = self.assets_value()

            if self.has_opened_short():
                position_v = self.positions_pnl()
                acc_value = assets_v + position_v
            else:
                acc_value = assets_v

            log.info(' ')
            log.info('Total value of account is {0} {1}'.format(round(acc_value, 1), self.quote))
            log.info('---Assets is {0} {1}'.format(round(assets_v, 1), self.quote))

            if self.has_opened_short():
                log.info('---Position PnL is {0} {1}'.format(round(position_v, 1), self.quote))

            # Determine synthetic cash of each coin
            if 'position' in self.balances.columns.get_level_values(0):
                spot = self.balances.spot.total.quantity
                posi = abs(self.balances.position.open.quantity)
                self.balances.loc[:, ('account', 'current', 'synthetic_cash')] = ((posi + spot) - posi).drop(self.quote)

            # Determine the total exposure of each coin
            mask = self.balances.columns.isin([('spot', 'total', 'quantity'),
                                               ('future', 'total', 'quantity'),
                                               ('position', 'open', 'quantity')])

            exposure = self.balances.loc[:, mask].dropna(axis=1, how='all').sum(axis=1)
            if self.has_opened_short():
                pos_value = self.balances.position.open.value.dropna().sum()
                exposure[self.quote] = max(0, exposure[self.quote] - abs(pos_value))

            self.balances.loc[:, ('account', 'current', 'exposure')] = exposure

            # Determine the percentage allocated to each coin
            for coin, exp in self.balances.account.current.exposure.items():
                bid = self.balances.price.spot.bid[coin]
                exposure_value = exp * bid

                if not np.isnan(exposure_value):
                    log.info('Total exposure of {0} is {1} {2}'.format(coin, round(exposure_value, 1), self.quote))

                percent = exposure_value / acc_value

                self.balances.loc[coin, ('account', 'current', 'percent')] = percent

            # Determine the net value allocated to each coin
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

            self.save()
        else:
            raise Exception('Dataframe does not has a column for account data, please run get_target()')

    # Return a list of codes to sell
    def codes_to_sell(self):
        delta = self.balances.account.target.delta
        sell = delta.loc[delta > 0].index.values.tolist()
        return [i for i in sell if i != self.quote]

    # Return a list of codes to sell in spot
    def codes_to_sell_spot(self):
        codes = list(set(self.codes_to_sell() + self.codes_synthetic_cash()))
        return [c for c in codes if self.has_spot_asset('free', c)]

    # Return a list of codes to open short
    def codes_to_open_short(self):
        return [c for c in self.codes_to_sell() if c not in self.codes_to_sell_spot()]

    # Return a list of codes to buy
    def codes_to_buy(self):
        delta = self.balances.account.target.delta
        buy = delta.loc[delta < 0].index.values.tolist()
        return [i for i in buy if i != self.quote]

    # Return a list of codes to long
    def codes_long(self):
        target = self.balances.account.target.quantity
        return [c for c in target.loc[target > 0].index.values.tolist() if c != self.quote]

    # Return a list of codes to short
    def codes_short(self):
        target = self.balances.account.target.quantity
        return [c for c in target.loc[target < 0].index.values.tolist() if c != self.quote]

    # Return a list of codes with simultaneous spot and short exposure
    def codes_synthetic_cash(self):
        if ('account', 'current', 'synthetic_cash') in self.balances.columns:
            return self.balances.account.current.synthetic_cash.dropna().index.tolist()
        else:
            return []

    # Return True is account has more than $10 of an asset in spot wallet
    def has_spot_asset(self, key, code=None):
        codes = self.balances.spot[key]['quantity'].dropna().index.tolist()
        if codes:
            if code:
                if code in codes:
                    if self.balances.spot[key]['value'][code] > 10:
                        return True
                    else:
                        return False
                else:
                    return False
            else:
                return True
        else:
            return False

    # Return True is account has asset in future wallet
    def has_future_asset(self, code=None):
        if ('future', 'total', 'quantity') in self.balances.columns:
            codes = self.balances.future.total['quantity'].dropna().index.tolist()
            if codes:
                if code:
                    if code in codes:
                        if code in self.balances.future.total.quantity.dropna().index:
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
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
            elif len(self.balances.position.open.quantity.dropna().index.tolist()):
                return True
            else:
                return False
        else:
            return False

    # Return absolute positions value
    def position_abs_value(self):
        if self.has_opened_short():
            return abs(self.balances.position.open.value.dropna()).sum()
        else:
            return 0

    # Return free margin
    def free_margin(self):
        if ('future', 'total', 'value') in self.balances.columns.tolist():
            total = self.balances.future.total.quantity[self.quote]
            if np.isnan(total):
                total = 0
            return max(0, total - self.position_abs_value())
        else:
            return 0

    # Validate order size and cost
    def validate_order(self, wallet, side, code, qty, price, action=None):

        log.info('Validate order to {0} {1} in {2}'.format(side, code, wallet))

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
                cost = size * price
                min_notional = limit_cost(market, cost)
                reduce_only = False

                # If cost not satisfied and close short
                # set reduce_only = True
                if not min_notional:
                    if market.exchange.exid == 'binance':
                        if market.type == 'derivative':
                            if market.margined.code == self.quote:
                                if action == 'close_short':
                                    return True, size, True
                                else:
                                    log.info('Cost not satisfied for {2} {1} {0}'
                                             .format(wallet, market.base.code, size), action=action)
                                    return False, size, False
                            else:
                                log.info('Cost not satisfied for {2} {1} {0}'
                                         .format(wallet, market.base.code, size), margined=market.margined.code)
                                return False, size, False
                        else:
                            log.info('Cost not satisfied for {2} {1} {0}'
                                     .format(wallet, market.base.code, size), type=market.type)
                            return False, size, False
                    else:
                        log.info('Cost not satisfied for {2} {1} {0}'
                                 .format(wallet, market.base.code, size), exid=market.exchange.exid)
                        return False, size, False
                else:
                    return True, size, reduce_only
            else:
                log.info('Condition not satisfied ({0} {1})'.format(size, code))
                return False, size, False

        else:
            log.error('Unable to validate order, market {0}/{1} not found'.format(code, self.quote))
            return False, qty, False

    # Create order object
    def create_object(self, wallet, code, side, action, qty, price):

        # Select market
        if wallet == 'spot':
            market, flip = self.exchange.get_spot_market(code, self.quote)
        elif wallet == 'future':
            market, flip = self.exchange.get_perp_market(code, self.quote)

        # Generate order_id
        alphanumeric = 'abcdefghijklmnopqrstuvwABCDEFGHIJKLMNOPQRSTUVWWXYZ01234689'
        clientid = ''.join((random.choice(alphanumeric)) for x in range(5))

        Order.objects.create(
            account=self,
            strategy=self.strategy,
            market=market,
            clientid=clientid,
            type=self.order_type,
            filled=0,
            side=side,
            action=action,
            amount=qty,
            price=price,
            cost=qty * price,
            status='preparation',
            sender='app'
        )

        return clientid

    # Update order object after an order is placed
    def update_order_object(self, wallet, response, new=False):
        #
        orderid = response['id']
        status = response['status'].lower()
        clientid = response['clientOrderId']

        log.bind(account=self.name)

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
                return

            else:
                # Set orderID assigned by exchange
                order.orderid = orderid

        finally:

            if new:
                log.info(' ')
                log.info('Update order {0}'.format(order.clientid))
                log.info('------------------')

            # Get traded amount
            filled_prev = order.filled
            filled_total = response['filled']

            # Determine new trade
            if filled_total > filled_prev:

                filled_new = filled_total - filled_prev
                log.info('> Trade of {0} {1} detected'.format(round(filled_new, 4), order.market.base.code))

                if filled_total < order.amount:
                    log.info('> Order {0} is partially filled'.format(order.clientid))
                else:
                    log.info('> Order {0} is filled'.format(order.clientid))

            else:
                filled_new = 0

            order.response = response
            order.status = status
            order.price = response['price']
            order.cost = response['cost']
            order.response = response
            order.average = response['average']
            order.fee = response['fee']
            order.remaining = response['remaining']
            order.filled = filled_total
            order.save()

            if filled_new:
                log.info('> Update status to {0}'.format(status))
                return filled_new, order.average

            else:
                return False, False

    # Offset transfer
    def offset_transfer(self, source, destination, amount, transfer_id):

        # Refresh dataframe
        self.refresh_from_db()

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

        # Restore nan
        self.balances.replace(0, np.nan, inplace=True)
        self.save()

    # Offset quantity after a trade
    def offset_order_filled(self, clientid, code, action, filled, average):

        log.info('Offset trade for order {0}'.format(clientid))

        offset = self.balances.copy()

        for col in offset.columns.get_level_values(0).unique().tolist():
            offset.loc[:, col] = np.nan

        # Determine trade value
        filled_value = filled * average

        log.info('Offset {0} {1} {2}'.format(action.title().replace('_', ' '), round(filled, 4), code))
        log.info('Offset trade value of {0} {1}'.format(round(filled_value, 1), self.quote))

        if action == 'buy_spot':
            offset.loc[code, ('spot', 'free', 'quantity')] = filled
            offset.loc[code, ('spot', 'free', 'value')] = filled_value
            offset.loc[code, ('spot', 'total', 'quantity')] = filled
            offset.loc[code, ('spot', 'total', 'value')] = filled_value

            offset.loc[self.quote, ('spot', 'total', 'quantity')] = -filled_value
            offset.loc[self.quote, ('spot', 'total', 'value')] = -filled_value
            offset.loc[self.quote, ('spot', 'used', 'quantity')] = -filled_value
            offset.loc[self.quote, ('spot', 'used', 'value')] = -filled_value

            offset.loc[code, ('account', 'current', 'exposure')] = filled
            offset.loc[code, ('account', 'current', 'value')] = filled_value
            offset.loc[self.quote, ('account', 'current', 'exposure')] = -filled_value
            offset.loc[self.quote, ('account', 'current', 'value')] = -filled_value
            offset.loc[code, ('account', 'target', 'delta')] = filled

        if action == 'sell_spot':
            offset.loc[code, ('spot', 'free', 'quantity')] = -filled
            offset.loc[code, ('spot', 'free', 'value')] = -filled_value
            offset.loc[code, ('spot', 'used', 'quantity')] = -filled
            offset.loc[code, ('spot', 'used', 'value')] = -filled_value
            offset.loc[code, ('spot', 'total', 'quantity')] = -filled
            offset.loc[code, ('spot', 'total', 'value')] = -filled_value

            offset.loc[self.quote, ('spot', 'total', 'quantity')] = filled_value
            offset.loc[self.quote, ('spot', 'total', 'value')] = filled_value
            offset.loc[self.quote, ('spot', 'free', 'quantity')] = filled
            offset.loc[self.quote, ('spot', 'free', 'value')] = filled_value

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

            offset.loc[code, ('position', 'open', 'quantity')] = -filled
            offset.loc[code, ('position', 'open', 'value')] = -filled_value

            # Offset position size and value
            offset.loc[code, ('account', 'current', 'exposure')] = -filled
            offset.loc[code, ('account', 'current', 'value')] = -filled_value
            offset.loc[code, ('account', 'target', 'delta')] = -filled

            # Create columns if needed
            if ('position', 'open', 'quantity') not in self.balances.columns:

                log.info('Create missing columns in position header')

                self.balances.loc[code, ('position', 'open', 'quantity')] = 0
                self.balances.loc[code, ('position', 'open', 'value')] = 0
                self.balances.loc[code, ('position', 'open', 'unrealized_pnl')] = 0
                self.save()

        if action == 'close_long':
            # Offset position size and value
            offset.loc[code, ('position', 'open', 'quantity')] = -filled
            offset.loc[code, ('position', 'open', 'value')] = -filled_value

            offset.loc[code, ('account', 'current', 'exposure')] = -filled
            offset.loc[code, ('account', 'current', 'value')] = -filled_value
            offset.loc[code, ('account', 'target', 'delta')] = -filled

        pct = self.balances.account.current.percent[code] * 100
        exp = self.balances.account.current.exposure[code]
        val = self.balances.account.current.value[code]
        dta = self.balances.account.target.delta[code]
        log.info('')
        log.info('Percenta for {0} was {1}%'.format(code, round(pct, 1)))
        log.info('Quantity for {0} was {1}'.format(code, round(exp, 4)))
        log.info('Value___ for {0} was {1}'.format(code, round(val, 1)))
        log.info('Delta___ for {0} was {1}'.format(code, round(dta, 4)))

        # Create offset and update dataframe
        offset = offset.dropna(axis=0, how='all').dropna(axis=1, how='all')

        # Refresh dataframe
        self.refresh_from_db()
        updated = self.balances.loc[offset.index, offset.columns].fillna(0) + offset
        self.balances.loc[offset.index, offset.columns] = updated

        # Restore nan
        self.balances.replace(0, np.nan, inplace=True)

        # Update new percentage
        account_value = self.account_value()
        for c in [code, self.quote]:
            exp = self.balances.account.current.value[c]
            dta = self.balances.account.target.delta[c]
            qty = self.balances.account.current.exposure[c]
            pct = exp / account_value
            self.balances.loc[c, ('account', 'current', 'percent')] = pct
            if c is not self.quote:
                log.info('Percenta for {0} is now {1}%'.format(c, round(pct * 100, 1)))
                log.info('Quantity for {0} is now {1}'.format(code, round(qty, 1)))
                log.info('Value___ for {0} is now {1}'.format(c, round(exp, 1)))
                log.info('Delta___ for {0} is now {1}'.format(c, round(dta, 4)))

        self.save()

    # Offset used resources after an order is opened
    def offset_order_new(self, code, action, qty, val):

        log.info('Offset used and free resources')

        # Refresh dataframe
        self.refresh_from_db()

        offset = self.balances.copy()
        for col in offset.columns.get_level_values(0).unique().tolist():
            offset.loc[:, col] = np.nan

        if action == 'buy_spot':

            log.info('Offset used and free quantity of {0} {1}'.format(round(qty, 1), self.quote))
            log.info('Offset used and free value of {0} {1}'.format(round(val, 1), self.quote))

            # Offset order value from free and used quote
            offset.loc[self.quote, ('spot', 'free', 'quantity')] = -val
            offset.loc[self.quote, ('spot', 'free', 'value')] = -val
            offset.loc[self.quote, ('spot', 'used', 'quantity')] = val
            offset.loc[self.quote, ('spot', 'used', 'value')] = val

        if action == 'sell_spot':

            log.info('Offset used and free quantity of {0} {1}'.format(round(qty, 4), code))
            log.info('Offset used and free value of {0} {1}'.format(round(val, 1), self.quote))

            # Offset order quantity and value from free and used code
            offset.loc[code, ('spot', 'free', 'quantity')] = -qty
            offset.loc[code, ('spot', 'free', 'value')] = -val
            offset.loc[code, ('spot', 'used', 'quantity')] = qty
            offset.loc[code, ('spot', 'used', 'value')] = val

        if action == 'close_short':
            return

        if action == 'open_short':
            margin_value = val / 20

            log.info('Offset margin used of {0} {1}'.format(round(margin_value, 4), self.quote))

            # Offset margin value from used and free quote
            offset.loc[self.quote, ('future', 'free', 'quantity')] = -margin_value
            offset.loc[self.quote, ('future', 'free', 'value')] = -margin_value
            offset.loc[self.quote, ('future', 'used', 'quantity')] = margin_value
            offset.loc[self.quote, ('future', 'used', 'value')] = margin_value

        if action == 'close_long':
            return

        # Create offset and update dataframe
        offset = offset.dropna(axis=0, how='all').dropna(axis=1, how='all')
        updated = self.balances.loc[offset.index, offset.columns].fillna(0) + offset
        self.balances.loc[offset.index, offset.columns] = updated

        # Restore nan
        self.balances.replace(0, np.nan, inplace=True)
        self.save()

    # Offset a cancelled order
    def offset_order_cancelled(self, code, side, qty, val, filled=0):
        pass

    # Return sum of open orders size
    def get_open_orders_spot(self, code, side, action):

        now = dt_aware_now()
        start = dt_aware_now(0)

        # Test if a close_spot or a buy_spot order is open
        market, flip = self.exchange.get_spot_market(code, self.quote)
        qs = Order.objects.filter(account=self,
                                  status__in=['open', 'preparation'],
                                  market=market,
                                  side=side,
                                  action=action,
                                  dt_created__range=[start, now]
                                  )
        if not qs.exists():
            # log.info('No pending order found for {0} in spot'.format(code))
            return 0
        else:

            log.info('Found {0} open orders in {1} spot'.format(qs.count(), market.symbol))

            for order in qs:
                log.info('> Order {0} to {1} {2}'.format(order.clientid, order.action.replace('_', ' '),
                                                         market.base.code))
                log.info('> Order {0} filled {1}/{2}'.format(order.clientid, order.filled, order.amount), flip=flip)

            qs_amount = qs.aggregate(Sum('amount'))['amount__sum']
            qs_price = qs.aggregate(Avg('price'))['price__avg']

            log.info('> Total order size is {0} {1}'.format(qs_amount, market.base.code))

            if flip:
                return qs_amount / qs_price
            else:
                return qs_amount

    # Return sum of open orders size
    def get_open_orders_futu(self, code, side, action):

        now = dt_aware_now()
        start = dt_aware_now(0)

        # Test if a close_spot or a buy_spot order is open
        market, flip = self.exchange.get_perp_market(code, self.quote)
        qs = Order.objects.filter(account=self,
                                  status__in=['open', 'preparation'],
                                  market=market,
                                  side=side,
                                  action=action,
                                  dt_created__range=[start, now]
                                  )
        if not qs.exists():
            # log.info('No pending order found for {0} in future'.format(code))
            return 0
        else:

            log.info('Found {0} open orders in {1} future'.format(qs.count(), market.symbol))

            for order in qs:
                log.info('> Order {0} to {1} {2}'.format(order.clientid, order.action.replace('_', ' '),
                                                         market.base.code))
                log.info('> Order {0} filled {1}/{2}'.format(order.clientid, order.filled, order.amount), flip=flip)

            qs_amount = qs.aggregate(Sum('amount'))['amount__sum']
            qs_price = qs.aggregate(Avg('price'))['price__avg']

            log.info('> Total order amount is {0} {1}'.format(qs_amount, market.base.code))

            if flip:
                return qs_amount / qs_price
            else:
                return qs_amount

    # Return True if all markets needed to synchronize the account are update
    def is_tradable(self):

        symbols_long = [c + '/' + self.quote for c in self.strategy.get_codes_long()]
        symbols_short = [c + '/' + self.quote for c in self.strategy.get_codes_short()]
        update = []

        for symbol in symbols_long:
            market = Market.objects.get(exchange=self.exchange,
                                        symbol=symbol,
                                        type='spot'
                                        )
            update.append(market.is_updated())

        for symbol in symbols_short:
            market = Market.objects.get(exchange=self.exchange,
                                        symbol=symbol,
                                        type='derivative',
                                        contract_type='perpetual'
                                        )
            update.append(market.is_updated())

        if False in update:
            return False
        else:
            return True


class Asset(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='asset', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='asset', null=True)
    currency = models.ForeignKey(Currency, on_delete=models.SET_NULL, related_name='asset', null=True)
    wallet = models.CharField(null=True, max_length=10, blank=True)
    total = models.FloatField(null=True)
    total_value = models.FloatField(null=True)
    free = models.FloatField(null=True)
    used = models.FloatField(null=True)
    weight = models.FloatField(null=True)
    dt_response = models.DateTimeField(null=True)
    dt_modified = models.DateTimeField(null=True)
    dt_created = models.DateTimeField(null=True)

    class Meta:
        verbose_name_plural = "Assets"

    def save(self, *args, **kwargs):

        if self.pk is None:
            self.dt_created = timezone.now()
        self.dt_modified = timezone.now()
        super(Asset, self).save(*args, **kwargs)

    def __str__(self):
        return str(self.currency.code)


class Fund(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='funds', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='funds', null=True)
    historical_balance = models.JSONField(null=True)
    dt = models.DateTimeField(null=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)
    owner = models.ForeignKey(
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
    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE, related_name='order', null=True)
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
    dt_created = models.DateTimeField(null=True)
    dt_modified = models.DateTimeField(null=True)
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        null=True
    )

    class Meta:
        verbose_name_plural = "Orders"

    def save(self, *args, **kwargs):
        if not self.pk:
            self.dt_created = timezone.now()

        self.dt_modified = timezone.now()
        return super(Order, self).save(*args, **kwargs)

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
    entry_price = models.FloatField(null=True)
    max_qty = models.FloatField(null=True)
    notional_value, initial_margin, maint_margin, order_initial_margin = [models.FloatField(null=True) for i in
                                                                          range(4)]
    realized_pnl, unrealized_pnl = [models.FloatField(null=True) for i in range(2)]
    instrument_id, side = [models.CharField(max_length=150, null=True) for i in range(2)]
    margin_mode = models.CharField(max_length=10, null=True)
    leverage = models.IntegerField(null=True)
    response = models.JSONField(null=True)
    dt_created = models.DateTimeField(null=True)
    dt_modified = models.DateTimeField(null=True)

    class Meta:
        verbose_name_plural = "Positions"

    def __str__(self):
        return str(self.pk)

    def save(self, *args, **kwargs):
        if not self.pk:
            self.dt_created = timezone.now()
        self.dt_modified = timezone.now()
        return super(Position, self).save(*args, **kwargs)


class Stat(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='stats', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name='stats', null=True)
    strategy = models.ForeignKey(Strategy, on_delete=models.CASCADE, related_name='stats', null=True)
    metrics = models.JSONField(null=True)
    dt_created = models.DateTimeField(null=True)
    dt_modified = models.DateTimeField(null=True)

    class Meta:
        verbose_name_plural = "Statistics"

    def __str__(self):
        return str(self.pk)

    def save(self, *args, **kwargs):
        if not self.pk:
            self.dt_created = timezone.now()
        self.dt_modified = timezone.now()
        return super(Stat, self).save(*args, **kwargs)
