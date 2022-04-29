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
                self.balances.loc[code] = np.nan

    # Insert bid/ask of assets in spot wallet
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
                    log.warning('Unable to select spot price of market {0}/{1}'.format(currency.code, self.quote))
                    if currency.code not in self.strategy.get_codes():
                        log.info('Asset {0} should be sold by the user in another market'.format(currency.code))
                else:
                    # Insert prices
                    self.balances.loc[code, ('price', 'spot', 'bid')] = bid
                    self.balances.loc[code, ('price', 'spot', 'ask')] = ask

        self.save()
        log.info('{0} spot prices complete'.format(action))

    # Insert bid/ask of assets in future wallet
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
                for coin, value in self.balances[wallet][tp]['quantity'].dropna().items():

                    if coin == self.quote:
                        price = 1
                    else:
                        price = self.balances.price.spot['bid'][coin]

                    # Calculate value
                    value = price * value
                    self.balances.loc[coin, (wallet, tp, 'value')] = value

        self.save()
        log.info('Calculate assets value complete')

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

            log.info('Get target weights complete')
            self.save()

    # Calculate net exposure and delta
    def calculate_delta(self):
        #
        log.info('Calculate delta')

        target = self.balances.account.target.quantity.dropna()

        # if self.has_opened_short():
        #     acc_value = self.assets_value() + self.positions_pnl()
        # else:
        acc_value = self.assets_value()

        log.info(' ')
        log.info('Total value of account is {0} {1}'.format(round(acc_value, 1), self.quote))

        #  Select columns with assets quantities
        mask = self.balances.columns.isin([('spot', 'total', 'quantity'),
                                           ('future', 'total', 'quantity'),
                                           ('position', 'open', 'quantity')])
        # Determine total exposure
        exposure = self.balances.loc[:, mask].dropna(axis=1, how='all').sum(axis=1)

        if self.has_opened_short():
            pos_value = self.balances.position.open.value.dropna().sum()
            exposure[self.quote] = max(0, exposure[self.quote] - abs(pos_value))

        self.balances.loc[:, ('account', 'current', 'exposure')] = exposure

        # Calculate percentage for each coin
        for coin, exp in self.balances.account.current.exposure.items():
            bid = self.balances.price.spot.bid[coin]
            exposure_value = exp * bid

            if not np.isnan(exposure_value):
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

    # # Return a list of codes to buy spot
    # def codes_to_buy_spot(self):
    #     long = self.balances.account.target.percent > 0  # codes to long
    #     return [c for c in self.codes_to_buy() if c in long[long].index.tolist()]  # codes to buy in spot
    #
    # # Return a list of codes to buy spot
    # def codes_to_sell_spot(self):
    #     spot = self.balances.spot.quantity.total.dropna().index.tolist()  # codes spot
    #     return [c for c in self.codes_to_sell() if c in spot]  # codes to sell in spot
    #
    # # Return a list of codes to buy spot
    # def codes_to_open_short(self):
    #     spot = self.balances.spot.quantity.total.dropna().index.tolist()  # codes spot
    #     short = self.balances.account.target.percent < 0  # codes to short
    #     short = short[short].index.tolist()
    #     return [c for c in self.codes_to_sell() if c in short and c not in spot]  # codes to sell and short not in spot

    # Return True is account has more than $10 of an asset in spot wallet
    def has_spot_asset(self, key, code=None):
        if ('spot', key, 'quantity') in self.balances.columns:
            codes = self.balances.spot[key]['quantity'].dropna().index.tolist()
            if code:
                if code in codes:
                    if self.balances.spot[key]['value'][code] > 10:
                        return True
                    else:
                        return False
                else:
                    return False
            else:
                return False
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
    def validate_order(self, wallet, code, qty, price, action=None):

        log.bind(account=self.name)
        log.info('Validate order {0} {1}'.format(code, wallet), qty=qty, price=price, action=action)

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
                                    log.info('Cost not satisfied for {2} {1} {0}. Can not set reduce_only to True'
                                             .format(wallet, market.base.code, size), action=action)
                                    return False, size, False
                            else:
                                log.info('Cost not satisfied for {2} {1} {0}. Can not set reduce_only to True'
                                         .format(wallet, market.base.code, size), margined=market.margined.code)
                                return False, size, False
                        else:
                            log.info('Cost not satisfied for {2} {1} {0}. Can not set reduce_only to True'
                                     .format(wallet, market.base.code, size), type=market.type)
                            return False, size, False
                    else:
                        log.info('Cost not satisfied for {2} {1} {0}. Can not set reduce_only to True'
                                 .format(wallet, market.base.code, size), exid=market.exchange.exid)
                        return False, size, False
                else:
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
        if wallet == 'spot':
            market, flip = self.exchange.get_spot_market(code, self.quote)
        elif wallet == 'future':
            market, flip = self.exchange.get_perp_market(code, self.quote)

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

            log.bind(orderid=order.orderid)

            # Get traded amount
            filled_prev = order.filled
            filled_total = response['filled']

            # Determine new trade
            if filled_total > filled_prev:
                filled_new = filled_total - filled_prev
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

            if new:
                log.info(' ')
                log.info('Update order with clientID {0}'.format(order.clientid))
                log.info('Update order with status {0}'.format(status))
                log.info('Update order for {0} ({1})'.format(order.market.base.code, order.market.wallet))
                log.info('Update order to {0}'.format(order.action.replace('_', ' ')))

            if filled_new:

                log.info('Trade new {0}'.format(filled_new))
                log.info('Trade total {0}'.format(filled_total))
                log.unbind('account', 'orderid')

                return filled_new, order.average

            else:
                log.unbind('account', 'orderid')
                return False, False

    # Offset transfer
    def offset_transfer(self, source, destination, amount, transfer_id):

        log.bind(id=transfer_id, account=self.name)
        log.info(' ')
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
        log.unbind('id', 'account')

    # Offset quantity after a trade
    def offset_order_filled(self, code, action, filled, average):

        log.info(' ')
        log.bind(account=self.name)
        log.info('Offset trade to {0}'.format(action.replace('_', ' ')))

        offset = self.balances.copy()

        for col in offset.columns.get_level_values(0).unique().tolist():
            offset.loc[:, col] = np.nan

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
                self.balances.loc[code, ('position', 'open', 'quantity')] = 0
                self.balances.loc[code, ('position', 'open', 'value')] = 0
                self.balances.loc[code, ('position', 'open', 'unrealized_pnl')] = 0

        pct = self.balances.account.current.percent[code] * 100
        log.info('Percentage for {0} was {1}%'.format(code, round(pct, 1)))

        offset = offset.dropna(axis=0, how='all').dropna(axis=1, how='all')
        try:
            # Apply offset
            updated = self.balances.loc[offset.index, offset.columns].fillna(0) + offset

        except KeyError:
            log.info(filled)
            log.info(average)
            log.info('action {0}'.format(action))
            log.info(offset.index)
            log.info(offset.columns)
            raise Exception('Unable to offset trade')

        else:

            updated = updated.replace(0, np.nan)

            # Update dataframe
            self.balances.loc[offset.index, offset.columns] = updated

            # Update new percentage
            assets_value = self.assets_value()
            for c in [code, self.quote]:
                exposure_value = self.balances.account.current.value[c]
                pct = exposure_value / assets_value
                self.balances.loc[c, ('account', 'current', 'percent')] = pct
                log.info('Percentage for {0} is now {1}%'.format(c, round(pct * 100, 1)))

            self.save()

            log.info('Offset complete')
            log.unbind('account')

    # Offset used resources after an order is opened
    def offset_order_new(self, code, action, qty, val):

        log.info(' ')
        log.bind(account=self.name)
        log.info('Offset used and free resources')

        offset = self.balances.copy()
        for col in offset.columns.get_level_values(0).unique().tolist():
            offset.loc[:, col] = np.nan

        log.info('Offset to {0}'.format(action.replace('_', ' ')))
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

        offset = offset.dropna(axis=0, how='all').dropna(axis=1, how='all')
        updated = self.balances.loc[offset.index, offset.columns].fillna(0) + offset
        self.balances.loc[offset.index, offset.columns] = updated
        self.save()

        log.info('Offset complete')
        log.unbind('account')

    # Offset a cancelled order
    def offset_order_cancelled(self, code, side, qty, val, filled=0):
        pass

    # Return sum of open orders size
    def get_open_orders_size(self, code, side, actions):

        # Test if a close_spot or a buy_spot order is open
        market, flip = self.exchange.get_spot_market(code, self.quote)
        qs = Order.objects.filter(account=self,
                                  status__in=['open', 'preparation'],
                                  market=market,
                                  side=side,
                                  action__in=actions
                                  )
        if not qs:
            return 0
        else:
            qs_amount = qs.aggregate(Sum('amount'))['amount__sum']
            qs_price = qs.aggregate(Avg('price'))['price__avg']

            if flip:
                return qs_amount / qs_price
            else:
                return qs_amount


class Asset(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='asset', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='asset', null=True)
    currency = models.ForeignKey(Currency, on_delete=models.SET_NULL, related_name='asset', null=True)
    wallet = models.CharField(max_length=20, null=True, blank=True)
    total = models.FloatField(max_length=10, null=True, default=0)
    free = models.FloatField(max_length=10, null=True, default=0)
    used = models.FloatField(max_length=10, null=True, default=0)
    dt_response = models.DateTimeField(null=True)
    dt_modified = models.DateTimeField(null=True)
    dt_created = models.DateTimeField(null=True)

    class Meta:
        verbose_name_plural = "Assets"

    def save(self, *args, **kwargs):
        is_new = self._state.adding
        self.dt_modified = timezone.now()
        super(Asset, self).save(*args, **kwargs)
        if is_new:
            self.dt_created = timezone.now()

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
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='stats', null=True)
    order_execution_success_rate = models.FloatField(null=True)
    order_execution_time_avg = models.FloatField(null=True)
    order_executed = models.FloatField(null=True)
    trade_total_value = models.FloatField(null=True)
    positions_notional_value = models.FloatField(null=True)
    fee_avg_spot = models.FloatField(null=True)
    fee_avg_future = models.FloatField(null=True)
    funding_rate_avg = models.FloatField(null=True)
    assets_value = models.FloatField(null=True)
    assets_distribution = PickledObjectField(null=True)
    historical_value = PickledObjectField(null=True)
    historical_returns = PickledObjectField(null=True)
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
