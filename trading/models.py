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
import collections

import warnings

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
    params = models.JSONField(null=True, blank=True)
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

    # Fetch coins and create balances dataframe
    def get_balances_qty(self):

        log.info('*** Fetch account balances ***')
        client = self.exchange.get_ccxt_client(self)

        # Del attribute
        if hasattr(self, 'balances'):
            del self.balances

        # Iterate through exchange's wallets
        for wallet in self.exchange.get_wallets():

            client.options['defaultType'] = wallet
            response = client.fetchBalance()
            for key in ['total', 'free', 'used']:

                # Exclude LBTC from dictionary (staking or earning account)
                dic = {k: v for k, v in response[key].items() if v > 0 and k != 'LDBTC'}

                if dic:
                    log.info('Get balances quantity in {1} ({0})'.format(key, wallet))
                    tmp = pd.DataFrame(index=dic.keys(),
                                       data=dic.values(),
                                       columns=pd.MultiIndex.from_product([[wallet], [key], ['quantity']])
                                       )
                    self.balances = tmp if not hasattr(self, 'balances') else pd.concat([self.balances, tmp])
                    self.balances = self.balances.groupby(level=0).last()
                else:
                    self.balances = pd.DataFrame() if not hasattr(self, 'balances') else self.balances

    # Convert quantity in dollar in balances dataframe
    def get_balances_value(self):

        log.info('Calculate dollar values')

        # Iterate through wallets, free, used and total quantities
        for wallet in list(set(self.balances.columns.get_level_values(0))):
            for state in self.balances.columns.get_level_values(1):
                funds = self.balances[wallet][state]['quantity']
                for coin in funds.index:
                    price = Currency.objects.get(code=coin).get_latest_price(self.quote, 'last')
                    value = price * funds[coin]
                    self.balances.loc[coin, (wallet, state, 'value')] = value

            # Drop dust < $10
            mask = self.balances.loc[:, self.balances.columns.get_level_values(2) == 'value'] > 10
            self.balances = self.balances.loc[(mask == True).any(axis=1)]
            self.save()

    # Fetch and update open positions in balances dataframe
    def get_positions_value(self):

        log.info('*** Fetch account positions ***')

        # Client client and query all futures positions
        client = self.exchange.get_ccxt_client(self)
        response = client.fapiPrivateGetPositionRisk()
        opened = [i for i in response if float(i['positionAmt']) != 0]
        closed = [i for i in response if float(i['positionAmt']) == 0]

        if opened:

            log.info('There is {0} position(s) open in futures market'.format(len(opened)))

            for position in opened:

                log.info('Update positions {0}'.format(position['symbol']))

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

        else:
            log.info('There is no position open in futures market')

    # Return account total value
    def account_value(self):
        val = []
        for level in list(set(self.balances.columns.get_level_values(0))):
            if level != 'position':
                val.append(self.balances[level].total.value)
        return sum(val)

    # Returns a Series with target value
    def get_target_value(self):
        account_value = self.account_value()
        target_pct = self.strategy.get_target_pct()
        return account_value * target_pct

    # Returns a Series with target quantity
    def get_target_qty(self):
        target = self.get_target_value()
        for code in target.index:
            target[code] /= Currency.objects.get(code=code).get_latest_price(self.quote, 'last')
        return target

    def get_delta(self):

        target = self.get_target_qty()
        print('target\n', target.name)
        print(type(target))

        #  Select quantities from wallet total balances and open positions
        df = self.balances.loc[:, (self.balances.columns.get_level_values(2) == 'quantity')]
        mask = df.columns.get_level_values(1).isin(['total', 'open'])
        df = df.loc[:, mask]

        # Determine total exposure
        self.balances.loc[:, ('account', 'net', 'quantity')] = df.sum(axis=1)

        # Iterate through target coins
        for coin in target.index:

            # Coins already in account ?
            if coin in df.index:
                qty = self.balances.loc[coin, ('account', 'net', 'quantity')]

                # if not np.isnan(qty):
                self.balances.loc[coin, ('account', 'trade', 'target')] = target[coin]
                self.balances.loc[coin, ('account', 'trade', 'delta')] = qty - target[coin]

            # Coins not in account ?
            else:
                self.balances.loc[coin, ('account', 'trade', 'target')] = target[coin]
                self.balances.loc[coin, ('account', 'trade', 'delta')] = -target[coin]

        # Iterate through coins in account
        for coin in [df.index]:

            # Coin not in target ?
            if coin not in target.index:
                qty = self.balances.loc[coin, ('account', 'net', 'quantity')]
                self.balances.loc[coin, ('account', 'trade', 'delta')] = qty

        self.save()

        # # Open positions
        # if 'position' in self.balances.columns.get_level_values(0):
        #     positions = self.balances.loc[self.balances.position.open.quantity < 0]
        #     for code, row in positions.iterrows():
        #         if code not in target.index:
        #             self.balances.loc[code, 'delta'] = row.position.open.quantity
        #         elif target[code] > 0:
        #             self.balances.loc[code, 'delta'] = row.position.open.quantity
        #         elif target[code] < 0:
        #
        #             # Format decimals
        #             market = Market.objects.get(base__code=code,
        #                                         exchange=self.exchange,
        #                                         quote__code='USDT',
        #                                         type='derivative',
        #                                         contract_type='perpetual'
        #                                         )
        #             amount = format_decimal(counting_mode=self.exchange.precision_mode,
        #                                     precision=market.precision['amount'],
        #                                     n=abs(target[code])
        #                                     )
        #             self.balances.loc[code, 'delta'] = amount - abs(row.position.open.quantity)
        # print('Delta')
        # print(self.balances)

    def sell_spot(self, force_update=False):

        log.info('*** Sell spot ***')

        # Create balance dataframe
        df = self.get_delta() if force_update else self.balances

        # Select codes. Don't sell quote currency
        codes = [i for i in df.loc[df['delta'] > 0].index if i != self.quote]

        if codes:
            log.info('Sell spot {0} base currencies'.format(len(codes)))
            for code in codes:

                # Select quantities
                free = df.spot.free.quantity[code]
                target = df.target[code]
                delta = df.delta[code]

                log.info('Sell spot {0} {1}'.format(round(delta, 3), code))

                # Determine amount we must sell
                if target < 0:  # short
                    if not np.isnan(free):
                        amount = free
                    else:
                        continue

                elif (target > 0) or np.isnan(target):
                    amount = delta

                price = Currency.objects.get(code=code).get_latest_price(self.quote, 'ask')
                price += (price * float(self.limit_price_tolerance))
                market = Market.objects.get(quote__code=self.quote,
                                            exchange=self.exchange,
                                            base__code=code,
                                            type='spot')

                self.place_order('sell spot', market, 'sell', amount, price)

        else:
            log.info('No base currency to sell spot')

    def close_short(self, force_update=False):

        log.info('*** Close short ***')

        df = self.get_delta() if force_update else self.balances

        for code, row in df.loc[df['delta'] < 0].iterrows():  # buy ?
            if 'position' in df.columns.get_level_values(0):
                if row.position.open.quantity < 0:  # short is open ?
                    delta = row[('delta', '', '')]
                    amount = min(abs(delta), abs(row.position.open.quantity))

                    market = Market.objects.get(quote__code=self.quote,
                                                exchange=self.exchange,
                                                base__code=code,
                                                type='derivative',
                                                contract_type='perpetual'
                                                )

                    if not self.has_order(market):
                        print('place order')
                        price = market.get_latest_price('last')  # bid not available
                        price -= (price * float(self.limit_price_tolerance))
                        self.place_order('close short', market, 'buy', amount, price)
                    else:
                        log.info('Unable to close short (open order)')

    def buy_spot(self, force_update=False):

        log.info('*** Buy spot ***')

        df = self.get_delta() if force_update else self.balances

        codes = [i for i in df.loc[df['delta'] < 0].index if i != self.quote]  # Don't buy quote currency

        if codes:
            log.info('Buy spot {0} base currencies'.format(len(codes)))
            for code in codes:

                if 'position' in df.columns.get_level_values(0):
                    pos_qty = df.loc[code, ('position', 'open', 'quantity')]
                else:
                    pos_qty = np.nan

                # Test if a position is open
                if np.isnan(pos_qty):

                    if self.quote in df.index:

                        # Determine quantities
                        qty_coin = abs(df.delta[code])
                        qty_cash = df.spot.free.quantity[self.quote]

                        # Check if cash is available
                        if not np.isnan(qty_cash) and qty_cash > 0:

                            # Determine buy price
                            price = Currency.objects.get(code=code).get_latest_price(self.quote, 'bid')
                            price -= (price * float(self.limit_price_tolerance))

                            # Not enough resources ?
                            if qty_coin > (qty_cash / price):

                                # Move available funds from future to spot wallet
                                qty_move = (qty_coin * price) - qty_cash
                                log.info('Move {0} {1} from future to spot'.format(round(qty_move, 2), self.quote))
                                qty_moved = self.move_fund(self.quote, qty_move, 'future', 'spot')
                                qty_cash += qty_moved

                            amount = min(qty_coin, qty_cash / price)
                            market = Market.objects.get(quote__code=self.quote,
                                                        exchange=self.exchange,
                                                        base__code=code,
                                                        type='spot'
                                                        )
                            self.place_order('buy spot', market, 'buy', amount, price)

                            # Remove cash amount from available fund
                            df.loc[self.quote, ('spot', 'free', 'quantity')] -= amount * price

                        else:
                            log.info('Unable to buy spot base currency {0} (no free resource)'.format(code))
                    else:
                        log.info('Unable to buy spot base currency {0} (no cash)'.format(code))
                else:
                    log.info('Unable to buy spot base currency {0} (position is open)'.format(code))

        else:
            log.info('No base currency to buy spot')

    def open_short(self, force_update=False):

        log.info('*** Open short ***')

        df = self.get_delta() if force_update else self.balances

        for code, row in df.loc[df['delta'] > 0].iterrows():  # is sell ?

            target = row[('target', '', '')]
            delta = row[('delta', '', '')]

            if target < 0:  # is short ?

                amount = delta
                market = Market.objects.get(quote__code=self.quote,
                                            exchange=self.exchange,
                                            base__code=code,
                                            type='derivative',
                                            contract_type='perpetual'
                                            )
                if not self.has_order(market):

                    price = market.get_latest_price('last')  # ask not available
                    price += (price * float(self.limit_price_tolerance))
                    margin = amount * price
                    free_margin = 0
                    free_spot = 0

                    if 'USDT' in df.index:

                        # Determine free margin and spot resources
                        if 'future' in df.columns.get_level_values(0):
                            free_margin = df.loc['USDT', ('future', 'free', 'quantity')]
                        if 'spot' in df.columns.get_level_values(0):
                            free_spot = df.loc['USDT', ('spot', 'free', 'quantity')]

                    trans = min(free_spot, margin) if free_margin == 0 else min(free_spot, (margin - free_margin))
                    if trans > 0:
                        self.move_fund('USDT', trans, 'spot', 'future')

                    self.place_order('open short', market, 'sell', amount, price)

                else:
                    log.warning('Unable to open short {0} {1} (open order)'.format(round(amount, 4),
                                                                                   market.symbol,
                                                                                   market.type)
                                )

    def move_fund(self, code, amount, from_wallet, to_wallet):
        client = self.exchange.get_ccxt_client(self)
        log.info('Transfer {0} {1} from {2} to {3}'.format(round(amount, 4), code, from_wallet, to_wallet))

        if from_wallet == 'future':
            if 'position' in self.balances.columns.get_level_values(0):

                # Check total margin and position's notional value in future account
                total_margin = self.balances.loc['USDT', ('future', 'total', 'quantity')]
                notional_values = self.balances[('position', 'open', 'value')].sum()
                free_margin = total_margin - notional_values

                if free_margin < amount:
                    amount = free_margin
                    log.info('Lower amount to {0} USDT to preserve 1:1 margin'.format(round(amount, 2)))

        try:
            client.transfer(code, amount, from_wallet, to_wallet)
        except Exception as e:
            log.error('Error transferring fund',
                      e=e,
                      source=from_wallet,
                      destination=to_wallet,
                      code=code,
                      amount=amount)
        else:
            log.info('Transfer success')
            return amount

    def place_order(self, action, market, side, raw_amount, price):

        # Format decimals
        amount = format_decimal(counting_mode=self.exchange.precision_mode,
                                precision=market.precision['amount'],
                                n=raw_amount)

        print('\n', market.symbol, market.type)
        print(action)
        print(amount)
        print(side)

        # Test for amount limit
        if limit_amount(market, amount):
            # Test min notional
            min_notional, reduce_only = test_min_notional(market, action, amount, price)
            if min_notional:

                log.info('Place order to {0} {3} {1} {2} market ({3})'.format(side, market.base.code, market.type,
                                                                              amount, action))

                args = dict(
                    symbol=market.symbol,
                    type='limit' if self.limit_order else 'market',
                    side=side,
                    amount=amount,
                    price=price
                )

                if reduce_only:
                    args['params'] = dict(reduceonly=True)

                print(market.type, 'order')
                pprint(args)

                # Place order and create object
                client = self.exchange.get_ccxt_client(self)
                client.options['defaultType'] = market.wallet
                response = client.create_order(**args)
                self.create_update_order(response, action, market)

            else:
                log.info('Unable to {1} {2} {0} (min notional)'.format(market.base.code, side, market.wallet),
                         amount=round(raw_amount, 4))
        else:
            log.info("Unable to {1} {2} {0} (limit amount)".format(market.base.code, side, market.wallet),
                     amount=round(raw_amount, 4))

    def update_orders(self):
        client = self.exchange.get_ccxt_client(account=self)
        for wallet in self.exchange.get_wallets():
            orders = Order.objects.filter(account=self, market__wallet=wallet, status='open')
            if orders.exists():

                client.options['defaultType'] = wallet
                client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
                for order in orders:
                    responses = client.fetchOrder(id=order.orderid, symbol=order.market.symbol)
                    self.create_update_order(responses, action=order.action, market=order.market)

                    log.info('Order update', id=order.orderid, wallet=wallet)

                    # Cancel order before strategy update
                    sec = self.strategy.seconds_before_update()
                    if sec < 240:
                        log.info('Order cancellation...', sec=sec)
                        self.cancel_order(wallet, order.market.symbol, order.orderid)
            else:
                pass
                # log.info('Update order object N/A', wallet=wallet)

    def create_update_order(self, response, action, market):
        args = dict(account=self, market=market, orderid=response['id'])
        try:
            order = Order.objects.get(**args)
        except ObjectDoesNotExist:
            pass
        finally:
            defaults = dict(
                action=action,
                amount=response['amount'],
                average=response['average'],
                cost=response['cost'],
                datetime=datetime.now().replace(tzinfo=pytz.UTC),
                fee=response['fee'],
                filled=float(response['filled']),
                last_trade_timestamp=response['lastTradeTimestamp'],
                price=response['price'],
                remaining=response['remaining'],
                response=response,
                side=response['side'],
                status=response['status'],
                timestamp=int(response['timestamp']) if response['timestamp'] else None,
                trades=response['trades'],
                type=response['type']
            )
            obj, created = Order.objects.update_or_create(**args, defaults=defaults)

            if created:
                log.info('Order created with status "{0}"'.format(response['status'], id=response['id']))

            else:
                if action in ['sell_spot', 'close_short']:
                    filled = float(response['filled']) - order.filled
                    if filled > 0:
                        log.info(
                            'Order filled at {0}% {1}'.format(round(filled / order.amount, 3) * 100, market.base.code))
                        self.buy_spot(load=True)

    def cancel_order(self, wallet, symbol, orderid):
        log.info('Order cancel', id=orderid, symbol=symbol)
        client = self.exchange.get_ccxt_client(account=self)
        client.options['defaultType'] = wallet

        client.cancel_order(id=orderid, symbol=symbol)

        try:
            obj = Order.objects.get(orderid=orderid)
        except ObjectDoesNotExist:
            pass
        else:
            obj.status = 'canceled'
            obj.save()

    # Cancel all open orders
    def cancel_orders(self, user_orders=False):
        log.info('Cancel orders start')
        client = self.exchange.get_ccxt_client(account=self)

        for wallet in self.exchange.get_wallets():
            client.options['defaultType'] = wallet
            client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

            if user_orders:
                # Fetch all open orders from exchange
                responses = client.fetchOpenOrders()
                if responses:
                    for order in responses:
                        self.cancel_order(wallet, order['symbol'], order['id'])
                    log.info('Cancel all orders complete')
                else:
                    log.info('Cancel orders N/A', wallet=wallet)
            else:
                # Query known open orders from db
                orders = Order.objects.filter(account=self,
                                              market__wallet=wallet,
                                              status='open'
                                              )
                if orders.exists():
                    for order in orders:
                        self.cancel_order(wallet, order.market.symbol, order.orderid)
                    log.info('Cancel all orders complete')
                else:
                    log.info('Cancel orders N/A', wallet=wallet)

    def has_order(self, market):
        client = self.exchange.get_ccxt_client(self)
        client.options['defaultType'] = market.wallet
        orders = client.fetchOpenOrders(market.symbol)

        if orders:
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

    # Cancel open orders and rebalance portfolio
    def trade(self):

        log.info('***')
        log.info('Start trade')
        log.info('***')

        # Cancel orders
        self.cancel_orders()

        # Construct a new dataframe
        self.get_delta()

        # Free resources
        self.sell_spot(force_update=True)
        self.close_short()

        # Allocate funds
        self.buy_spot(force_update=True)
        self.open_short()

        log.info('***')
        log.info('End trade')
        log.info('***')


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
