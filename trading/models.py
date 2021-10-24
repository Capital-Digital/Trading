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

    def get_balances_qty(self, wallet):

        client = self.exchange.get_ccxt_client(self)
        client.options['defaultType'] = wallet
        response = client.fetchBalance()
        for key in ['total', 'free', 'used']:
            dic = {k: v for k, v in response[key].items() if v > 0}
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

        return self.balances

    # Return a dictionary with balance of a specific wallet
    def get_balances_value(self):

        # Get wallets balances
        for wallet in self.exchange.get_wallets():
            balances_qty = self.get_balances_qty(wallet)
            if wallet in balances_qty.columns.get_level_values(0):
                if 'value' not in balances_qty.columns.get_level_values(2):
                    log.info('Get balances value ({0})'.format(wallet))

                    df = balances_qty.apply(lambda row: convert_balance(row, wallet, self.exchange), axis=1)
                    df.columns.set_levels(['value'], level=1, inplace=True)
                    df.columns = pd.MultiIndex.from_tuples(map(lambda x: (wallet, x[0], x[1]), df.columns))
                    self.balances = pd.concat([self.balances, df], axis=1)

            # Drop coins < $10
            mask = self.balances.loc[:, self.balances.columns.get_level_values(2) == 'value'] > 10
            self.balances = self.balances.loc[(mask == True).any(axis=1)]

        # Get open positions
        self.get_positions_value()
        return self.balances

    def get_positions_value(self):

        client = self.exchange.get_ccxt_client(self)
        response = client.fapiPrivateGetPositionRisk()
        opened = [i for i in response if float(i['positionAmt']) != 0]
        closed = [i for i in response if float(i['positionAmt']) == 0]

        if not hasattr(self, 'balances'):
            self.balances = pd.DataFrame(columns=pd.MultiIndex.from_product([['position'], ['open'], ['value']]))

        if opened:
            log.info('Get open positions')
            for position in opened:
                market = Market.objects.get(exchange=self.exchange,
                                            response__id=position['symbol'],
                                            type='derivative'
                                            )
                quantity = float(position['positionAmt'])
                self.balances.loc[market.base, ('position', 'open', 'quantity')] = quantity
                self.balances.loc[market.base, ('position', 'open', 'side')] = 'buy' if quantity > 0 else 'sell'
                self.balances.loc[market.base, ('position', 'open', 'value')] = quantity * float(position['markPrice'])
                self.balances.loc[market.base, ('position', 'open', 'leverage')] = float(position['leverage'])
                self.balances.loc[market.base, ('position', 'open', 'unrealized_pnl')] = float(
                    position['unRealizedProfit'])
                self.balances.loc[market.base, ('position', 'open', 'liquidation')] = float(
                    position['liquidationPrice'])

        return self.balances

    # Returns a Series with target value
    def get_target_value(self):

        df = self.get_balances_value()
        tmp = df.loc[:, df.columns.get_level_values(2) == 'value']
        if 'position' in tmp.columns:  # drop open position's value
            tmp = tmp.drop('position', axis=1)
        balance = tmp.loc[:, tmp.columns.get_level_values(1) == 'total'].sum().sum()
        return balance * self.strategy.get_target_pct()

    # Returns a Series with target quantity per coin
    def get_target_qty(self):

        target = self.get_target_value()
        for code in target.index:
            target[code] /= Currency.objects.get(code=code).get_latest_price(self.exchange, 'last')

        return target

    def get_delta(self):

        target = self.get_target_qty()
        df = self.balances.loc[:, self.balances.columns.get_level_values(2) == 'quantity']  # get spot and position qty

        for coin_target in target.index:

            # Coins in account
            if coin_target in df.index:
                for source in df.columns:
                    qty = df.loc[coin_target, source]
                    if not np.isnan(qty):
                        self.balances.loc[coin_target, 'target'] = target[coin_target]
                        self.balances.loc[coin_target, 'delta'] = qty - target[coin_target]

            # Coins not in account
            if coin_target not in df.index:
                self.balances.loc[coin_target, 'target'] = target[coin_target]
                self.balances.loc[coin_target, 'delta'] = -target[coin_target]

        # Coins not in target portfolio
        for coin_account in df.index:
            if coin_account != self.exchange.dollar_currency:
                if coin_account not in target.index:
                    for source in df.columns:
                        qty = df.loc[coin_account, source]
                        if not np.isnan(qty):
                            self.balances.loc[coin_account, 'delta'] = qty

        # Open positions
        if 'position' in self.balances.columns.get_level_values(0):
            positions = self.balances.loc[self.balances.position.open.quantity < 0]
            for code, row in positions.iterrows():
                if code not in target.index:
                    self.balances.loc[code, 'delta'] = row.position.open.quantity
                elif target[code] > 0:
                    self.balances.loc[code, 'delta'] = row.position.open.quantity
                elif target[code] < 0:

                    # Format decimals
                    market = Market.objects.get(base__code=code,
                                                exchange=self.exchange,
                                                quote__code='USDT',
                                                type='derivative',
                                                contract_type='perpetual'
                                                )
                    amount = format_decimal(counting_mode=self.exchange.precision_mode,
                                            precision=market.precision['amount'],
                                            n=abs(target[code])
                                            )
                    self.balances.loc[code, 'delta'] = - amount - row.position.open.quantity

        print('Delta')
        print(self.balances)
        return self.balances

    def sell_spot(self, load=False):
        log.info('*** Sell spot ***')
        df = self.get_delta() if load else self.balances
        for code, row in df.loc[df['delta'] > 0].iterrows():  # sell

            # Select quantities
            hold = row.spot.total.quantity
            target = row[('target', '', '')]
            delta = row[('delta', '', '')]

            # Determine amount we must sell
            if target < 0:  # short
                if not np.isnan(hold):
                    amount = hold
                else:
                    continue

            elif (target > 0) or np.isnan(target):
                amount = delta

            price = Currency.objects.get(code=code).get_latest_price(self.exchange, 'ask')
            price += (price * float(self.limit_price_tolerance))
            market = Market.objects.get(quote__code=self.exchange.dollar_currency,
                                        exchange=self.exchange,
                                        base__code=code,
                                        type='spot')

            self.place_order('sell spot', market, 'sell', amount, price)

    def close_short(self, load=False):
        log.info('*** Close short ***')
        df = self.get_delta() if load else self.balances
        for code, row in df.loc[df['delta'] < 0].iterrows():  # buy ?
            if 'position' in df.columns.get_level_values(0):
                if row.position.open.quantity < 0:  # short is open ?
                    delta = row[('delta', '', '')]
                    amount = min(abs(delta), abs(row.position.open.quantity))

                    market = Market.objects.get(quote__code=self.exchange.dollar_currency,
                                                exchange=self.exchange,
                                                base__code=code,
                                                type='derivative',
                                                contract_type='perpetual'
                                                )

                    if not self.has_order(market):
                        price = market.get_latest_price('last')  # bid not available
                        price -= (price * float(self.limit_price_tolerance))
                        self.place_order('close short', market, 'buy', amount, price)
                    else:
                        log.info('Unable to close short (open order)')

    def buy_spot(self, load=False):
        log.info('*** Buy spot ***')
        df = self.get_delta() if load else self.balances
        for code, row in df.loc[df['delta'] < 0].iterrows():  # buy

            pos_qty = row.position.open.quantity if 'position' in df.columns.get_level_values(0) else None
            if not pos_qty:  # no short position already open

                # Determine buy price
                price = Currency.objects.get(code=code).get_latest_price(self.exchange, 'bid')
                price -= (price * float(self.limit_price_tolerance))

                if self.exchange.dollar_currency in df.index:

                    # Determine quantities
                    qty_usdt = df.loc['USDT', ('spot', 'free', 'quantity')]
                    qty_usdt = qty_usdt if not np.isnan(qty_usdt) else 0
                    qty_coin = abs(row[('delta', '', '')])

                    # Not enough resources ?
                    if qty_coin > (qty_usdt / price):
                        # Move available funds from future to spot wallet
                        trans = (qty_coin * price) - qty_usdt
                        log.info('Move {0} USDT from future to spot'.format(round(trans, 2)))
                        moved = self.move_fund('USDT', trans, 'future', 'spot')
                        qty_usdt += moved

                    amount = min(qty_coin, qty_usdt / price)
                    market = Market.objects.get(quote__code=self.exchange.dollar_currency,
                                                exchange=self.exchange,
                                                base__code=code,
                                                type='spot'
                                                )
                    self.place_order('buy spot', market, 'buy', amount, price)

                    # Remove USDT amount from available fund
                    df.loc['USDT', ('spot', 'free', 'quantity')] -= amount * price
                    print('after buy', df)

                else:
                    log.info('Unable to buy spot (no free resource)')
            else:
                log.info('Unable to buy spot ({0}/USDT position open)'.format(code))

    def open_short(self, load=False):
        log.info('*** Open short ***')
        df = self.get_delta() if load else self.balances
        for code, row in df.loc[df['delta'] > 0].iterrows():  # is sell ?

            target = row[('target', '', '')]
            delta = row[('delta', '', '')]

            if target < 0:  # is short ?

                amount = delta
                market = Market.objects.get(quote__code=self.exchange.dollar_currency,
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

                    trans = min(free_spot, margin) if free_margin == 0 else max(0, (margin - free_margin))
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
        print(raw_amount)
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
                log.warning('Unable to {1} {2} {0} (min notional)'.format(market.base.code, side, market.wallet),
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

    def cancel_orders(self, web=False):
        log.info('Cancel orders start')
        client = self.exchange.get_ccxt_client(account=self)

        for wallet in self.exchange.get_wallets():
            client.options['defaultType'] = wallet
            client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

            if web:
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
