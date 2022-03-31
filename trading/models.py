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
            for tp in list(set(self.balances[wallet].columns.get_level_values(0))):
                funds = self.balances[wallet][tp]['quantity']
                for coin in funds.index:
                    price = Currency.objects.get(code=coin).get_latest_price(self.quote, 'last')
                    value = price * funds[coin]
                    self.balances.loc[coin, (wallet, tp, 'value')] = value

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

    # Return account total value
    def account_value(self):
        wallets = []
        for level in list(set(self.balances.columns.get_level_values(0))):
            if level != 'position':
                # Sum value of all coins
                wallets.append(self.balances[level].total.value.sum())

        # Sum value of all wallet
        return sum(wallets)

    # Returns a Series with target value
    def get_target_value(self):
        account_value = self.account_value()
        target_pct = self.strategy.get_target_pct()

        log.info('Target percentages')
        print(target_pct)

        return account_value * target_pct

    # Returns a Series with target quantity
    def get_target_qty(self):
        target = self.get_target_value()
        for code in target.index:
            target[code] /= Currency.objects.get(code=code).get_latest_price(self.quote, 'last')
        return target

    # Calculate net exposure and delta
    def get_delta(self):

        target = self.get_target_qty()

        #  Select quantities from wallet total balances and open positions
        df = self.balances.loc[:, (self.balances.columns.get_level_values(2) == 'quantity')]
        mask = df.columns.get_level_values(1).isin(['total', 'open'])
        df = df.loc[:, mask]

        # Determine total exposure
        self.balances.loc[:, ('account', 'current', 'exposure')] = df.sum(axis=1)

        # Iterate through target coins and calculate delta
        for coin in target.index.values.tolist():

            # Coins already in account ?
            if coin in df.index.values.tolist():
                qty = self.balances.loc[coin, ('account', 'current', 'exposure')]

                # if not np.isnan(qty):
                self.balances.loc[coin, ('account', 'trade', 'target')] = target[coin]
                self.balances.loc[coin, ('account', 'trade', 'delta')] = qty - target[coin]

            # Coins not in account ?
            else:
                self.balances.loc[coin, ('account', 'trade', 'target')] = target[coin]
                self.balances.loc[coin, ('account', 'trade', 'delta')] = -target[coin]

        # Iterate through coins in account and calculate delta
        for coin in df.index.values.tolist():

            # Coin not in target ?
            if coin not in target.index.values.tolist():
                qty = self.balances.loc[coin, ('account', 'current', 'exposure')]
                self.balances.loc[coin, ('account', 'trade', 'delta')] = qty
                self.balances.loc[coin, ('account', 'trade', 'target')] = 0

        self.save()

    # Sell in spot market
    def sell_spot(self):

        # Select codes to sell (exclude quote currency)
        delta = self.balances.account.trade.delta
        codes_to_sell = [i for i in delta.loc[delta > 0].index.values.tolist() if i != self.quote]

        # Codes should be sold ?
        if codes_to_sell:

            log.info('Sell spot {0} currencies'.format(len(codes_to_sell)))
            for code in codes_to_sell:

                # Select quantities
                free = self.balances.spot.free.quantity[code]
                target = self.balances.account.trade.target[code]
                qty_delta = delta[code]

                # Spot resources could be released ?
                if not np.isnan(free):

                    log.info('Sell spot {0} {1}'.format(round(qty_delta, 3), code))

                    # Sell all resources available if coin must be shorted
                    if target < 0:
                        amount = free

                    # Sell all resources available if coin is not allocated
                    elif target == 0:
                        amount = free

                    else:
                        amount = qty_delta

                    # Place sell order
                    price = Currency.objects.get(code=code).get_latest_price(self.quote, 'ask')
                    price += (price * float(self.limit_price_tolerance))
                    market = Market.objects.get(quote__code=self.quote,
                                                exchange=self.exchange,
                                                base__code=code,
                                                type='spot')

                    self.place_order('sell spot', market, 'sell', amount, price)

    # Sell in derivative market
    def close_short(self):

        # Select codes to buy (exclude quote currency)
        delta = self.balances.account.trade.delta
        codes_to_buy = [i for i in delta.loc[delta < 0].index.values.tolist() if i != self.quote]

        for code in codes_to_buy:

            # Code is shorted now ?
            if 'position' in self.balances.columns.get_level_values(0):
                if self.balances.position.open.quantity[code] < 0:

                    # Get quantities
                    delta = abs(delta[code])
                    shorted = abs(self.balances.position.open.quantity)
                    amount = min(delta, shorted)

                    # Place buy order
                    price = Currency.objects.get(code=code).get_latest_price(self.quote, 'bid')
                    price -= (price * float(self.limit_price_tolerance))
                    market = Market.objects.get(quote__code=self.quote,
                                                exchange=self.exchange,
                                                base__code=code,
                                                type='derivative',
                                                contract_type='perpetual'
                                                )
                    self.place_order('close short', market, 'buy', amount, price, reduce_only=True)

    # Buy in spot market
    def buy_spot(self):

        # Select codes to buy (exclude quote currency)
        delta = self.balances.account.trade.delta
        codes_to_buy = [i for i in delta.loc[delta < 0].index.values.tolist() if i != self.quote]

        if codes_to_buy:

            for code in codes_to_buy:

                # Determine missing quantity and it's dollar value
                delta = self.balances.account.trade.delta[code]
                price = Currency.objects.get(code=code).get_latest_price(self.quote, 'ask')
                delta_value = delta * price

                # Check if cash is available
                if self.quote in self.balances.index.values.tolist():
                    if 'spot' in self.balances.columns.get_level_values(0):
                        cash = self.balances.spot.free.quantity[self.quote]

                        # Not enough cash available?
                        if cash < delta_value:

                            log.warning('Cash is needed to buy {0} spot'.format(code))

                            # Determine cash needed and move fund
                            cash_needed = delta_value - cash
                            self.move_fund(self.quote, cash_needed, 'spot')

                        # Place order
                        amount = delta_value
                        price -= (price * float(self.limit_price_tolerance))
                        market = Market.objects.get(quote__code=self.quote,
                                                    exchange=self.exchange,
                                                    base__code=code,
                                                    type='spot'
                                                    )
                        self.place_order('buy spot', market, 'buy', amount, price, quote_order_qty=True)

    # Sell in derivative market
    def open_short(self):

        # Select codes to sell (exclude quote currency)
        delta = self.balances.account.trade.delta
        to_sell = [i for i in delta.loc[delta > 0].index.values.tolist() if i != self.quote]

        # Select codes to short
        target = self.balances.account.trade.target
        to_short = [i for i in target.loc[target < 0].index.values.tolist()]

        # Determine codes to open short
        to_open = list(set(to_sell) & set(to_short))

        if to_open:
            for code in to_open:

                # Determine desired quantity and value
                amount = delta[code]
                price = Currency.objects.get(code=code).get_latest_price(self.quote, 'bid')
                pos_value = amount * price

                # Account has a future wallet ?
                if 'future' in self.balances.columns.get_level_values(0):

                    # Future wallet requires additional free margin ?
                    free_margin = self.balances.future.free.quantity[self.quote]
                    if free_margin < pos_value:
                        log.warning('Free margin is needed to open {0} short'.format(code))
                        res = self.move_fund(self.quote, pos_value, 'spot')
                        if not res:
                            return

                else:
                    log.warning('Free margin is needed to open {0} short'.format(code))
                    res = self.move_fund(self.quote, pos_value, 'spot')
                    if not res:
                        return

                # Place order
                price -= (price * float(self.limit_price_tolerance))
                market = Market.objects.get(quote__code=self.quote,
                                            exchange=self.exchange,
                                            base__code=code,
                                            type='derivative',
                                            contract_type='perpetual'
                                            )

                self.place_order('open short', market, 'sell', amount, price)

    # Move funds between account wallets
    def move_fund(self, code, amount, to_wallet):

        client = self.exchange.get_ccxt_client(self)
        log.info('Transfer {0} {1} to {2} is needed'.format(round(amount, 4), code, to_wallet))

        # Determine candidate source wallets
        candidates = [i for i in self.exchange.get_wallets() if i != to_wallet]

        # Iterate through wallets and move available funds
        for wallet in list(set(self.balances.columns.get_level_values(0))):
            if wallet in candidates:

                # Determine quantities
                total = self.balances.loc[code, (wallet, 'total', 'quantity')]
                free = self.balances.loc[code, (wallet, 'free', 'quantity')]

                if not np.isnan(free):

                    # Preserve 1:1 margin if a position is open
                    if 'position' in self.balances.columns.get_level_values(0):
                        notional_values = self.balances[('position', 'open', 'value')].sum()
                        free = total - notional_values

                    if amount:
                        move = min(free, amount)

                        try:
                            client.transfer(code, move, wallet, to_wallet)
                        except ccxt.AuthenticationError:
                            log.error('Authentication error, can not move fund')
                            return
                        except Exception as e:
                            log.error('Transfer error: {0}'.format(e))
                            return
                        else:
                            log.info('Transfer of {0} {1} ({2} -> {3})'.format(round(amount, 2), code, wallet, to_wallet))

                        # Update amount
                        amount -= move

                else:
                    log.error('Transfer not possible from wallet {0}'.format(wallet))

        return True

    # Send order to an exchange and create order object
    def place_order(self, action, market, side, raw_amount, price, reduce_only=False, quote_order_qty=False):

        # Format decimals
        amount = format_decimal(counting_mode=self.exchange.precision_mode,
                                precision=market.precision['amount'],
                                n=raw_amount)

        # Test amount limits MIN and MAX
        if limit_amount(market, amount):

            # Test cost limits MIN and MAX
            cost = amount * price
            min_notional = limit_cost(market, cost)

            # If cost limit not satisfied and close short set reduce_only = True
            if not min_notional:
                if market.exchange.exid == 'binance':
                    if market.type == 'derivative':
                        if market.margined.code == 'USDT':
                            if action == 'close_short':
                                reduce_only = True

                # Else return
                if not reduce_only:
                    log.info('Cost conditions not satisfied to {0} {1} in {2}'.format(action,
                                                                                      market.base.code,
                                                                                      market.type))
                    return

            # Prepare order
            args = dict(
                symbol=market.symbol,
                type='limit' if self.limit_order else 'market',
                side=side,
                amount=amount,
                price=price
            )

            # Set parameters
            if quote_order_qty:
                args['price'] = None
                args['amount'] = None
                args['params'] = dict(quoteOrderQty=int(raw_amount))

            # Set parameters
            if reduce_only:
                if 'params' not in args:
                    args['params'] = dict(reduceonly=True)
                else:
                    args['params']['reduceonly'] = True

            # Place order
            client = self.exchange.get_ccxt_client(self)
            client.options['defaultType'] = market.wallet
            response = client.create_order(**args)

            # And create object
            self.create_update_order(response, action, market)

            log.info('Place order to {0} {3} {1} {2} market ({3})'.format(side,
                                                                          market.base.code,
                                                                          market.type,
                                                                          amount,
                                                                          action
                                                                          )
                     )

            print(market.type, 'order')
            pprint(args)

        else:
            log.info('Limit conditions not satisfied to {0} {1}'.format(action, market.base.code))

    # Query exchange and update open orders
    def update_orders(self):

        # Get client and iterate through wallets
        client = self.exchange.get_ccxt_client(account=self)
        for wallet in self.exchange.get_wallets():

            # Open orders ?
            orders = Order.objects.filter(account=self, market__wallet=wallet, status='open')
            if orders.exists():

                client.options['defaultType'] = wallet
                client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

                # Iterate through order
                for order in orders:

                    # Update order
                    responses = client.fetchOrder(id=order.orderid, symbol=order.market.symbol)
                    self.create_update_order(responses, action=order.action, market=order.market)

                    log.info('Order update', id=order.orderid, wallet=wallet)

            else:
                pass
                # log.info('No order to update in wallet {0}'.format(wallet))

    # Update order object
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
                        self.buy_spot()

    # Cancel an order by it's ID
    def cancel_order(self, wallet, symbol, orderid):
        client = self.exchange.get_ccxt_client(account=self)
        client.options['defaultType'] = wallet

        client.cancel_order(id=orderid, symbol=symbol)

        try:
            obj = Order.objects.get(orderid=orderid)
        except ObjectDoesNotExist:
            pass
        else:
            log.info('Cancel order {0}'.format(orderid))
            obj.status = 'canceled'
            obj.save()

    # Cancel all open orders
    def cancel_orders(self, user_orders=False):

        client = self.exchange.get_ccxt_client(account=self)

        # Iterate through wallets
        for wallet in self.exchange.get_wallets():

            client.options['defaultType'] = wallet
            client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

            # Cancel all orders including user orders ?
            if user_orders:
                responses = client.fetchOpenOrders()

                # Iterate through orders
                if responses:
                    log.info('Cancel {0} order(s) in {1}'.format(len(responses), wallet))
                    for order in responses:
                        self.cancel_order(wallet, order['symbol'], order['id'])

                else:
                    log.info('No order to cancel in wallet {0}'.format(wallet))

            # Only cancel tracked orders ?
            else:
                orders = Order.objects.filter(account=self,
                                              market__wallet=wallet,
                                              status='open'
                                              )
                # Iterate through orders
                if orders.exists():
                    log.info('Cancel {0} order(s)'.format(len(orders)))
                    for order in orders:
                        self.cancel_order(wallet, order.market.symbol, order.orderid)

                else:
                    log.info('No order to cancel in wallet {0}'.format(wallet))

    # Return True if a market has open order else false
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

    # Construct a fresh self.balances dataframe
    def create_balances(self):

        self.get_balances_qty()
        self.get_balances_value()
        self.get_positions_value()
        self.get_target_qty()
        self.get_delta()

    # Rebalance portfolio
    def trade(self):

        log.info('***')
        log.info('Start trade')
        log.info('***')

        # Cancel orders and create dataframe
        self.cancel_orders()
        self.create_balances()

        log.info('***')
        log.info('Free resources')
        log.info('***')

        # Free resources
        self.sell_spot()
        self.close_short()

        log.info('***')
        log.info('Update balances')
        log.info('***')

        # Update dataframe
        self.create_balances()

        log.info('***')
        log.info('Allocate funds')
        log.info('***')

        # Allocate funds
        self.buy_spot()
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
