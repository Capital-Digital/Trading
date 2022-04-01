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
    active = models.BooleanField(null=True, blank=False, default=False)
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

        client = self.exchange.get_ccxt_client(self)
        log.info('Get balances start')

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

        # Iterate through wallets, free, used and total quantities
        for wallet in list(set(self.balances.columns.get_level_values(0))):
            for tp in list(set(self.balances[wallet].columns.get_level_values(0))):
                funds = self.balances[wallet][tp]['quantity']
                for coin in funds.index:
                    price = Currency.objects.get(code=coin).get_latest_price(self.quote, 'last')
                    value = price * funds[coin]
                    self.balances.loc[coin, (wallet, tp, 'value')] = value

        # Drop dust < $10
        mask = self.balances.loc[:, self.balances.columns.get_level_values(2) == 'value'] > 1
        self.balances = self.balances.loc[(mask == True).any(axis=1)]
        self.save()

        log.info('Get balances done')

    # Fetch and update open positions in balances dataframe
    def get_positions_value(self):

        log.info('Get positions start')

        # Client client and query all futures positions
        client = self.exchange.get_ccxt_client(self)
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

        log.info('Get positions done')

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

        return account_value * target_pct

    # Returns a Series with target quantity
    def get_target_qty(self):

        target = self.get_target_value()
        for code in target.index:
            target[code] /= Currency.objects.get(code=code).get_latest_price(self.quote, 'last')

        return target

    # Calculate net exposure and delta
    def get_delta(self):

        log.info('Get delta start')
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
        log.info('Get delta done')

    # Sell in spot market
    def sell_spot(self):

        log.info(' ')
        log.info('Sell spot')
        log.info('*********')

        # Select codes to sell (exclude quote currency)
        delta = self.balances.account.trade.delta
        codes_to_sell = [i for i in delta.loc[delta > 0].index.values.tolist() if i != self.quote]
        codes_to_sell = [i for i in codes_to_sell if i in self.balances.spot.free.quantity.index.values.tolist()]

        # Codes should be sold ?
        if codes_to_sell:

            log.info('Codes in spot'.format(self.balances.spot.free.quantity.index.values.tolist()))
            log.info('Codes to sell {0}'.format(codes_to_sell))

            for code in codes_to_sell:

                log.info('-> {0}'.format(code))

                market = Market.objects.get(quote__code=self.quote,
                                            exchange=self.exchange,
                                            base__code=code,
                                            type='spot')

                # Don't sell spot if an order is open
                if not self.has_order(market):

                    # Select quantities
                    free = self.balances.spot.free.quantity[code]
                    target = self.balances.account.trade.target[code]
                    qty_delta = delta[code]

                    # Fund is not nan ?
                    if not np.isnan(free):

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

                        self.place_order('sell_spot', market, 'sell', amount, price)

                    else:
                        log.info('{0} is nan in spot'.format(code))
        else:
            log.info('No code to sell in spot')

    # Sell in derivative market
    def close_short(self):

        log.info(' ')
        log.info('Close short')
        log.info('***********')

        # Select codes to buy (exclude quote currency)
        delta = self.balances.account.trade.delta
        codes_to_buy = [i for i in delta.loc[delta < 0].index.values.tolist() if i != self.quote]

        if codes_to_buy:
            for code in codes_to_buy:

                market = Market.objects.get(quote__code=self.quote,
                                            exchange=self.exchange,
                                            base__code=code,
                                            type='derivative',
                                            contract_type='perpetual'
                                            )

                # Don't close short if an order is open
                if not self.has_order(market):

                    # Code is shorted now ?
                    if 'position' in self.balances.columns.get_level_values(0):
                        if self.balances.position.open.quantity[code] < 0:

                            log.info('-> {0}'.format(code))

                            # Get quantities
                            delta = abs(delta[code])
                            shorted = abs(self.balances.position.open.quantity[code])
                            amount = min(delta, shorted)

                            # Place buy order
                            price = Currency.objects.get(code=code).get_latest_price(self.quote, 'bid')
                            price -= (price * float(self.limit_price_tolerance))

                            self.place_order('close_short', market, 'buy', amount, price, reduce_only=True)
        else:
            log.info('No code to close short')

    # Buy in spot market
    def buy_spot(self):

        log.info(' ')
        log.info('Buy spot')
        log.info('********')

        # Select codes to buy (exclude quote currency)
        delta = self.balances.account.trade.delta
        codes_to_buy = [i for i in delta.loc[delta < 0].index.values.tolist() if i != self.quote]

        if codes_to_buy:
            for code in codes_to_buy:

                log.info('-> {0}'.format(code))

                market = Market.objects.get(quote__code=self.quote,
                                            exchange=self.exchange,
                                            base__code=code,
                                            type='spot'
                                            )

                # Don't buy spot if an order is open
                if not self.has_order(market):

                    # Determine missing quantity and it's dollar value
                    delta = abs(self.balances.account.trade.delta[code])
                    price = Currency.objects.get(code=code).get_latest_price(self.quote, 'ask')
                    delta_value = delta * price

                    # Cash is available in spot wallet ?
                    if self.quote in self.balances.index.values.tolist():
                        if 'spot' in self.balances.columns.get_level_values(0):
                            cash = self.balances.spot.free.quantity[self.quote]

                            print('\ncash: ', cash, '\n')

                            # Cash is not nan ?
                            if not np.isnan(cash):

                                # Not enough cash available?
                                if cash < delta_value:
                                    # log.info('Cash is needed to buy {0} spot'.format(code))
                                    desired = delta_value - cash
                                    moved = self.move_fund(self.quote, desired, 'spot')
                                    order_value = cash + moved

                                else:
                                    order_value = delta_value

                            else:
                                # log.info('Cash is needed to buy {0} spot'.format(code))
                                moved = self.move_fund(self.quote, delta_value, 'spot')
                                if not moved:
                                    continue
                                else:
                                    order_value = moved

                        else:
                            # log.info('Cash is needed to buy {0} spot'.format(code))
                            moved = self.move_fund(self.quote, delta_value, 'spot')
                            if not moved:
                                continue
                            else:
                                order_value = moved

                        # Place order
                        amount = order_value / price
                        price -= (price * float(self.limit_price_tolerance))

                        trade = self.place_order('buy_spot', market, 'buy', amount, price)
                        if trade:
                            self.create_balances()
                            log.info(' ')

                    else:
                        log.info('No cash found in account wallets')
        else:
            log.info('No code to buy in spot')

    # Sell in derivative market
    def open_short(self):

        log.info(' ')
        log.info('Open short')
        log.info('**********')

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

                log.info('-> {0}'.format(code))

                market = Market.objects.get(quote__code=self.quote,
                                            exchange=self.exchange,
                                            base__code=code,
                                            type='derivative',
                                            contract_type='perpetual'
                                            )

                # Don't open short if an order is open
                if not self.has_order(market):

                    # Determine desired quantity and value
                    amount = delta[code]
                    price = Currency.objects.get(code=code).get_latest_price(self.quote, 'bid')
                    pos_value = amount * price

                    # Check margin
                    if self.quote in self.balances.index.values.tolist():
                        if 'future' in self.balances.columns.get_level_values(0):

                            # Select free and total margin
                            free_margin = self.balances.future.free.quantity[self.quote]
                            total_margin = self.balances.future.total.quantity[self.quote]

                            # Free margin is not nan ?
                            if not np.isnan(free_margin):

                                # If a position is already open in another market
                                # then reserve notional value as margin to maintain 1:1 ratio
                                if 'position' in self.balances.columns.get_level_values(0):
                                    notional_values = abs(self.balances[('position', 'open', 'value')]).sum()
                                    free_margin = max(0, total_margin - notional_values)

                                # Determine order value
                                if free_margin < pos_value:
                                    # log.info('Margin is needed to open {0} short'.format(code))
                                    desired = pos_value - free_margin
                                    moved = self.move_fund(self.quote, desired, 'future')
                                    order_value = free_margin + moved

                                else:
                                    order_value = free_margin

                            else:
                                # log.info('Free margin is needed to open {0} short'.format(code))
                                moved = self.move_fund(self.quote, pos_value, 'future')
                                if not moved:
                                    continue
                                else:
                                    order_value = moved

                        else:
                            # log.info('Free margin is needed to open {0} short'.format(code))
                            moved = self.move_fund(self.quote, pos_value, 'future')
                            if not moved:
                                continue
                            else:
                                order_value = moved

                        # Place order
                        amount = order_value / price
                        price -= (price * float(self.limit_price_tolerance))

                        trade = self.place_order('open_short', market, 'sell', amount, price)
                        if trade:
                            self.create_balances()
                            log.info(' ')

                    else:
                        log.info('No cash found in account wallets')
        else:
            log.info('No code to open short')

    # Move funds between account wallets
    def move_fund(self, code, desired, to_wallet):

        log.info('{0} {1} is needed in {2}'.format(round(desired, 4), code, to_wallet))

        client = self.exchange.get_ccxt_client(self)
        moved = 0

        # Determine candidates for source wallet
        candidates = [i for i in self.exchange.get_wallets() if i != to_wallet]
        candidates = list(set(candidates) & set(list(set(self.balances.columns.get_level_values(0)))))

        if candidates:

            # Iterate through wallets and move available funds
            for wallet in candidates:

                # Determine free resource
                total = self.balances.loc[code, (wallet, 'total', 'quantity')]
                free = self.balances.loc[code, (wallet, 'free', 'quantity')]

                if not np.isnan(free):

                    log.info('Wallet {0} has {1} {2}'.format(wallet, round(free, 2), code))

                    # Wallet is derivative test a position is open ?
                    if wallet != 'spot' and 'position' in self.balances.columns.get_level_values(0):
                        # Reserve notional value as margin to maintain 1:1 ratio
                        notional_values = abs(self.balances[('position', 'open', 'value')]).sum()
                        free = max(0, total - notional_values)
                        log.info('Wallet {0} has {1} {2} of free margin'.format(wallet, round(free, 2), code))

                    # Determine maximum amount that can be moved
                    movable = min(free, desired)
                    if movable > 0.5:

                        try:
                            client.transfer(code, movable, wallet, to_wallet)

                        except ccxt.AuthenticationError:
                            log.error('Authentication error, can not move fund')
                            return

                        except Exception as e:
                            log.error('Transfer error: {0}'.format(e))
                            continue

                        else:
                            # Update funds moved and desired
                            moved += movable
                            desired -= movable

                            log.info('Transfer of {0} {1} done'.format(round(movable, 2), code))

                            # All fund have been moved ?
                            if not desired:
                                log.info('Transfert complete')
                                return moved
                    else:
                        log.info('Fund available is less than $0.5')

                else:
                    log.info('Wallet {0} has 0 {1}'.format(wallet, code))
                    continue

        else:
            log.info('Source wallet not found')

        if moved:
            return moved

        else:
            log.error('No fund transferred')
            return 0

    # Send order to an exchange and create order object
    def place_order(self, action, market, side, raw_amount, price, reduce_only=False):

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
                    log.info('Cost not satisfied to {0} {2} {1}'.format(action,
                                                                        market.base.code,
                                                                        amount,
                                                                        ))
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
            if reduce_only:
                args['params'] = dict(reduceonly=True)

            # Place order
            client = self.exchange.get_ccxt_client(self)
            client.options['defaultType'] = market.wallet

            log.info('Place order to {0} {1} {2} in {3}'.format(side,
                                                                amount,
                                                                market.base.code,
                                                                market.type
                                                                )
                     )

            print('\nFree quantity\n')
            print(self.balances.spot.free.quantity)
            print('\nMarket:', market.type, '\n')
            pprint(args)

            response = client.create_order(**args)

            # And create object
            trade = self.create_update_order(response, action, market)
            if trade:
                return True

        else:
            log.info('Limit not satisfied to {0} {2} {1}'.format(action, round(amount, 3), market.base.code))

    # Fetch open orders
    def fetch_open_orders(self):

        # Iterate through wallets
        trades = []
        client = self.exchange.get_ccxt_client(account=self)
        for wallet in self.exchange.get_wallets():

            # Open orders ?
            orders = Order.objects.filter(account=self,
                                          market__wallet=wallet,
                                          status='open'
                                          )
            if orders.exists():

                # Set options
                client.options['defaultType'] = wallet
                client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

                for order in orders:
                    responses = client.fetchOrder(id=order.orderid, symbol=order.market.symbol)
                    trade = self.create_update_order(responses, action=order.action, market=order.market)
                    trades.append(trade)

        # If resource is liberated after trades occurred then return True
        if True in trades:
            return True

    # Update an order object
    def create_update_order(self, response, action, market):

        try:
            # Select order object
            args = dict(account=self,
                        market=market,
                        orderid=response['id']
                        )
            order = Order.objects.get(**args)

        except ObjectDoesNotExist:
            pass

        finally:

            # Data to be inserted
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

            # Create of update object
            obj, created = Order.objects.update_or_create(**args, defaults=defaults)

            # New order ?
            if created:

                log.info('Place order success', id=response['id'])

                # Trade occurred ?
                if float(response['filled']):
                    log.info('Trade detected')
                    return True
                else:
                    log.info('No trade detected')

            else:

                # Action is to liberate resources ?
                if action in ['sell_spot', 'close_short']:

                    # New trades occurred since last update ?
                    filled = float(response['filled']) - order.filled
                    if filled > 0:

                        log.info('Update {0} order'.format(market.base.code), account=self.name, id=response['id'])
                        log.info('Trade detected', account=self.name)
                        log.info('Order filled at {0}%'.format(round(filled / order.amount, 3) * 100),
                                 account=self.name)

                        return True

    # Cancel an order by its ID
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

                    log.info(' ')
                    log.info('Cancel app and user orders')
                    log.info('**************************')

                    for order in responses:
                        log.info('Cancel order {0}'.format(order['id']))
                        self.cancel_order(wallet, order['symbol'], order['id'])

            # Only cancel tracked orders ?
            else:
                orders = Order.objects.filter(account=self,
                                              market__wallet=wallet,
                                              status='open'
                                              )
                # Iterate through orders
                if orders.exists():

                    log.info(' ')
                    log.info('Cancel app orders')
                    log.info('*****************')

                    for order in orders:
                        log.info('Cancel order {0}'.format(order.orderid))
                        self.cancel_order(wallet, order.market.symbol, order.orderid)

    # Return True if a market has open order else false
    def has_order(self, market):
        client = self.exchange.get_ccxt_client(self)
        client.options['defaultType'] = market.wallet
        orders = client.fetchOpenOrders(market.symbol)

        if orders:
            log.info('Order is already open in {0} {1}'.format(market.symbol, market.type))
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

        log.info(' ')
        log.info('Create balances dataframe')
        log.info('*************************')

        self.get_balances_qty()
        self.get_balances_value()
        self.get_positions_value()
        self.get_target_qty()
        self.get_delta()

    # Mark the account as currently trading (busy) or not
    def set_busy_flag(self, busy):

        log.info(' ')

        if busy:
            log.info('Set busy flag = True')
        else:
            log.info('Set busy flag = False')

        self.trading = busy
        self.save()

    # Rebalance portfolio
    def trade(self, cancel=True):

        log.info(' ')
        log.info(' ')
        log.info('Start trading with account : {0}'.format(self.name))
        log.info('##########################')
        log.info(' ')
        log.info(' ')

        log.bind(account=self.name)

        # Mark account are busy
        self.set_busy_flag(True)

        if cancel:
            self.cancel_orders()

        # Create dataframe and free resources
        self.create_balances()
        self.sell_spot()
        self.close_short()

        # Update dataframe and allocate funds
        self.create_balances()
        self.buy_spot()
        self.open_short()

        # Mark the account as not busy
        self.set_busy_flag(False)

        log.unbind('account')

        log.info(' ')
        log.info('End trading with account : {0}'.format(self.name))
        log.info('-------------------------------------------')
        log.info(' ')


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
