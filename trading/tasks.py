from __future__ import absolute_import, unicode_literals
from datetime import datetime, date, timedelta
from django.db.models import Q
from django.utils import timezone
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
import capital.celery as celery
from celery import chain, group, shared_task, Task
from marketsdata.models import Market, Candle, Currency, Exchange
from strategy.models import Allocation, Strategy
from trading.methods import target_size_n_side, format_decimal, get_spot_balance_used, get_position_size, \
    get_spot_balance_free, calculate_target_quantity
from trading.models import Account, Order, Fund, Position
from trading import methods
from trading import error
from capital.error import *
from itertools import chain as itertools_chain
from itertools import accumulate
import ccxtpro, ccxt
import asyncio
import pandas as pd
import sys, traceback, time
import structlog
from pprint import pprint

log = structlog.get_logger(__name__)


class BaseTaskWithRetry(Task):
    autoretry_for = (ccxt.DDoSProtection,
                     ccxt.RateLimitExceeded,
                     ccxt.RequestTimeout,
                     ccxt.ExchangeNotAvailable,
                     ccxt.NetworkError)

    retry_kwargs = {'max_retries': 5, 'default_retry_delay': 2}
    retry_backoff = True
    retry_backoff_max = 30
    retry_jitter = False


# Fetch open orders for all symbols of a market type
@shared_task(name='Trading_____Fetch open orders by account', base=BaseTaskWithRetry)
def fetch_order_open(account):
    account = Account.objects.get(name=account)
    if account.exchange.has['fetchOpenOrders']:

        log.bind(account=account.name, exchange=account.exchange.exid)
        client = account.exchange.get_ccxt_client(account=account)
        default_type = None

        # Set market type
        if account.exchange.default_types:
            # Select default_type of this account
            default_type = list(set(Market.objects.filter(exchange=account.exchange,
                                                          derivative=account.derivative,
                                                          type=account.type,
                                                          margined=account.margined,
                                                          active=True,
                                                          ).values_list('default_type', flat=True)))[0]

            client.options['defaultType'] = default_type

        # Disable Binance warning
        if account.exchange.exid == 'binance':
            client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

        log.info('Fetch all open orders')

        # fetch OKEx orders one by one
        ##############################
        if account.exchange.exid in ['okex']:

            instrument_ids = Market.objects.filter(exchange=account.exchange,
                                                   type=account.type,
                                                   derivative=account.derivative,
                                                   margined=account.margined
                                                   ).values_list('response__id', flat=True)

            for instrument_id in instrument_ids:

                # Check credit, fetch and insert open orders
                if account.exchange.has_credit():
                    response = client.fetchOpenOrders(instrument_id)
                    account.exchange.update_credit('fetchOpenOrders', default_type)

                    if response:

                        for order in response:
                            methods.order_create_update(account, order, default_type)

        # fetch others exchanges orders
        ###############################
        else:

            # Check credit and fetch open orders
            if account.exchange.has_credit():
                response = client.fetchOpenOrders()
                account.exchange.update_credit('fetchAllOpenOrders', default_type)

            for order in response:
                methods.order_create_update(account, order, default_type)

    else:
        raise MethodUnsupported('Method fetchOpenOrders not supported for exchange'.format(account.exchange.exid))


# Fetch past orders for a specific period of time
@shared_task(name='Trading_____Fetch orders by date', base=BaseTaskWithRetry)
def fetch_order_past(account, market, timestamp):
    account = Account.objects.get(name=account)
    log.bind(account=account.name, exchange=account.exchange.exid)

    if account.exchange.has['fetchOrders']:

        # Initialize client
        client = account.exchange.get_ccxt_client(account)

        if market.default_type:
            client.options['defaultType'] = market.default_type

        log.info('Fetch orders for {0}'.format(market.symbol))

        # Check credit and fetch orders
        if account.exchange.has_credit():
            response = client.fetch_orders(market.symbol, since=timestamp)
            account.exchange.update_credit('fetch_orders', market.default_type)

        # Insert orders
        for order in response:
            methods.order_create_update(account, order, market.default_type)

    else:
        raise Exception('Methode fetchOrders is not supported by {0}'.format(account.exchange.name))


# Fetch an orderId (reload = True)
@shared_task(name='Trading_____Fetch order by ID', base=BaseTaskWithRetry)
def fetch_order_id(account, orderid):
    account = Account.objects.get(name=account)
    log.bind(account=account.name, exchange=account.exchange.exid, orderId=orderid)

    try:
        # First select object
        order = Order.objects.get(account=account, orderId=orderid)

    except ObjectDoesNotExist:
        log.error('Unable to select order object')

    else:
        client = account.exchange.get_ccxt_client(account)

        if order.market.default_type:
            client.options['defaultType'] = order.market.default_type

        # check if method is supported
        if account.exchange.has['fetchOrder']:

            params = None

            # OKEx specific
            if account.exchange.exid == 'okex':
                params = dict(instrument_id=order.market.info['instrument_id'], order_id=orderid)

            log.info('Fetch order')

            # Check credit and insert order
            if account.exchange.has_credit():
                response = client.fetchOrder(id=orderid, symbol=order.market.symbol, params=params)
                account.exchange.update_credit('fetchOrder', order.market.default_type)
                methods.order_create_update(account, response, order.market.default_type)

        else:
            raise Exception('Methode fetchOrder is not supported by {0}'.format(account.exchange.name))


# Cancel order by orderId
@shared_task(name='Trading_____Cancel order by ID', base=BaseTaskWithRetry)
def cancel_order_id(account, orderid):
    account = Account.objects.get(name=account)
    log.bind(account=account.name, exchange=account.exchange.exid, orderId=orderid)

    try:
        # First select object
        order = Order.objects.get(orderId=orderid)

    except ObjectDoesNotExist:
        log.error('Unable to select order object')

    else:
        client = account.exchange.get_ccxt_client(account)

        if order.market.default_type:
            client.options['defaultType'] = order.market.default_type

        log.info('Cancel order')

        # Check credit and cancel order
        if account.exchange.has_credit():
            client.cancel_order(id=order.orderId, symbol=order.market.symbol)
            account.exchange.update_credit('cancel_order', order.market.default_type)


# Place an order to the market after an object is created
@shared_task(name='Trading_____Place order by ID', base=BaseTaskWithRetry)
def order_place(account, orderid):
    account = Account.objects.get(name=account)
    log.bind(account=account.name, exchange=account.exchange.exid)

    try:
        # First select object
        order = Order.objects.get(orderId=orderid)

    except ObjectDoesNotExist:
        log.error('Unable to select order object')

    else:
        client = account.exchange.get_ccxt_client(account)

        if order.market.default_type:
            client.options['defaultType'] = order.market.default_type

        log.info('Place order')

        args = []

        symbol = args['symbol']
        order_type = args['type']
        clientOrderId = args['params']['newClientOrderId']

        # Specific to OKEx
        # if order.account.exchange.exid == 'okex':
        #     # Set params
        #     if order.account.limit_order:
        #         args['params'] = {'order_type': '0'}
        #     else:
        #         args['params'] = {'order_type': '4'}
        #     # Rewrite type
        #     args['type'] = 1 if order.type == 'open_long' else 2 if order.type == 'open_short' \
        #         else 3 if order.type == 'close_long' else 4 if order.type == 'close_short' else None

        # Place limit or market order
        if order_type == 'limit':

            if account.exchange.has['createLimitOrder']:
                args['price'] = 0
                response = client.create_order(**args)
            else:
                raise MethodUnsupported('Limit order not supported with'.format(account.exchange.exid))
        else:
            if account.exchange.has['createMarketOrder']:
                response = client.create_order(**args)
            else:
                raise MethodUnsupported('Market order not supported with'.format(account.exchange.exid))

        # if account.exchange.exid == 'okex':
        #     if response['info']['error_code'] == '0':
        #         order.refresh()
        #     else:
        #         log.error('Error code is not 0', account=account.name, args=args)
        #         pprint(response)

        print('response')

        if response['id']:

            order.orderId = response['id']
            order.price_average = response['average']
            order.fee = response['fee']
            order.filled = response['filled']
            order.price = response['price']
            order.status = response['status']

            if 'clientOrderId' in response:
                order.clientOrderId = response['clientOrderId']

            log.info('Order ID {0} placed'.format(order.orderId), account=order.account.name)

        else:
            log.error('Order ID unknown')

        order.response = response
        order.save()
        log.info('Order ID {0} object saved'.format(order.orderId), account=order.account.name)


# Fetch balance and create fund object
@shared_task(name='Trading_____Create_fund', base=BaseTaskWithRetry)
def create_fund(account):
    log.bind(account=account)
    account = Account.objects.get(name=account)

    log.info('Fetch balance')
    client = account.exchange.get_ccxt_client(account)

    # add 1h because the balance is fetched at :59
    dt = timezone.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    # Returns a dictionary with assets quantity > 0
    def create_dict(response):

        total, free, used = [dict() for _ in range(3)]

        # Select total quantities > 0
        for k, v in response['total'].items():
            if v > 0:
                code = dict()
                code[k] = dict(quantity=v, value=calculate_value(k, v))
                total.update(code)

        for k, v in response['free'].items():
            if response['total'][k] > 0:
                code = dict()
                code[k] = dict(quantity=v, value=calculate_value(k, v))
                free.update(code)

        for k, v in response['used'].items():
            if response['total'][k] > 0:
                code = dict()
                code[k] = dict(quantity=v, value=calculate_value(k, v))
                used.update(code)

        return total, used, free

    # Create Fund objects
    def create_fund(total, free, used, derivative):

        kwargs = dict(
            account=account,
            exchange=account.exchange,
            balance=sum(value['value'] for key in total.keys() for value in total[key].values()),
            derivative=derivative,
            total=total,
            used=used,
            free=free,
            dt=dt
        )
        try:
            Fund.objects.get(account=account,
                             exchange=account.exchange,
                             dt=dt
                             )

        except ObjectDoesNotExist:
            Fund.objects.create(**kwargs)

    # Calculate value in dollar_currency
    def calculate_value(code, quantity):

        # Test if code is a currency or an instrument (OKEx)
        if Currency.objects.filter(code=code).exists():

            # Convert base quantity in USD
            if not Currency.objects.get(code=code).stable_coin:
                from django.db.models import Q

                # Select spot market to prevent MultipleObjects
                market = Market.objects.filter(Q(default_type='spot') | Q(default_type=None)).get(
                    exchange=account.exchange,
                    base__code=code,
                    quote__code=account.exchange.dollar_currency
                )
                return quantity * market.get_candle_price_last()

            else:
                return quantity

        else:
            market = Market.objects.get(exchange=account.exchange, symbol=code)

            # Return quantity is market is marginated in USDT
            if market.margined.stable_coin:
                return quantity

            else:
                # Else it's marginated in base currency
                return quantity * market.get_candle_price_last()

    # Create Position objects
    def get_derivative(response, default_type=None):

        if account.exchange.exid == 'binance':
            if default_type in ['future', 'delivery']:
                return [i for i in response['info']['assets'] if float(i['walletBalance']) > 0]

        elif account.exchange.exid == 'bybit':
            return [v for k, v in response['info']['result'].items() if v['wallet_balance'] > 0]

        return None

    # Create empty dictionaries
    total, used, free, derivative = [dict() for _ in range(4)]

    if account.exchange.default_types:

        for default_type in account.exchange.get_default_types():
            log.bind(defaultType=default_type)

            client.options['defaultType'] = default_type

            if account.exchange.has_credit(default_type):
                response = client.fetchBalance()
                account.exchange.update_credit('fetchBalance', default_type)

                t, u, f = create_dict(response)

                total[default_type] = t
                used[default_type] = u
                free[default_type] = f

                derivative[default_type] = get_derivative(response, default_type)

        create_fund(total, free, used, derivative)

    else:

        default_type = 'default'

        if account.exchange.has_credit():
            response = client.fetchBalance()
            account.exchange.update_credit('fetchBalance')

            t, u, f = create_dict(response)

            total[default_type] = t
            used[default_type] = u
            free[default_type] = f

            derivative[default_type] = get_derivative(response)

            create_fund(total, free, used, derivative)


# Create, update or delete objects
@shared_task(name='Trading_____Update position', base=BaseTaskWithRetry)
def update_positions(account):
    log.bind(account=account.name)
    account = Account.objects.get(name=account)

    client = account.exchange.get_ccxt_client(account)

    # Create/update object of an open position
    def create_update(market, defaults):

        # create search arguments
        args = dict(exchange=account.exchange, account=account, market=market)

        try:
            Position.objects.get(**args)

        except Position.DoesNotExist:
            args.update(defaults)
            Position.objects.create(**args)
            log.info('Position object created for {0}'.format(market.symbol))

        else:
            Position.objects.update_or_create(**args, defaults=defaults)
            log.info('Position object updated for {0}'.format(market.symbol))

    # Delete object of a closed position
    def delete(position):

        try:
            obj = Position.objects.get(account=account,
                                       exchange=account.exchange,
                                       market__type=account.type,
                                       market__derivative=account.derivative,
                                       market__response__id=position['symbol']
                                       )

        except ObjectDoesNotExist:
            pass

        else:
            obj.delete()
            log.info('Position object deleted for {0}'.format(position['symbol']))

    # OKEx
    ######
    if account.exchange.exid == 'okex':

        # fetch Okex perpetual positions
        def okex_swap():

            if account.exchange.has_credit():

                response = client.swapGetPosition()
                account.exchange.update_credit('swapGetPosition', 'swap')

                # Construct dictionary
                for position in response[0]['holding']:

                    try:
                        # First select market
                        market = Market.objects.get(exchange=account.exchange,
                                                    type='derivative',
                                                    derivative='perpetual',
                                                    response__id=position['instrument_id']
                                                    )

                    except ObjectDoesNotExist:

                        log.error('Unable to select {0}'.format(position['instrument_id']))
                        continue

                    else:

                        size = float(position['position'])  # contract qty
                        side = 'buy' if position['side'] == 'long' else 'sell'
                        last = float(position['last'])
                        size = abs(size)

                        # calculate position value in USDT
                        if market.contract_value_currency.stable_coin:
                            value = size * market.contract_value
                        else:
                            value = size * market.contract_value * last

                        defaults = dict(
                            size=size,
                            side=side,
                            last=last,
                            value_usd=value,
                            entry_price=float(position['avg_cost']),
                            liquidation_price=float(position['liquidation_price']),
                            leverage_max=position['leverage'],
                            margin_mode='crossed' if response[0]['margin_mode'] == 'crossed' else 'isolated',
                            margin_maint_ratio=float(position['maint_margin_ratio']),
                            realized_pnl=float(position['realized_pnl']),
                            unrealized_pnl=float(position['unrealized_pnl']),
                            response=position
                        )

                        create_update(market, defaults)

        # fetch Okex futures positions
        def okex_futures():
            log.info('OKEx futures not supported')

        okex_swap()
        okex_futures()

    # Binance
    #########
    if account.exchange.exid == 'binance':

        # fetch Binance USDT-margined positions
        def binance_usd_margined():

            if not account.exchange.has_credit():
                return

            response = client.fapiPrivateGetPositionRisk()
            account.exchange.update_credit('positionRisk', 'future')

            # Select long and short positions
            positions_open = [i for i in response if float(i['positionAmt']) != 0]
            positions_close = [i for i in response if float(i['positionAmt']) == 0]

            # Create dictionary of open positions
            for position in positions_open:

                try:
                    market = Market.objects.get(exchange=account.exchange,
                                                type='derivative',
                                                response__id=position['symbol']
                                                )

                except ObjectDoesNotExist:

                    log.error('Unable to select {0} and update position'.format(position['symbol']))
                    continue

                else:

                    size = float(position['positionAmt'])
                    side = 'buy' if size > 0 else 'sell'
                    size = abs(size)

                    # calculate position value in USDT
                    value = size * market.contract_value

                    defaults = dict(
                        size=size,
                        side=side,
                        value_usd=round(value, 2),
                        last=float(position['markPrice']),
                        leverage_max=float(position['leverage']),
                        entry_price=float(position['entryPrice']),
                        unrealized_pnl=float(position['unRealizedProfit']),
                        liquidation_price=float(position['liquidationPrice']),
                        margin_mode='crossed' if position['marginType'] == 'cross' else 'isolated',
                        response=position
                    )

                    create_update(market, defaults)

            # Finally delete object of closed positions
            if Position.objects.filter(account=account).exists():
                for position in positions_close:
                    delete(position)

        # fetch Binance COIN-margined positions
        def binance_coin_margined():

            if not account.exchange.has_credit():
                return

            response = client.dapiPrivateGetPositionRisk()
            account.exchange.update_credit('positionRisk', 'delivery')

            # Select long and short open positions
            positions_open = [i for i in response if float(i['positionAmt']) != 0]
            positions_close = [i for i in response if float(i['positionAmt']) == 0]

            # Create dictionary of open positions
            for position in positions_open:

                try:
                    market = Market.objects.get(exchange=account.exchange,
                                                type='derivative',
                                                response__id=position['symbol']
                                                )

                except ObjectDoesNotExist:

                    log.error('Unable to select {0}'.format(position['symbol']))
                    continue

                else:

                    size = float(position['notionalValue'])
                    side = 'buy' if size > 0 else 'sell'
                    size = abs(size)

                    # calculate position value in USDT
                    value = size * market.contract_value * float(position['markPrice'])

                    defaults = dict(
                        size=size,
                        side=side,
                        value_usd=round(value, 2),
                        last=float(position['markPrice']),
                        leverage_max=float(position['leverage']),
                        entry_price=float(position['entryPrice']),
                        unrealized_pnl=float(position['unRealizedProfit']),
                        liquidation_price=float(position['liquidationPrice']),
                        margin_mode='crossed' if position['marginType'] == 'cross' else 'isolated',
                        max_qty=float(position['maxQty']),  # defines the maximum quantity allowed
                        response=position
                    )

                    create_update(market, defaults)

            # Finally delete object of closed positions
            if Position.objects.filter(account=account).exists():
                for position in positions_close:
                    delete(position)

        binance_usd_margined()
        binance_coin_margined()

    # Bybit
    #######
    if account.exchange.exid == 'bybit':

        # Fetch Bybit USDT-marginated positions
        def bybit_usdt_margined():

            if not account.exchange.has_credit():
                return

            response = client.privateLinearGetPositionList()
            account.exchange.update_credit('positionList')

            # Select long and short positions
            positions_open = [i['data'] for i in response['result'] if i['data']['size'] > 0]
            positions_close = [i['data'] for i in response['result'] if i['data']['size'] == 0]

            # Create dictionary of open positions
            for position in positions_open:

                try:
                    # First select market
                    market = Market.objects.get(exchange=account.exchange,
                                                type='derivative',
                                                response__id=position['symbol']
                                                )

                except ObjectDoesNotExist:

                    log.error('Unable to select {0} and update position'.format(position['symbol']))
                    continue

                else:

                    size = position['size']
                    side = position['side'].lower()

                    defaults = dict(
                        size=size,
                        side=side,
                        value_usd=position['position_value'],
                        leverage_max=float(position['leverage']),
                        entry_price=float(position['entry_price']),
                        realized_pnl=float(position['realised_pnl']),
                        unrealized_pnl=float(position['unrealised_pnl']),
                        liquidation_price=float(position['liq_price']),
                        margin_mode='isolated' if position['is_isolated'] else 'crossed',
                        margin=position['position_margin'],
                        response=position
                    )

                    create_update(market, defaults)

            # Delete object of closed positions
            for position in positions_close:
                if Position.objects.filter(account=account,
                                           side=position['side'].lower(),
                                           market__response__id=position['symbol']).exists():
                    delete(position)

        # Fetch Bybit COIN-margined positions
        def bybit_coin_margined():

            if not account.exchange.has_credit():
                return

            response = client.v2PrivateGetPositionList()
            account.exchange.update_credit('positionList')

            # Select long and short positions
            positions_open = [i['data'] for i in response['result'] if i['data']['size'] > 0]
            positions_close = [i['data'] for i in response['result'] if i['data']['size'] == 0]

            # Create dictionary of open positions
            for position in positions_open:

                try:
                    # First select market
                    market = Market.objects.get(exchange=account.exchange,
                                                type='derivative',
                                                response__id=position['symbol']
                                                )

                except ObjectDoesNotExist:

                    log.error('Unable to select {0} and update position'.format(position['symbol']))
                    continue

                else:

                    size = position['size']
                    side = position['side'].lower()

                    defaults = dict(
                        size=size,
                        side=side,
                        value_usd=position['size'],  # Position size in USD
                        leverage=float(position['effective_leverage']),
                        leverage_max=float(position['leverage']),
                        entry_price=float(position['entry_price']),
                        realized_pnl=float(position['realised_pnl']),
                        unrealized_pnl=float(position['unrealised_pnl']),
                        liquidation_price=float(position['liq_price']),
                        margin_mode='isolated' if position['is_isolated'] else 'crossed',
                        margin=position['position_margin'],
                        response=position
                    )

                    create_update(market, defaults)

            # Delete object of closed positions
            for position in positions_close:
                if Position.objects.filter(account=account,
                                           side=position['side'].lower(),
                                           market__response__id=position['symbol']).exists():
                    delete(position)

        bybit_usdt_margined()
        bybit_coin_margined()


# Trade with account
# @shared_task(name='account_trade', base=BaseTaskWithRetry)
def trade(account):
    # Check account
    if not account.is_valid_credentials():
        raise error.InvalidCredentials(
            'Account {0} has invalid credentials {1}'.format(account.name, account.api_key))
    if not account.trading:
        raise error.InactiveAccount(
            'Account trading is deactivated for {0}'.format(account.name))

    # Check exchange
    if not account.exchange.is_active():
        raise TradingError(
            'Exchange {1} of account {0} is inactive'.format(account.name, account.exchange.exid))

    # Fire an exception if account is not compatible with the strategy
    account.is_compatible()

    # Check strategy update
    if not account.strategy.is_updated():
        raise error.StrategyNotUpdated(
            'Cannot update account {0} ''strategy {1} is not updated'.format(account.name, account.strategy.name))

    # Check time
    # if timezone.now().hour not in account.strategy.get_hours():
    #     raise WaitUpdateTime('Account {0} does not need to trade now'.format(account.name))

    # get allocations
    allocations = Allocation.objects.filter(strategy=account.strategy, dt=account.strategy.get_latest_alloc_dt())

    # calculate delta between open position and target
    def delta_size(allocation, position):
        size, side = target_size_n_side(account, allocation)
        delta = size - float(position.size)
        amount = float(format_decimal(abs(delta), position.market.precision['amount'], account))
        if delta < 0:
            return -amount  # return a negative amount if contracts need to be removed from position
        elif delta > 0:
            return amount

    return

    # close open positions if necessary (i.e opposite side or undesired market)
    ###########################################################################
    for position in account.positions.all():
        if position.market in markets_marg:
            if position.side != alloc.side:
                position.close()
        else:
            position.close()

    # remove contracts from open positions if necessary
    ###################################################
    for position in account.positions.all():
        if position.market in markets_marg:
            if position.side == alloc.side:
                delta = delta_size(alloc, position)
                if delta < 0:
                    position.remove(-delta)

    # add contracts to an open position
    ###################################
    for position in account.positions.all():
        if position.market in markets_marg:
            if position.side == alloc.side:
                delta = delta_size(alloc, position)
                if delta > 0:
                    position.add(delta)

    # or create a new position if necessary
    #######################################
    for market in markets_marg:
        try:
            Position.objects.get(account=account, market=market)
        except ObjectDoesNotExist:
            account.create_update_delete_position(market)
        except MultipleObjectsReturned:
            pass
        else:
            pass

    # determine long only bases that need to be bought
    buy = []
    for alloc in allocations:
        if not alloc.margin:
            buy.append(alloc.market.base.code)

    # determine long only bases that need to be sold
    sell = []
    for fund in Fund.objects.filter(account=account, dt=dt, type='spot'):
        if not alloc.margin:
            sell.append(alloc.market.base.code)


# Fetch order book every x seconds
# @shared_task(bind=True, name='trade_ws_account')
def trade_ws_account(self, account):
    log.info('Trade with account {0}'.format(account))
    account = Account.objects.get(name=account)
    exchange = account.exchange

    # return cumulative orderbook
    def cumulative_book(ob):
        asks = ob['asks']
        bids = ob['bids']
        asks_p = [a[0] for a in asks]
        bids_p = [a[0] for a in bids]
        cum_a = list(accumulate([a[1] for a in asks]))
        cum_b = list(accumulate([a[1] for a in bids]))
        return [[bids_p[i], cum_b[i]] for i, a in enumerate(bids)], \
               [[asks_p[i], cum_a[i]] for i, a in enumerate(asks)]

    # return bid quantity available at a limited price
    def get_bid_quantity(book, limit):
        best = book[0][0]
        price_limit = best - best * limit

        if price_limit < book[-1][0]:
            raise Exception('Increase orderbook limit over {0}'.format(account.exchange.orderbook_limit))

        # filter
        bid_filtered = [bid for bid in book if bid[0] > price_limit]

        # convert base qty in quote
        return bid_filtered[-1][1] * best

    # return ask quantity available at a limited price
    def get_ask_quantity(book, limit):
        best = book[0][0]
        price_limit = best + best * limit

        if price_limit > book[-1][0]:
            raise Exception('Increase orderbook limit over {0}'.format(account.exchange.orderbook_limit))

        # filter
        ask_filtered = [ask for ask in book if ask[0] < price_limit]

        # convert base qty in quote
        return ask_filtered[-1][1] * best

    # determine average buy/sell price based on desired amount
    def get_prices(book, amount):
        for i, b in enumerate(book):
            if b[1] > amount:
                if i == 0:
                    return book[0][0], book[0][0]
                else:
                    ob = book[:i]  # select the first n elements needed
                    break
        prices = [p[0] for p in ob]  # select prices
        qty = sum([q[1] for q in ob])  # sum total quantity needed
        weights = [q[1] / qty for q in ob]  # weight each element
        price = sum([a * b for a, b in zip(prices, weights)])  # multiply prices by weights and sum
        best = book[0][0]
        return price, best

    # return a list of symbols with open orders
    def get_symbols_with_open_orders():

        bases = [f.currency.code for f in account.get_funds('spot')]
        used = [get_spot_balance_used(account, base) for base in bases]
        print('get_symbols_with_open_orders()', used)
        return used

    # sort bases currencies by group (Ie. hold, buy, close_long, close_short, etc)
    def get_instructions_table():

        rows = []
        # select bases in spot account
        bases_spot = [f.currency for f in account.get_funds('spot')
                      if f.type in ['spot', None] and f.currency.code != 'USDT']

        # select bases in open positions
        bases_posi_long = [p.market.base for p in account.get_positions_long()]
        bases_posi_short = [p.market.base for p in account.get_positions_short()]

        # select bases to trade with margin (short) or without (long)
        bases_alloc_no_margin = [a.market.base for a in (account.bases_alloc_no_margin())]
        bases_alloc_margin = [a.market.base for a in account.bases_alloc_margin()]

        if bases_spot:
            for base in bases_spot:
                qty = get_position_size(account, base) + get_spot_balance_free(account, base)
                if base in bases_alloc_no_margin:
                    qty_needed = calculate_target_quantity(account, exchange, base)
                    if qty_needed > qty:  # bases to increase
                        rows.append(dict(base=base.code, action='buy', qty=qty_needed - qty))
                    elif qty_needed < qty:  # bases to decrease
                        rows.append(dict(base=base.code, action='sell_spot', qty=qty - qty_needed))
                else:
                    # spot to sell completely
                    rows.append(dict(base=base.code, action='sell_spot', qty=qty))

        if bases_posi_long:
            for base in bases_posi_long:
                qty = get_position_size(account, base) + get_spot_balance_free(account, base)
                if base in bases_alloc_no_margin:
                    qty_needed = calculate_target_quantity(account, exchange, base)
                    if qty_needed > qty:
                        rows.append(dict(base=base.code, action='buy', qty=qty_needed - qty))
                    elif qty_needed < qty:
                        rows.append(
                            dict(base=base.code, action='close_long', qty=qty - qty_needed))  # bases to decrease
                else:
                    # long positions to close completely
                    rows.append(dict(base=base.code, action='close_long', qty=qty))

        if bases_posi_short:
            for base in bases_posi_short:
                # short positions to close completely
                if base not in bases_alloc_margin:
                    rows.append(dict(base=base.code, action='close_short', qty=get_position_size(account, base)))

        if bases_alloc_no_margin:
            for base in bases_alloc_no_margin:
                # new bases to buy (spot or future/swap)
                if base not in (bases_spot + bases_posi_long):
                    rows.append(dict(base=base.code,
                                     action='buy',
                                     qty=calculate_target_quantity(account, exchange, base)))

        if bases_alloc_margin:
            for base in bases_alloc_margin:
                qty_needed = calculate_target_quantity(account, exchange, base)
                qty_position = get_position_size(account, base)
                if bases_posi_short:
                    if base not in bases_posi_short:
                        # short positions to open
                        rows.append(dict(base=base.code, action='open_short', qty=qty_needed))
                    else:
                        # short positions to increase/decrease
                        if qty_needed > qty_position:
                            # bases to increase
                            rows.append(dict(base=base.code, action='open_short', qty=qty_needed - qty_position))
                        elif qty_needed < qty_position:
                            # bases to decrease
                            rows.append(dict(base=base.code, action='close_short', qty=qty_position - qty_needed))
                else:
                    # short positions to open
                    rows.append(dict(base=base.code, action='open_short', qty=qty_needed - qty_position))

        # markets = Market.objects.filter(type=tp, base__code__in=bases, exchange=exchange)
        df = pd.DataFrame(rows)

        # remove duplicate rows (ex: BTC in spot and long position)
        df.drop_duplicates(inplace=True)

        # select bases and all theirs quotes
        bases = df['base'].to_list()
        quotes = [Market.objects.filter(exchange=account.exchange,
                                        base__code=base).values_list('quote__code', flat=True) for base in bases]
        types = [list(Market.objects.filter(exchange=account.exchange,
                                            base__code=base).values_list('type', flat=True)) for base in bases]
        types = sum(types, [])  # unnest list

        # create new columns for quotes and types and insert lists
        df = df.assign(quote=quotes)

        # explode rows to assign 1 quote and 1 type per row
        df = df.explode('quote')
        df = df.assign(type=types)

        # remove duplicate
        df.drop_duplicates(inplace=True)

        # remove future and swap when action is 'sell_spot'
        query = df.query("type == 'future' & action == 'sell_spot'")
        df.drop(query.index, inplace=True)

        # create multi indexes
        df = df.set_index(['base', 'quote', 'type'])

        df.columns = df.columns.str.replace('qty', 'qty_base_delta')
        df['qty_quote_equiv'] = ''
        df['price_avg'] = ''
        df['price_best'] = ''
        df['spread'] = ''
        df['qty_quote_limit'] = ''
        df['direct_trade'] = ''

        log.info('Create instruction dataframe for {0}'.format(account.name))
        print(df)

        return df

    df = get_instructions_table()

    async def trade():
        log.info('Trade for {0}'.format(account.name))
        nonlocal df

        # sort indexes to avoid PerformanceWarning
        df = df.sort_index()

        while True:
            try:
                if len(df.loc[df['direct_trade'] == True]) > 0:
                    spreads = df.loc[df['direct_trade'] == True]['spread'].to_list()
                    if '' not in spreads:

                        # select direct trades and sort by spread
                        direct = df.loc[df['direct_trade'] == True]
                        direct = direct.sort_values(by=['spread'])

                        orders = dict()

                        for index, columns in direct.iterrows():

                            base = index[0]
                            quote = index[1]
                            tp = index[2]

                            # select the desired quantity of quote
                            buys = df.query('base == @quote & action == "buy"')
                            qty_base_delta_buy = buys.iloc[0]['qty_base_delta']

                            # select how much quote we could get from the sell
                            qty_quote_equiv = columns['qty_quote_equiv']
                            qty_quote_limit = columns['qty_quote_limit']

                            # select quantity
                            if qty_base_delta_buy > max(qty_quote_equiv, qty_quote_limit):
                                qty = max(qty_quote_equiv, qty_quote_limit)
                            else:
                                qty = qty_base_delta_buy

                            # prepare order
                            symbol = index[0] + '/' + index[1]
                            market = Market.objects.get(symbol=symbol, type=tp, exchange=account.exchange)
                            clientOrderId = str(account.id) + str(datetime.now().strftime("%Y%m%d%H%M%S%f"))
                            amount = float(format_decimal(qty, market.precision['amount'], account))

                            if amount == 0:
                                continue

                            # update base quantity in 'buy'
                            df.loc[buys.index, 'qty_base_delta'] -= amount

                            # update base quantity in 'sell_spot'
                            if qty == qty_quote_equiv:
                                df.loc[index, 'qty_base_delta'] -= amount / columns['price_avg']
                            else:
                                df.loc[index, 'qty_base_delta'] -= amount / columns['price_best']

                            # reinitialize others columns
                            df.loc[index, ['qty_quote_equiv', 'price_avg', 'price_best', 'spread',
                                           'qty_quote_limit', 'direct_trade']] = ''

                            # create order object
                            log.info('Create object {0}'.format(symbol))

                            obj = dict(
                                account=account,
                                market=market,
                                type='market',
                                status='created',
                                side='sell',
                                price=None,
                                amount=None,
                                clientOrderId=clientOrderId,
                                params={'quoteOrderQty': str(amount),
                                        'newClientOrderId': clientOrderId}
                            )

                            Order.objects.create(**obj)

                            # place order
                            log.info('Place order {0}'.format(symbol))

                            orders[symbol] = dict(
                                symbol=symbol,
                                type='market',
                                side='sell',
                                price=None,
                                amount=0,
                                params={'quoteOrderQty': str(amount),
                                        'newClientOrderId': clientOrderId}
                            )

                            # place order
                            # order_place(account.name, tp, args)

                        if orders:

                            print(account.name)

                            gp = group([order_place.s(account.name, tp, args) for args in orders.values()])
                            res = gp.delay()

                            while not res.ready():
                                time.sleep(0.2)

                            if res.successful():
                                log.info(
                                    '{0} {1} placed'.format(len(orders), 'order' if len(orders) == 1 else 'orders'))

                await asyncio.sleep(5)

            except Exception as e:
                print(e)
                traceback.print_exc()

    async def watch_direct_trades():
        log.info('Watch direct trades for {0}'.format(account.name))
        nonlocal df

        while True:

            try:
                # select rows
                sell_spot = df.query('action == "sell_spot"')
                buys = df.query('action == "buy"')

                if sell_spot.empty or buys.empty:
                    return

                for index, columns in sell_spot.iterrows():
                    quote = list(index)[1]
                    bases_to_buy = list(buys.index.get_level_values('base'))
                    if quote in bases_to_buy:
                        df.loc[index, 'direct_trade'] = True

                await asyncio.sleep(5)

            except Exception as e:
                print(e)
                traceback.print_exc()

    async def watch_book(client, tp, market):

        symbol = market.symbol
        base = market.base.code
        quote = market.quote.code

        # access df variable in a nested function
        nonlocal df

        # sort indexes to avoid PerformanceWarning
        df = df.sort_index()

        while True:

            try:

                ob = await client.watch_order_book(symbol, limit=account.exchange.orderbook_limit)
                bids, asks = cumulative_book(ob)

                # select row for this combination of base+quote+tp
                # alternatively use df.loc[pd.IndexSlice[(base, quote, tp)]]
                query = df.query('base == @base & quote == @quote & type == @tp')

                # extract the type of action and the base quantity we need to buy or sell
                action = query['action'][0]
                qty_base_delta = query['qty_base_delta'][0]  # quantity delta

                # select depth side for this market
                if action in ['sell_spot', 'close_long', 'open_short']:
                    book = bids

                    # determine the quote quantity received (or required)
                    # if we buy (or sell) at 0.07% depth
                    qty_quote_limit = get_bid_quantity(book, 0.0007)

                else:
                    book = asks

                    # determine the quote quantity received (or required)
                    # if we buy (or sell) at 0.007% depth
                    qty_quote_limit = get_ask_quantity(book, 0.0007)

                # determine the average price we would buy (or sell) qty_base_delta
                price_avg, price_best = get_prices(book, qty_base_delta)

                # determine the quote quantity received (or required) if we buy (or sell) qty_base_delta
                qty_quote_equiv = price_avg * qty_base_delta

                if action in ['sell_spot', 'close_long', 'open_short']:
                    spread = 1 - price_avg / price_best
                else:
                    spread = price_avg / price_best - 1

                # update dataframe
                df.loc[query.index, 'qty_quote_equiv'] = qty_quote_equiv
                df.loc[query.index, 'qty_quote_limit'] = qty_quote_limit
                df.loc[query.index, 'price_avg'] = price_avg
                df.loc[query.index, 'price_best'] = price_best
                df.loc[query.index, 'spread'] = spread

                await client.sleep(5000)

            except Exception as e:
                # print('exception', str(e))
                traceback.print_exc()
                raise e  # uncomment to break all loops in case of an error in any one of them
                # break  # you can break just this one loop if it fails

    async def create_client(loop, tp):

        client = getattr(ccxtpro, exchange.exid)({'enableRateLimit': True,
                                                  'asyncio_loop': loop, })
        client.apiKey = account.api_key
        client.secret = account.api_secret

        # configure client for market
        if 'defaultType' in client.options:
            client.options['defaultType'] = tp

        # select first level of index (bases)
        indexes = df.index.get_level_values(0)
        bases = list(set(indexes))
        markets = Market.objects.filter(type=tp, base__code__in=bases, exchange=exchange)

        loops = [watch_book(client, tp, market) for market in markets]
        log.info('Watch markets for {0}'.format(account.name), type=tp)

        await asyncio.gather(*loops)
        await client.close()

    async def main(loop):

        # select markets to watch for this account
        if account.strategy.margin:
            types = list(set(Market.objects.filter(exchange=account.exchange).values_list('type', flat=True)))
        else:
            types = list(set(Market.objects.filter(exchange=account.exchange,
                                                   type__in=[None, 'spot']).values_list('type', flat=True)))

        # create clients
        loops = [create_client(loop, tp) for tp in types]

        await asyncio.gather(*loops)
        # await asyncio.wait([*loops, watch_direct_trades()])

    loop = asyncio.get_event_loop()
    # loop.run_until_complete(main(loop))

    gp = asyncio.wait([main(loop), watch_direct_trades(), trade()])

    # gp = asyncio.gather(main(loop)) #, watch_direct_trades())
    loop.run_until_complete(gp)

    # loop.create_task(main(loop))
    # loop.create_task(watch_direct_trades())
    # loop.run_forever()
    # loop.close()


# @shared_task(name='fetch_balance_n_positions')
def fetch_balance_n_positions():
    accounts = [account.name for account in Account.objects.filter(trading=True)]
    chains = [chain(fetch_order_open.si(account),
                    cancel_order_id.si(account),
                    create_fund.si(account),
                    update_positions.si(account)) for account in accounts]

    res = group(*chains).delay()
