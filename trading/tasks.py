from __future__ import absolute_import, unicode_literals
from datetime import datetime, date, timedelta
from django.db.models import Q
from django.utils import timezone
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
import capital.celery as celery
from celery import chain, group, shared_task, Task
from itertools import chain as itertools_chain
from itertools import accumulate
import ccxtpro
import asyncio
from trading.models import Account, Order, Fund, Position
from trading.methods import *
from trading.error import *
from marketsdata.models import Market, Candle, Currency, Exchange
from capital.error import *
from strategy.models import Allocation, Strategy
import pandas as pd
import sys, traceback
from trading.methods import format_decimal
import time

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


# Place an order to the market after an object is created
#@shared_task(name='account_order_place', base=BaseTaskWithRetry)
def order_place(account, tp, args):
    try:
        symbol = args['symbol']
        order_type = args['type']
        clientOrderId = args['params']['newClientOrderId']

        account = Account.objects.get(name=account)
        client = account.exchange.get_client(account)
        market = Market.objects.get(exchange=account.exchange, type=tp, symbol=symbol)
        order = Order.objects.get(clientOrderId=clientOrderId)

        if 'defaultType' in client.options:
            client.options['defaultType'] = market.type

        # Specific to OKEx
        # if order.account.exchange.ccxt == 'okex':
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
                raise MethodUnsupported('Limit order not supported with'.format(account.exchange.ccxt))
        else:
            if account.exchange.has['createMarketOrder']:
                response = client.create_order(**args)
            else:
                raise MethodUnsupported('Market order not supported with'.format(account.exchange.ccxt))

    except ccxt.ExchangeError as e:
        log.exception('Error when placing an order: {0}'.format(e), args=args)
    except Exception as e:
        log.exception('Exception when placing an order: {0}'.format(e), args=args)
    else:

        # if account.exchange.ccxt == 'okex':
        #     if response['info']['error_code'] == '0':
        #         order.refresh()
        #     else:
        #         log.error('Error code is not 0', account=account.name, args=args)
        #         pprint(response)

        print('response')
        pprint(response)

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
            pprint(response)
            log.error('Order ID unknown', account=order.account.name, args=args)

        order.response = response
        order.save()
        log.info('Order ID {0} object saved'.format(order.orderId), account=order.account.name)


# Fetch all orders for a list of symbols of a market type
# create or update objects
# weight = 1 per symbol
#@shared_task(name='account_orders_fetch_symbols', base=BaseTaskWithRetry)
def orders_fetch_symbols(account, symbols, tp):
    account = Account.objects.get(name=account)

    if account.exchange.has['fetchOrders']:

        log.bind(account=account.name, exchange=account.exchange.ccxt)
        client = account.exchange.get_client(account)

        # set market type
        if 'defaultType' in client.options:
            client.options['defaultType'] = tp
            client.load_markets(reload=True)

        for symbol in symbols:

            market = Market.objects.get(symbol=symbol, type=tp, exchange=account.exchange)
            log.info('Fetch orders for {1} {0}'.format(market.symbol, tp))
            since = max(client.parse8601(account.created_at), account.get_latest_order_dt)
            response = client.fetch_orders(symbol, since)

            if account.exchange.ccxt == 'binance':
                account.exchange.credit[int(timezone.now().timestamp())] = 1
                account.exchange.save()

            for order in response:
                account.order_create_update(order, tp)


# Fetch open orders for all symbols of a market type
# create or update objects
# weight = 40
#@shared_task(name='account_orders_fetch_all_open', base=BaseTaskWithRetry)
def orders_fetch_all_open(account):
    account = Account.objects.get(name=account)
    types = account.exchange.get_market_types()

    if account.exchange.has['fetchOpenOrders']:

        log.bind(account=account.name, exchange=account.exchange.ccxt)
        client = account.exchange.get_client(account=account)

        if account.exchange.ccxt == 'binance':
            client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

        def fetch_orders(tp=None):

            log.info('Fetch open orders for {0}'.format(account.name), tp=tp)

            # reload markets and fetch orders
            client.load_markets(reload=True)
            response = client.fetchOpenOrders()

            if account.exchange.ccxt == 'binance':
                account.exchange.credit[int(timezone.now().timestamp())] = 40
                account.exchange.save()

            for order in response:
                account.order_create_update(order, tp)

        # set market type
        if 'defaultType' in client.options:
            for tp in types:
                client.options['defaultType'] = tp
                fetch_orders(tp)
        else:
            fetch_orders()

    else:
        raise MethodUnsupported('Method fetchOpenOrders not supported for exchange'.format(account.exchange.ccxt))


# Fetch an orderId (reload = True)
# update object
# weight = 1
#@shared_task(name='account_order_fetch_id', base=BaseTaskWithRetry)
def order_fetch_id(account, orderid):
    account = Account.objects.get(name=account)
    order = Order.objects.get(orderId=orderid)

    # check if market is active
    if not order.market.active:
        raise TradingError('Cannot fetch order {0} market {1} on {2} is inactive'
                           .format(orderid, order.market.symbol, order.market.exchange.ccxt))

    # check order status
    if order.status != 'open':
        raise TradingError('Cannot fetch order {0} with status {1}'.format(orderid, order.status))

    client = account.exchange.get_client(account)

    # set client options
    if 'defaultType' in client.options:
        client.options['defaultType'] = order.market.type

    # check if method is supported
    if not account.exchange.has['fetchOrder']:
        raise TradingError('Cannot fetch order {0} method fetchOrder() is not supported by {1}'
                           .format(orderid, order.market.exchange.ccxt))

    params = None
    if account.exchange.ccxt == 'okex':
        params = dict(instrument_id=order.market.info['instrument_id'],
                      order_id=orderid)

    log.info('Fetch order {0}'.format(orderid))

    client.load_markets(True)
    response = client.fetchOrder(id=orderid, symbol=order.market.symbol, params=params)

    if account.exchange.ccxt == 'binance':
        account.exchange.credit[int(timezone.now().timestamp())] = 1
        account.exchange.save()

    # update order object
    account.order_create_update(response)

    log.info('Fetch order {0} complete'.format(orderid))


# Fetch a list of open orders by orderId for a market type
# update objects
# weight = 1 per order
#@shared_task(name='account_orders_fetch_id', base=BaseTaskWithRetry)
def orders_fetch_id(account, tp):

    # search for open orders
    orders = Order.objects.filter(Q(status='open') | Q(status='open'), account=account, market__type=tp)

    if not orders.exists():
        log.info('Unable to find open order on {0} for {1}'.format(tp, account.name))
        return

    for order in orders:

        # check if market is active
        if not order.market.active:
            raise TradingError('Cannot fetch order {0} for {3} market {1} on {2} is inactive'
                               .format(order.orderId, order.market.symbol, order.market.exchange.ccxt, order.market.type))

        # check order status
        if order.status != 'open':
            raise TradingError('Cannot fetch order {0} with status {1}'.format(order.orderId, order.status))

        client = account.exchange.get_client(account)

        # set client options
        if 'defaultType' in client.options:
            client.options['defaultType'] = order.market.type

        # check if method is supported
        if not account.exchange.has['fetchOrder']:
            raise TradingError('Cannot fetch order {0} method fetchOrder() is not supported by {1}'
                               .format(order.orderId, order.market.exchange.ccxt))

        params = None
        if account.exchange.ccxt == 'okex':
            params = dict(instrument_id=order.market.info['instrument_id'],
                          order_id=order.orderId)

        log.info('Fetch order {0}'.format(order.orderId))
        response = client.fetchOrder(id=order.orderId, symbol=order.market.symbol, params=params)

        # update order object
        account.order_create_update(response)


# Cancel an open order by orderId
# weight = 1
#@shared_task(name='account_orders_cancel_all', base=BaseTaskWithRetry)
def order_cancel_pending(account, orderid):
    account = Account.objects.get(name=account)
    order = Order.objects.get(orderId=orderid)

    # check if market is active
    if not order.market.active:
        raise TradingError('Cannot fetch order {0} for {3} market {1} on {2} is inactive'
                           .format(orderid, order.market.symbol, order.market.exchange.ccxt, order.market.type))

    # check order status
    if order.status != 'open':
        raise TradingError('Cannot fetch order {0} with status {1}'.format(orderid, order.status))

    log.info('Cancel order', id=order.orderId)
    client = account.exchange.get_client(account)

    # set market type
    if 'defaultType' in client.options:
        client.options['defaultType'] = order.market.type

    # reload markets and cancel order
    client.load_markets(True)
    client.cancel_order(id=order.orderId, symbol=order.market.symbol)

    if account.exchange.ccxt == 'binance':
        account.exchange.credit[int(timezone.now().timestamp())] = 1
        account.exchange.save()

    log.info('Cancel order complete', id=order.orderId)


# Cancel all open orders by market type
# weight = 1 per order
#@shared_task(name='account_orders_cancel_all', base=BaseTaskWithRetry)
def orders_cancel_pending(account, tp):
    account = Account.objects.get(name=account)

    # search for open orders
    orders = Order.objects.filter(Q(status='open') | Q(status='open'), account=account, market__type=tp)

    if orders.exists():
        for order in orders:

            # check if market is active
            if not order.market.active:
                raise TradingError('Cannot fetch order {0} for {3} market {1} on {2} is inactive'
                                   .format(order.orderId, order.market.symbol, order.market.exchange.ccxt, order.market.type))

            # check order status
            if order.status != 'open':
                raise TradingError('Cannot fetch order {0} with status {1}'.format(order.orderId, order.status))

            log.info('Cancel order', id=order.orderId)
            client = account.exchange.get_client(account)

            # set market type
            if 'defaultType' in client.options:
                client.options['defaultType'] = order.market.type

            # reload markets and cancel order
            client.load_markets(True)
            client.cancel_order(id=order.orderId, symbol=order.market.symbol)

            if account.exchange.ccxt == 'binance':
                account.exchange.credit[int(timezone.now().timestamp())] = 1
                account.exchange.save()

            log.info('Cancel order complete', id=order.orderId)


# Fetch balance and create fund object
#@shared_task(name='account_fund_create', base=BaseTaskWithRetry)
def create_fund(account):
    log.bind(account=account)
    account = Account.objects.get(name=account)

    log.info('Fetch balance')
    client = account.exchange.get_ccxt_client(account)

    # add 1h because the balance is fetched at :59
    dt = timezone.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    # insert one object per currency
    def insert(response):
        total = [[code, value] for code, value in response['total'].items() if value > 0]
        for fund in total:
            code = fund[0]
            qty = fund[1]

            kwargs = dict(
                account=account,
                exchange=account.exchange,
                total=qty,
                used=response['used'][code],
                free=response['free'][code],
                type=tp,
                response=response['info'],
                currency=Currency.objects.get(code=code),
                dt=dt
            )
            try:
                Fund.objects.get(account=account,
                                 exchange=account.exchange,
                                 currency=Currency.objects.get(code=code),
                                 type=tp,
                                 dt=dt)

            except ObjectDoesNotExist:
                Fund.objects.create(**kwargs)

    # replace dictionary keys for OKEX swap response
    def okex_keys(response):
        for dic in ['total', 'used', 'free']:

            # sum swap accounts USDT marginated
            usdt = sum([response[dic][key] for key, value in response[dic].items() if '-USDT' in key])

            # select keys with value > 0
            usdt_k = [key for key, value in response[dic].items() if '-USDT' in key and value > 0]
            usd_k = [key for key, value in response[dic].items() if '-USD-' in key and value > 0]

            # clean dictionary
            for key in list(response[dic]):
                if '-USDT' in key:
                    del response[dic][key]
                elif '-USD-' in key:
                    response[dic][key.replace('-USD-SWAP', '')] = response[dic].pop(key)

            # create new key for USDT
            response[dic]['USDT'] = usdt

        # select exchange info for swap accounts
        usdt_info = [d for d in response['info']['info'] if d['instrument_id'] in usdt_k]
        usd_info = [d for d in response['info']['info'] if d['instrument_id'] in usd_k]

        # rewrite info
        response['info'] = usdt_info + usd_info

        return response

    if 'defaultType' in client.options:
        for tp in client.get_market_types():
            print(tp)
            client.options['defaultType'] = tp
            client.load_markets(True)
            response = client.fetchBalance()
            if account.exchange.ccxt == 'okex' and tp == 'swap':
                response = okex_keys(response)
            insert(response)
    else:
        response = client.fetchBalance()
        insert(response)


# Create or update future and swap open positions
#@shared_task(name='account_position_refresh', base=BaseTaskWithRetry)
def fetch_positions(account):
    log.bind(account=account)
    account = Account.objects.get(name=account)

    log.info('Fetch position')
    client = account.exchange.get_client(account)

    if account.exchange.ccxt == 'okex':
        if not account.strategy.margin:
            return

        # select all bases from strategy and select markets
        bases = list(
            chain([s.get_markets().values_list('base', flat=True) for s in account.strategy.sub_strategies.all()]))
        markets = Market.objects.filter(exchange=account.exchange, base__in=bases)

        if account.contract_preference == 0:  # swap

            # loop through markets we can possibly trade
            for market in markets.filter(type='swap'):

                params = dict(instrument_id=market.info['instrument_id'])
                response = client.swap_get_instrument_id_position(params)
                for p in response['holding']:
                    if p['last']:
                        defaults = dict(
                            last=float(p['last']),
                            liquidation_price=float(p['liquidation_price']),
                            instrument_id=p['instrument_id'],
                            response=p,
                            leverage=p['leverage'],
                            size_available=p['avail_position'],
                            size=float(p['position']),
                            side='buy' if p['side'] == 'long' else 'sell',
                            margin_mode=response['margin_mode'],
                            entry_price=float(p['avg_cost']),
                            margin_maint_ratio=float(p['maint_margin_ratio']),
                            realized_pnl=float(p['realized_pnl']),
                            unrealized_pnl=float(p['unrealized_pnl']),
                            created_at=timezone.make_aware(
                                datetime.strptime(p['timestamp'], datetime_directives_std))
                        )
                        account.create_update_delete_position(market, defaults)

        elif account.contract_preference == 1:  # future w. delivery

            # loop through markets we can possibly trade
            for market in markets.filter(type='futures'):
                params = dict(instrument_id=market.info['instrument_id'])
                response = client.futures_get_instrument_id_position(params)

                for p in response['holding']:
                    dic = dict(
                        last=float(p['last']),
                        liquidation_price=float(p['liquidation_price']),
                        instrument_id=p['instrument_id'],
                        response=p,
                        leverage=p['leverage'],
                        margin_mode=p['margin_mode'],
                        realized_pnl=float(p['realised_pnl']),
                        size_available=float(p['long_avail_qty']),
                        size=float(p['long_qty']),
                        entry_price=float(p['long_avg_cost']),
                        margin=float(p['long_margin']),
                        unrealized_pnl=float(p['long_unrealised_pnl']),
                        side='buy',
                        created_at=timezone.make_aware(datetime.strptime(p['created_at'], datetime_directives_std))
                    )
                    account.create_update_delete_position(market, dic)

                    # insert short position
                    dic['size_available'] = float(p['short_avail_qty'])
                    dic['size'] = float(p['short_qty'])
                    dic['entry_price'] = float(p['short_avg_cost'])
                    dic['margin'] = float(p['short_margin'])
                    dic['unrealized_pnl'] = float(p['short_unrealised_pnl'])
                    dic['side'] = 'sell'

                    account.create_update_delete_position(market, dic)

    if account.exchange.ccxt == 'binance':
        if not account.strategy.margin:
            return

        # fetch all positions in USDT margined swap
        if account.contract_preference == 0:  # swap
            response = client.fapiPrivateGetPositionRisk()
            for r in response:

                if 'USDT' in r['symbol']:
                    symbol = r['symbol'].replace('USDT', '/USDT')

                # select market
                try:
                    market = Market.objects.get(exchange=account.exchange, type='future', symbol=symbol)
                except ObjectDoesNotExist:
                    log.error('Future market {0} is not created'.format(symbol))
                    continue
                else:
                    size = float(r['positionAmt'])
                    side = 'buy' if size > 0 else 'sell'
                    size = abs(size)

                    if size != 0:
                        defaults = dict(
                            size=size,
                            side=side,
                            last=float(r['markPrice']),
                            leverage=float(r['leverage']),
                            entry_price=float(r['entryPrice']),
                            unrealized_pnl=float(r['unRealizedProfit']),
                            liquidation_price=float(r['liquidationPrice']),
                            margin_mode=r['marginType'],
                            response=r
                        )
                        account.create_update_delete_position(market, defaults)

        # fetch all positions in coin margined futures (perp, delivery)
        elif account.contract_preference == 1:
            log.error('Binance delivery future markets not supported created')
            return

            # response = client.dapiPrivateGetPositionRisk()
            #
            # for r in response:
            #
            #     if 'USDT' in r['symbol']:
            #         symbol = r['symbol'].replace('USDT', '/USDT')
            #
            #     print(symbol)
            #     # select market
            #     try:
            #         market = Market.objects.get(exchange=account.exchange, type='future', symbol=symbol)
            #     except ObjectDoesNotExist:
            #         log.error('Future market {0} is not created'.format(symbol))
            #         continue
            #     else:
            #         size = float(r['positionAmt'])
            #         side = 'buy' if size > 0 else 'sell'
            #         size = abs(size)
            #
            #         if size != 0:
            #
            #             defaults = dict(
            #                 size=size,
            #                 side=side,
            #                 last=float(r['markPrice']),
            #                 leverage=float(r['leverage']),
            #                 entry_price=float(r['entryPrice']),
            #                 unrealized_pnl=float(r['unRealizedProfit']),
            #                 liquidation_price=float(r['liquidationPrice']),
            #                 margin_mode=r['marginType'],
            #                 max_qty=r['maxQty'],  # defines the maximum quantity allowed
            #                 response=r
            #             )
            #             account.create_update_delete_position(market, defaults)


# Trade with account
#@shared_task(name='account_trade', base=BaseTaskWithRetry)
def trade(account):
    # Check account
    if not account.is_valid_credentials():
        raise InvalidCredentials(
            'Account {0} has invalid credentials {1}'.format(account.name, account.api_key))
    if not account.trading:
        raise InactiveAccount(
            'Account trading is deactivated for {0}'.format(account.name))

    # Check exchange
    if not account.exchange.is_active():
        raise TradingError(
            'Exchange {1} of account {0} is inactive'.format(account.name, account.exchange.ccxt))

    # Fire an exception if account is not compatible with the strategy
    account.is_compatible()

    # Check strategy update
    if not account.strategy.is_updated():
        raise StrategyNotUpdated(
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
#@shared_task(bind=True, name='trade_ws_account')
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
            raise SettingError('Increase orderbook limit over {0}'.format(account.exchange.orderbook_limit))

        # filter
        bid_filtered = [bid for bid in book if bid[0] > price_limit]

        # convert base qty in quote
        return bid_filtered[-1][1] * best

    # return ask quantity available at a limited price
    def get_ask_quantity(book, limit):
        best = book[0][0]
        price_limit = best + best * limit

        if price_limit > book[-1][0]:
            raise SettingError('Increase orderbook limit over {0}'.format(account.exchange.orderbook_limit))

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
                            pprint(orders)

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

        client = getattr(ccxtpro, exchange.ccxt)({'enableRateLimit': True,
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


# Fetch order book every x seconds
#@shared_task(bind=True, name='trade_ws_accounts')
def trade_ws_accounts(self):
    accounts = [account.name for account in Account.objects.filter(strategy__production=True, trading=True)]
    gp = group([trade_ws_account.s(account) for account in accounts])

    res = gp.delay()


#@shared_task(name='fetch_balance_n_positions')
def fetch_balance_n_positions():
    accounts = [account.name for account in Account.objects.filter(trading=True)]
    chains = [chain(orders_fetch_all_open.si(account),
                    orders_cancel_pending.si(account),
                    create_fund.si(account),
                    fetch_positions.si(account)) for account in accounts]

    res = group(*chains).delay()
