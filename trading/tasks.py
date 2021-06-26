from __future__ import absolute_import, unicode_literals

import asyncio
import time
import traceback
from itertools import accumulate
from pprint import pprint

import warnings
import ccxt
import ccxtpro
import numpy as np
import pandas as pd
import logging
import structlog
from celery import chain, group, shared_task, Task
from django.core.exceptions import ObjectDoesNotExist
from timeit import default_timer as timer

from capital.error import *
from capital.methods import *
from marketsdata.models import Market, Currency, Exchange
from strategy.models import Strategy
from trading.methods import *
from trading.models import Account, Order, Fund, Position, Transfer

log = structlog.get_logger(__name__)
warnings.simplefilter(action='ignore', category=FutureWarning)


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
                    responses = client.fetchOpenOrders(instrument_id)
                    account.exchange.update_credit('fetchOpenOrders', default_type)

                    if responses:

                        for response in responses:
                            order_create_update(account, response)

        # fetch others exchanges orders
        ###############################
        else:

            # Check credit and fetch open orders
            if account.exchange.has_credit():
                responses = client.fetchOpenOrders()
                account.exchange.update_credit('fetchAllOpenOrders', default_type)

            for response in responses:
                order_create_update(account, response)

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
            responses = client.fetch_orders(market.symbol, since=timestamp)
            account.exchange.update_credit('fetch_orders', market.default_type)

        # Insert orders
        for response in responses:
            order_create_update(account, response)

    else:
        raise Exception('Methode fetchOrders is not supported by {0}'.format(account.exchange.name))


# Cancel order by orderid
@shared_task(name='Trading_____Cancel order by ID', base=BaseTaskWithRetry)
def cancel_order_id(account, orderid):
    account = Account.objects.get(name=account)
    log.bind(account=account.name, exchange=account.exchange.exid, orderid=orderid)

    try:
        # First select object
        order = Order.objects.get(orderid=orderid)

    except ObjectDoesNotExist:
        log.error('Unable to select order object')

    else:
        client = account.exchange.get_ccxt_client(account)

        if order.market.default_type:
            client.options['defaultType'] = order.market.default_type

        log.info('Cancel order {0}'.format(orderid))

        # Check credit and cancel order
        if account.exchange.has_credit():
            try:
                client.cancel_order(id=order.orderid, symbol=order.market.symbol)
            except ccxt.OrderNotFound:
                log.warning('Unable to cancel order with ID {0}. Order not found.'.format(order.orderid))
            finally:
                account.exchange.update_credit('cancel_order', order.market.default_type)


# Place an order to the market after an object is created
@shared_task(name='Trading_____Place order by ID', base=BaseTaskWithRetry)
def place_order(account_id, pk):
    account = Account.objects.get(id=account_id)

    try:
        # Select object by it's primary key
        order = Order.objects.get(id=pk)

    except ObjectDoesNotExist:
        raise Exception('Placing order {0} failed, cannot select object'.format(pk))

    else:

        # Get client
        client = account.exchange.get_ccxt_client(account)

        # Set default_type is necessary
        if order.market.default_type:
            client.options['defaultType'] = order.market.default_type

        # Set primary key as clientOrderId
        if order.params is None:
            order.params = dict(clientOrderId=pk)
        else:
            order.params['clientOrderId'] = pk

        args = dict(
            symbol=order.market.symbol,
            type=order.type,
            side=order.side,
            amount=float(order.amount) if order.amount else None,
            params=order.params
        )

        pprint(args)

        # Set limit price
        if account.limit_order:
            args['price'] = order.price

        # Check API credit
        if account.exchange.has_credit():

            # Place order
            try:
                response = client.create_order(**args)

            except ccxt.InvalidOrder as e:
                log.error('Invalid order', e=e, pk=pk)

            except ccxt.InsufficientFunds:
                log.error('Insufficient funds to place order', pk=pk)

            except Exception as e:
                log.error('Unknown error when placing order', exction=e)

            else:

                # Update credit
                account.exchange.update_credit('create_order', order.market.default_type)

                if response['id']:

                    # Check if it's our order
                    if float(response['clientOrderId']) == pk:
                        return response

                    else:
                        pprint(response)
                        raise Exception('Placing order {0} failed, unknown clientOrderId')
                else:
                    pprint(response)
                    raise Exception('Placing order {0} failed, missing id')
        else:
            raise Exception('Placing order {0} failed, no credit left')

        # Specific to OKEx
        # clientOrderId = args['params']['newClientOrderId']
        # if order.account.exchange.exid == 'okex':
        #     # Set params
        #     if order.account.limit_order:
        #         args['params'] = {'order_type': '0'}
        #     else:
        #         args['params'] = {'order_type': '4'}
        #     # Rewrite type
        #     args['type'] = 1 if order.type == 'open_long' else 2 if order.type == 'open_short' \
        #         else 3 if order.type == 'close_long' else 4 if order.type == 'close_short' else None
        # if account.exchange.exid == 'okex':
        #     if response['info']['error_code'] == '0':
        #         order.refresh()
        #     else:
        #         log.error('Error code is not 0', account=account.name, args=args)
        #         pprint(response)


@shared_task(base=BaseTaskWithRetry)
def create_fund(account_id):
    """
    Used to create (update) a fund object. Fund object contains JSON data of wallets in a trading account. Wallet
    like spot, derivative wallets like Binance future and Binance delivery wallets.
    Fund object is created hourly and is updated after an order is passed.
    """

    start = timer()
    log.info('Fund object')

    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    dt = timezone.now().replace(minute=0, second=0, microsecond=0)

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
    def create_fund_object(total, free, used, margin_assets, positions):

        kwargs = dict(
            account=account,
            exchange=account.exchange,
            balance=sum(value['value'] for key in total.keys() for value in total[key].values()),
            margin_assets=margin_assets,
            positions=positions,
            total=total,
            used=used,
            free=free,
            dt=dt
        )

        args = dict(
            account=account,
            exchange=account.exchange,
            dt=dt
        )

        obj, created = Fund.objects.update_or_create(**args, defaults=kwargs)

        if created:
            log.info('Fund object created')

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

    # Create a dictionary of margin assets
    def get_margin_assets(response, default_type):

        if account.exchange.exid == 'binance':
            if default_type in ['future', 'delivery']:
                return [i for i in response['info']['assets'] if float(i['walletBalance']) > 0]

        elif account.exchange.exid == 'bybit':
            return [v for k, v in response['info']['result'].items() if v['wallet_balance'] > 0]

        else:
            return

    def get_positions_leverage(response, default_type):

        if account.exchange.exid == 'binance':
            if default_type in ['future', 'delivery']:
                return [dict(leverage=i['leverage'], instrument=i['symbol'])
                        for i in response['info']['positions']]

    # Create a dictionary of position
    def update_positions(response, default_type):

        if account.exchange.exid == 'binance':
            if default_type in ['future', 'delivery']:

                for position in response['info']['positions']:
                    initial_margin = float(position['initialMargin'])
                    if initial_margin > 0:

                        defaults = dict(
                            account=account,
                            initial_margin=initial_margin,
                            maint_margin=float(position['maintMargin']),
                            order_initial_margin=float(position['openOrderInitialMargin']),
                            response_2=position
                        )

                        market = Market.objects.get(exchange=account.exchange,
                                                    default_type=default_type,
                                                    response__id=position['symbol']
                                                    )

                        obj, created = Position.objects.update_or_create(exchange=account.exchange,
                                                                         market=market,
                                                                         defaults=defaults)
                        if created:
                            log.warning('New position object for {0}'.format(position['symbol']))
                        else:
                            log.info('Position object updated with margin')

        elif account.exchange.exid == 'bybit':
            raise Exception('Missing')

        else:
            raise Exception('Missing')

    # Create empty dictionaries
    total, used, free, margin_assets, positions = [dict() for _ in range(5)]

    if account.exchange.default_types:

        for default_type in account.exchange.get_default_types():

            client.options['defaultType'] = default_type

            if account.exchange.has_credit(default_type):
                response = client.fetchBalance()

                account.exchange.update_credit('fetchBalance', default_type)

                t, u, f = create_dict(response)

                total[default_type] = t
                used[default_type] = u
                free[default_type] = f

                margin_assets[default_type] = get_margin_assets(response, default_type)
                positions[default_type] = get_positions_leverage(response, default_type)

                update_positions(response, default_type)

        create_fund_object(total, free, used, margin_assets, positions)

    else:

        default_type = 'default'

        if account.exchange.has_credit():
            response = client.fetchBalance()
            account.exchange.update_credit('fetchBalance')

            t, u, f = create_dict(response)

            total[default_type] = t
            used[default_type] = u
            free[default_type] = f

            margin_assets[default_type] = get_margin_assets(response, default_type)
            positions[default_type] = get_positions_leverage(response, default_type)

            create_fund_object(total, free, used, margin_assets, positions)

    end = timer()
    log.info('Update funds in {0} sec'.format(round(end - start, 2)))


@shared_task(name='Trading_____Create_funds')
def create_funds():
    accounts = [account.name for account in Account.objects.filter(trading=True)]
    chains = [chain(create_fund.si(account.id)) for account in accounts]

    result = group(*chains).delay()

    while not result.ready():
        time.sleep(0.5)

    if result.successful():
        log.info('Funds successfully created')


# Fetch an order by it's ID and update object
@shared_task(base=BaseTaskWithRetry)
def update_order_id(account_id, orderid):
    account = Account.objects.get(id=account_id)
    log.info('Update order {0}'.format(orderid))

    try:
        # First select object
        order = Order.objects.get(account=account, orderid=orderid)

    except ObjectDoesNotExist:
        log.error('Unable to select order object')

    else:

        if order.status == 'open':

            client = account.exchange.get_ccxt_client(account)

            # Set default_type if necessary
            if order.market.default_type:
                client.options['defaultType'] = order.market.default_type

            # check if method is supported
            if account.exchange.has['fetchOrder']:

                params = None

                # OKEx specific
                if account.exchange.exid == 'okex':
                    params = dict(instrumentid=order.market.info['instrument_id'], orderid=orderid)

                # Check credit and insert order
                if account.exchange.has_credit():
                    response = client.fetchOrder(id=orderid, symbol=order.market.symbol)  # , params=params)
                    account.exchange.update_credit('fetchOrder', order.market.default_type)

                    # Update order and return it's primary key if new trade detected
                    orderid = order_create_update(account, response)
                    return orderid

            else:
                raise Exception('Methode fetchOrder is not supported by {0}'.format(account.exchange.name))
        else:
            log.warning('Order is not open but {0}'.format(order.status))

        return False


# Create, update or delete objects
@shared_task(name='Trading_____Update position', base=BaseTaskWithRetry)
def update_positions(account_id, orderids=None):
    start = timer()

    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    if orderids:
        # Create a list of derivative markets open orders belong to
        markets = list(set([order.market for order in Order.objects.filter(orderid__in=orderids) if
                            order.market.type == 'derivative']))

    # Create/update object of an open position
    def create_update(market, defaults):

        obj, created = Position.objects.update_or_create(exchange=account.exchange,
                                                         account=account,
                                                         market=market,
                                                         defaults=defaults
                                                         )

        if created:
            log.info('Create new positions {0}'.format(market.symbol))
        else:
            log.info('Update positions {0}'.format(market.symbol))

    # Delete object of a closed position
    def delete(position):

        try:
            obj = Position.objects.get(account=account,
                                       exchange=account.exchange,
                                       market__response__id=position['symbol']
                                       )
        except ObjectDoesNotExist:
            pass

        else:
            obj.delete()
            log.info('Update positions {0}'.format(position['symbol']), action='delete')

    # OKEx
    ######
    if account.exchange.exid == 'okex':

        # fetch Okex perpetual positions
        def okex_swap():

            if account.exchange.has_credit():

                response = client.swapGetPosition()
                account.exchange.update_credit('swapGetPosition', 'swap')

                # Construct dictionary
                if response[0]['holding']:
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

        # Update all wallets
        if orderids is None:
            okex_swap()
            okex_futures()

        elif markets:
            # or update positions of a specific API
            if 'swap' in [m.default_type for m in markets]:
                okex_swap()
            if 'futures' in [m.default_type for m in markets]:
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

                    log.error('Update position failed, unable to select object'.format(position['symbol']))
                    continue

                else:
                    size = float(position['positionAmt'])
                    side = 'buy' if size > 0 else 'sell'
                    asset = market.base

                    # calculate position value in USDT
                    value_usd = size * float(position['markPrice'])

                    defaults = dict(
                        side=side,
                        size=size,
                        asset=asset,
                        notional_value=float(position['notional']),
                        settlement=market.margined,
                        value_usd=value_usd,
                        last=float(position['markPrice']),
                        leverage=float(position['leverage']),
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

                    log.error('Update position failed, unable to select object'.format(position['symbol']))
                    continue

                else:

                    size = position['positionAmt']  # contract
                    side = 'buy' if float(size) > 0 else 'sell'
                    asset = None  # cont

                    # calculate position value in USDT
                    value_usd = float(size) * market.contract_value

                    defaults = dict(
                        side=side,
                        size=size,
                        asset=asset,
                        notional_value=float(position['notionalValue']),
                        settlement=market.margined,
                        value_usd=value_usd,
                        last=float(position['markPrice']),
                        leverage=float(position['leverage']),
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

        # Update all wallets
        if orderids is None:
            binance_usd_margined()
            binance_coin_margined()

        elif markets:
            # or update positions of a specific API
            if 'future' in [m.default_type for m in markets]:
                binance_usd_margined()
            if 'delivery' in [m.default_type for m in markets]:
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

        # Update all wallets
        if orderids is None:
            bybit_usdt_margined()
            bybit_coin_margined()

        # or update positions of a specific API
        elif markets:
            margined = list(set([m.margined.code for m in markets]))
            if margined:
                if 'USDT' in margined:
                    bybit_usdt_margined()
                if len(margined) > 1:
                    bybit_coin_margined()

    end = timer()
    log.info('Update positions in {0} sec'.format(round(end - start, 2)))


# Transfer fund between wallets
@shared_task(base=BaseTaskWithRetry)
def transfer(id, segment):
    start = timer()

    # Select transfer informations
    code = segment.transfer.asset
    quantity = segment.transfer.quantity
    from_wallet = segment.transfer.from_wallet
    to_wallet = segment.transfer.to_wallet

    account = Account.objects.get(id=id)
    client = account.exchange.get_ccxt_client(account)

    if account.exchange.has_credit():
        try:

            log.info('Transfer {0} {1} from {2} to {3}'.format(round(quantity, 5), code, from_wallet, to_wallet))
            response = client.transfer(code, quantity, from_wallet, to_wallet)

        except Exception as e:

            log.error('Unable to transfer fund')

            pprint(dict(
                code=code,
                quantity=quantity,
                from_wallet=from_wallet,
                to_wallet=to_wallet
            ))

            account.exchange.update_credit('transfer', 'spot')
            traceback.print_exc()
            raise e

        else:
            account.exchange.update_credit('transfer', 'spot')

            if response['id']:

                if response['status'] is None:
                    status = True
                else:
                    status = True

                if not response['timestamp']:
                    response['timestamp'] = client.milliseconds()
                if not response['datetime']:
                    response['datetime'] = client.iso8601(client.milliseconds())

                args = dict(
                    account=account,
                    exchange=account.exchange,
                    currency=Currency.objects.get(code=code),
                    amount=quantity,
                    response=response,
                    from_wallet=from_wallet,
                    to_wallet=to_wallet,
                    transferid=int(response['id']),
                    status=status,
                    datetime=response['datetime'],
                    timestamp=response['timestamp']
                )

                Transfer.objects.create(**args)

                end = timer()
                log.info('Transfer funds in {0} sec'.format(round(end - start, 2)))

                return True

            else:
                pprint(response)
                log.error('Unable to transfer fund')
                return


global accounts, codes


@shared_task()
def update_accounts(id):
    # Return cumulative orderbook
    def cumulative_book(ob):

        asks = ob['asks']
        bids = ob['bids']
        asks_p = [a[0] for a in asks]
        bids_p = [a[0] for a in bids]
        cum_a = list(accumulate([a[1] for a in asks]))
        cum_b = list(accumulate([a[1] for a in bids]))
        return [[bids_p[i], cum_b[i]] for i, a in enumerate(bids)], [[asks_p[i], cum_a[i]] for i, a in enumerate(asks)]

    # Create a dataframes with markets
    def create_markets(id):

        log.info('Create dataframe markets')

        # Select markets to build dataframe
        mks = Market.objects.filter(exchange=exchange,
                                    base__code__in=codes,
                                    quote__code__in=codes,
                                    excluded=False,
                                    active=True
                                    )

        markets = pd.DataFrame()

        # Loop through codes
        for code in codes:

            # Loop through markets
            for market in mks.filter(base__code=code):
                if market.is_updated():

                    margined = market.margined.code if market.margined else None

                    # Create multilevel columns
                    indexes = pd.MultiIndex.from_tuples([(code,
                                                          market.quote.code,
                                                          market.default_type,
                                                          market.symbol,
                                                          market.type,
                                                          market.derivative,
                                                          margined
                                                          )],
                                                        names=['base',
                                                               'quote',
                                                               'default_type',
                                                               'symbol',
                                                               'type',
                                                               'derivative',
                                                               'margined'
                                                               ])
                    cols = pd.MultiIndex.from_product([['depth'], ['spread']], names=['first', 'second'])

                    # Select funding rate for perp
                    if market.derivative == 'perpetual':
                        funding = float(market.funding_rate['lastFundingRate'])
                    else:
                        funding = np.nan

                    # Construct dataframe and normalize rows
                    df = pd.DataFrame(np.nan, index=indexes, columns=cols)

                    # Fill funding rate and latest price
                    df['funding', 'rate'] = funding
                    df['price', 'close'] = market.get_candle_price_last()

                    # Fill order status and route type with nan
                    df['route', 'type'] = np.nan
                    df['order', 'status'] = np.nan
                    df['order', 'id'] = np.nan

                    markets = pd.concat([df, markets], axis=0)  # .groupby(level=[0, 1, 2, 3, 4, 5, 6]).mean()

        # Sort indexes and columns
        markets.sort_index(axis=0, inplace=True)
        markets.sort_index(axis=1, inplace=True)

        log.info('Create dataframe markets OK')

        return markets

    # Create a dataframe with available routes
    def create_routes(id):

        start = timer()
        log.info('Prepare buy and sell candidates')

        account = Account.objects.get(id=id)

        # Currencies
        ############

        markets_index = markets[id].index.tolist()
        stablecoins = account.exchange.get_stablecoins()

        # Create a list of currencies with free balance > 0
        free_value = balances[id][('wallet', 'free_value')] > 0
        spot_wallet = balances[id].index.isin(['spot'], 'wallet')
        codes_free_spot = list(balances[id][free_value & spot_wallet].index.get_level_values('code').unique())
        codes_free_spot_stable = [c for c in codes_free_spot if c in stablecoins]

        # Create a list of currencies to buy and sell
        codes_sell = list(balances[id][(balances[id][('delta', 'value')] > 0)].index.get_level_values('code').unique())
        codes_sell_spot = [code for code in codes_free_spot if code in codes_sell]
        codes_buy = list(balances[id][(balances[id][('delta', 'value')] < 0)].index.get_level_values('code').unique())

        # Prevent buying stablecoin if the value hedged is larger than cash allocation
        # if synthetic_cash < 0:
        # codes_buy = [c for c in codes_buy if c not in stablecoins]

        # Give the opportunity to sell stablecoin if hedge
        if account.get_hedge_total(prices) > 0:
            codes_sell_spot = list(set(codes_sell_spot + codes_free_spot_stable))

        # Markets
        #########

        # Create a list of markets with an open position to close
        mk_close_long = [i for i, p in positions[id].iterrows() if p['side'] == 'buy' and i[0] in codes_sell]
        mk_close_short = [i for i, p in positions[id].iterrows() if p['side'] == 'sell' and i[0] in codes_buy]
        mk_close = mk_close_long + mk_close_short

        if 'hedge_code' in positions[id]:
            mk_close_hedge = positions[id].loc[positions[id]['hedge_code'] > 0, :].index

        # Create a list of markets available to open
        mk_candidates = [mk for mk in markets_index]
        mk_candidates_spot = [mk for mk in mk_candidates if mk[4] == 'spot']
        mk_candidates_open_long = [mk for mk in mk_candidates if mk[0] in codes_buy and mk[4] == 'derivative']
        mk_candidates_open_short = [mk for mk in mk_candidates if mk[0] in codes_sell and mk[4] == 'derivative']

        # Create a list of spot markets with free balance > 0
        mk_spot_base = [mk for mk in mk_candidates if mk[4] == 'spot' and mk[0] in codes]
        mk_spot_quote = [mk for mk in mk_candidates if mk[4] == 'spot' and mk[1] in codes]
        mk_spot = list(set(mk_spot_base + mk_spot_quote))

        # Wallets
        #########

        # Create a list of wallet with derivative markets
        wallets_deri = list(set([mk[2] for mk in mk_candidates if mk[4] == 'derivative']))
        wallets_spot = list(set([mk[2] for mk in mk_candidates if mk[4] == 'spot']))

        end = timer()
        log.info('Prepare buy and sell candidates in {0} sec'.format(round(end - start, 2)))

        # print('Currencies', codes)
        # print('Instructions buy', codes_buy)
        # print('Instructions sell', codes_sell)
        # print('Instructions sell spot', codes_sell_spot)
        # print('Wallet derivative', wallets_deri)
        # print('Wallet spot', wallets_spot)
        #
        # for i in mk_close_long:
        #     print('Market close long:', i[3], i[2])
        #
        # for i in mk_close_short:
        #     print('Market close short:', i[3], i[2])
        #
        # if 'mk_close_hedge' in locals():
        #     for i in mk_close_hedge:
        #         print('Market close hedge:', i[3], i[2])
        #
        # for i in mk_candidates:
        #     print('Candidates:', i[3], i[2])
        #
        # for i in mk_candidates_open_long:
        #     print('Candidates open long:', i[3], i[2])
        #
        # for i in mk_candidates_open_short:
        #     print('Candidates open short:', i[3], i[2])
        #
        # for i in mk_candidates_spot:
        #     print('Candidates spot:', i[3], i[2])
        #
        # for i in mk_spot:
        #     print('Market spot:', i[3], i[2])

        # Create an empty dataframe
        def create_dataframe(segment):
            tuples = [(segment, 'market', 'base'),
                      (segment, 'market', 'quote'),
                      (segment, 'market', 'symbol'),
                      (segment, 'market', 'wallet'),
                      (segment, 'market', 'type'),
                      (segment, 'market', 'derivative'),
                      (segment, 'market', 'margined'),

                      (segment, 'type', 'action'),
                      (segment, 'type', 'transfer'),
                      (segment, 'type', 'priority'),
                      ]

            columns = pd.MultiIndex.from_tuples(tuples, names=["level_1", "level_2", 'level_3'])
            return pd.DataFrame(columns=columns)

        # Find available routes
        def find_routes(args):

            # Find gateway market
            def get_gateway(instruction, candidate):

                # Determine the code needed to trade candidate market
                if instruction in ['open_long', 'open_short']:
                    code_needed = candidate[6]
                elif instruction == 'buy_base':
                    code_needed = candidate[1]
                elif instruction == 'sell_base':
                    code_needed = candidate[0]

                for gateway in mk_candidates_spot:

                    if code == gateway[1]:
                        if code_needed == gateway[0]:
                            instruction_gw = 'buy_base'
                    elif code == gateway[0]:
                        if code_needed == gateway[1]:
                            instruction_gw = 'sell_base'

                    if 'instruction_gw' in locals():
                        segment = 'segment'
                        gw = create_dataframe(segment)

                        gw.loc[0, (segment, 'market', 'base')] = gateway[0]
                        gw.loc[0, (segment, 'market', 'quote')] = gateway[1]
                        gw.loc[0, (segment, 'market', 'wallet')] = gateway[2]
                        gw.loc[0, (segment, 'market', 'symbol')] = gateway[3]
                        gw.loc[0, (segment, 'market', 'type')] = gateway[4]
                        gw.loc[0, (segment, 'market', 'derivative')] = gateway[5]
                        gw.loc[0, (segment, 'market', 'margined')] = gateway[6]

                        gw.loc[0, (segment, 'type', 'priority')] = 2
                        gw.loc[0, (segment, 'type', 'transfer')] = need_transfer(wallet, gateway[2])
                        gw.loc[0, (segment, 'type', 'action')] = instruction_gw

                        del instruction_gw
                        return gw

                log.warning('No gateway between {0} and {1}'.format(code, code_needed))

            # Return True if a gateway market is necessary to trade the candidate market
            def need_gateway(code, wallet, instruction, candidate):

                # Margin isn't compatible with code (can't open position)
                if instruction in ['open_long', 'open_short'] and candidate[6] != code:
                    return True

                # Quote isn't compatible with code (can't buy base)
                elif instruction == 'buy_base' and candidate[1] != code:
                    return True

                # Base isn't compatible with code (can't buy quote)
                elif instruction == 'sell_base' and candidate[0] != code:
                    return True

                else:
                    return False

            # Return True is a transfer of fund between wallets is needed
            def need_transfer(from_wallet, to_wallet):
                if from_wallet != to_wallet:
                    return True
                else:
                    return False

            args = sorted(args.items())
            code, market, source, wallet = [v[1] for v in args]

            # Close open position
            if market:
                code = market[6]
                wallet = market[2]
                segment = 's1'
                s1 = create_dataframe(segment)

                s1.loc[0, (segment, 'market', 'base')] = market[0]
                s1.loc[0, (segment, 'market', 'quote')] = market[1]
                s1.loc[0, (segment, 'market', 'wallet')] = wallet
                s1.loc[0, (segment, 'market', 'symbol')] = market[3]
                s1.loc[0, (segment, 'market', 'type')] = market[4]
                s1.loc[0, (segment, 'market', 'derivative')] = market[5]
                s1.loc[0, (segment, 'market', 'margined')] = code

                s1.loc[0, (segment, 'type', 'source')] = source
                s1.loc[0, (segment, 'type', 'priority')] = 2
                s1.loc[0, (segment, 'type', 'id')] = 1
                s1.loc[0, (segment, 'type', 'transfer')] = False
                s1.loc[0, (segment, 'type', 'action')] = 'close_long' if market in mk_close_long else 'close_short'

                # Margin currency is a desired currency
                # if code in codes_buy + stablecoins:
                lst.append(s1)

            for candidate in mk_candidates:

                if candidate[4] == 'spot':

                    if candidate[0] in codes_buy:
                        if candidate[0] != code:
                            if candidate[1] not in codes_sell:
                                instruction = 'buy_base'

                    if candidate[1] in codes_buy:
                        if candidate[1] != code:
                            if candidate[0] not in codes_sell:
                                instruction = 'sell_base'

                    # Give the opportunity to sell undesired currency as base
                    if code in codes_sell:
                        if candidate[0] == code:
                            if candidate[1] in stablecoins:
                                instruction = 'sell_base'

                    # Give the opportunity to sell undesired currency as quote
                    if code in codes_sell:
                        if candidate[1] == code:
                            if candidate[0] in codes_buy:
                                instruction = 'buy_base'

                elif candidate[4] == 'derivative':

                    if candidate in mk_candidates_open_long:
                        if candidate not in mk_close_short:  # Avoid open_long when a close_short is needed
                            instruction = 'open_long'
                    elif candidate in mk_candidates_open_short:
                        instruction = 'open_short'

                if 'instruction' in locals():

                    if source == 'close_position':

                        # Prevent duplicated instruction in source and destination
                        # For example close_long->open_short or close_short->open_long on the same base
                        if market[0] == candidate[0]:
                            del instruction
                            continue

                        # Test if a gateway market is necessary to reach candidate market
                        if need_gateway(code, wallet, instruction, candidate):

                            gateway = get_gateway(instruction, candidate)
                            if gateway is not None:

                                # Set level name and segment id
                                gateway.columns.set_levels(['s2'], level='level_1', inplace=True)
                                gateway.loc[0, ('s2', 'type', 'id')] = 2

                                segment = 's3'
                                s3 = create_dataframe(segment)

                                # Trade candidate market in third segment
                                s3.loc[0, (segment, 'market', 'base')] = candidate[0]
                                s3.loc[0, (segment, 'market', 'quote')] = candidate[1]
                                s3.loc[0, (segment, 'market', 'wallet')] = candidate[2]
                                s3.loc[0, (segment, 'market', 'symbol')] = candidate[3]
                                s3.loc[0, (segment, 'market', 'type')] = candidate[4]
                                s3.loc[0, (segment, 'market', 'derivative')] = candidate[5]
                                s3.loc[0, (segment, 'market', 'margined')] = candidate[6]

                                s3.loc[0, (segment, 'type', 'priority')] = 2
                                s3.loc[0, (segment, 'type', 'id')] = 3
                                s3.loc[0, (segment, 'type', 'action')] = instruction
                                s3.loc[0, (segment, 'type', 'transfer')] = need_transfer(
                                    gateway['s2']['market']['wallet'][0],
                                    candidate[2])

                                # Set gateway to second segment and create route
                                route = pd.concat([s1, gateway, s3], axis=1)
                                lst.append(route)

                            else:
                                del instruction
                                continue

                        else:

                            segment = 's2'
                            s2 = create_dataframe(segment)

                            # Trade candidate market in second segment
                            s2.loc[0, (segment, 'market', 'base')] = candidate[0]
                            s2.loc[0, (segment, 'market', 'quote')] = candidate[1]
                            s2.loc[0, (segment, 'market', 'wallet')] = candidate[2]
                            s2.loc[0, (segment, 'market', 'symbol')] = candidate[3]
                            s2.loc[0, (segment, 'market', 'type')] = candidate[4]
                            s2.loc[0, (segment, 'market', 'derivative')] = candidate[5]
                            s2.loc[0, (segment, 'market', 'margined')] = candidate[6]

                            s2.loc[0, (segment, 'type', 'priority')] = 2
                            s2.loc[0, (segment, 'type', 'id')] = 2
                            s2.loc[0, (segment, 'type', 'action')] = instruction
                            s2.loc[0, (segment, 'type', 'transfer')] = need_transfer(wallet, candidate[2])

                            # Create route
                            route = pd.concat([s1, s2], axis=1)
                            lst.append(route)

                    else:
                        # Prevent closing a position with free margin. Instructions close_long and close_short
                        # are limited to source market when market != None, not destination
                        if candidate in mk_close:
                            if candidate not in mk_candidates_open_long + mk_candidates_open_short:
                                del instruction
                                continue

                        # Test if a gateway market is necessary to reach candidate market
                        if need_gateway(code, wallet, instruction, candidate):
                            gateway = get_gateway(instruction, candidate)
                            if gateway is not None:

                                # Create segment 1 to trade funds in gateway market
                                gateway.loc[0, ('segment', 'funds', 'code')] = code
                                gateway.loc[0, ('segment', 'funds', 'wallet')] = wallet
                                gateway.loc[0, ('segment', 'type', 'source')] = source
                                gateway.loc[0, ('segment', 'type', 'id')] = 1

                                # Set label
                                gateway.columns.set_levels(['s1'], level='level_1', inplace=True)

                                segment = 's2'
                                s2 = create_dataframe(segment)

                                # Create segment 2 to trade candidate market
                                s2.loc[0, (segment, 'market', 'base')] = candidate[0]
                                s2.loc[0, (segment, 'market', 'quote')] = candidate[1]
                                s2.loc[0, (segment, 'market', 'wallet')] = candidate[2]
                                s2.loc[0, (segment, 'market', 'symbol')] = candidate[3]
                                s2.loc[0, (segment, 'market', 'type')] = candidate[4]
                                s2.loc[0, (segment, 'market', 'derivative')] = candidate[5]
                                s2.loc[0, (segment, 'market', 'margined')] = candidate[6]

                                s2.loc[0, (segment, 'type', 'priority')] = 2
                                s2.loc[0, (segment, 'type', 'id')] = 2
                                s2.loc[0, (segment, 'type', 'action')] = instruction
                                s2.loc[0, (segment, 'type', 'transfer')] = need_transfer(
                                    gateway['s1']['market']['wallet'][0],
                                    candidate[2])

                                # Set gateway to first segment and create route
                                route = pd.concat([gateway, s2], axis=1)
                                lst.append(route)

                            else:
                                del instruction
                                continue

                        else:

                            segment = 's1'
                            s1 = create_dataframe(segment)

                            # Trade candidate market in first segment
                            s1.loc[0, (segment, 'market', 'base')] = candidate[0]
                            s1.loc[0, (segment, 'market', 'quote')] = candidate[1]
                            s1.loc[0, (segment, 'market', 'wallet')] = candidate[2]
                            s1.loc[0, (segment, 'market', 'symbol')] = candidate[3]
                            s1.loc[0, (segment, 'market', 'type')] = candidate[4]
                            s1.loc[0, (segment, 'market', 'derivative')] = candidate[5]
                            s1.loc[0, (segment, 'market', 'margined')] = candidate[6]

                            s1.loc[0, (segment, 'funds', 'code')] = code
                            s1.loc[0, (segment, 'funds', 'wallet')] = wallet

                            s1.loc[0, (segment, 'type', 'source')] = source
                            s1.loc[0, (segment, 'type', 'id')] = 1
                            s1.loc[0, (segment, 'type', 'priority')] = 2
                            s1.loc[0, (segment, 'type', 'transfer')] = need_transfer(wallet, candidate[2])
                            s1.loc[0, (segment, 'type', 'action')] = instruction

                            route = s1
                            lst.append(route)

                    del instruction

        # Market structure
        # 0 'code',
        # 1 'quote',
        # 2 'default_type',
        # 3 'symbol',
        # 4 'type',
        # 5 'derivative',
        # 6 'margined'

        log.info('Build routes')
        start = timer()

        lst = []

        # Create routes to close positions
        ##################################

        for market in mk_close:
            args = dict(code=None,
                        market=market,
                        source='close_position',
                        wallet=None
                        )

            find_routes(args)

        # Create routes for currencies in spot
        ######################################

        for wallet in wallets_spot:
            for code in codes_sell_spot:
                args = dict(code=code,
                            market=None,
                            source='spot',
                            wallet=wallet
                            )
                find_routes(args)

        # Create routes for available margin
        #####################################

        for wallet in wallets_deri:
            margin = list(balances[id][(balances[id][('wallet', 'free_value')] > 0)
                                       & (balances[id].index.get_level_values('wallet') == wallet)
                                       ].index.get_level_values('code').unique())

            for code in margin:
                if code in codes_sell + stablecoins:
                    args = dict(code=code,
                                market=None,
                                source='margin',
                                wallet=wallet
                                )
                    find_routes(args)

        # Create routes to close hedge
        ##############################

        ratio = synthetic_cash[id]['ratio']
        capacity = synthetic_cash[id]['capacity']

        if ratio < 0:

            log.warning('Hedging capacity is too low {0}'.format(round(capacity, 2)))
            log.info('Create routes to close some short')

            for market in mk_close_hedge:
                segment = 's1'
                s1 = create_dataframe(segment)

                # Trade candidate market in third segment
                s1.loc[0, (segment, 'market', 'base')] = market[0]
                s1.loc[0, (segment, 'market', 'quote')] = market[1]
                s1.loc[0, (segment, 'market', 'wallet')] = market[2]
                s1.loc[0, (segment, 'market', 'symbol')] = market[3]
                s1.loc[0, (segment, 'market', 'type')] = market[4]
                s1.loc[0, (segment, 'market', 'derivative')] = market[5]
                s1.loc[0, (segment, 'market', 'margined')] = market[6]

                s1.loc[0, (segment, 'type', 'source')] = 'close_hedge'
                s1.loc[0, (segment, 'type', 'id')] = 1
                s1.loc[0, (segment, 'type', 'priority')] = 1
                s1.loc[0, (segment, 'type', 'action')] = 'close_short'
                s1.loc[0, (segment, 'type', 'transfer')] = False

                route = s1
                lst.append(route)

        # Concatenate dataframes
        df = pd.concat(lst)

        # Normalize number of segments
        if 's2' not in df:
            df = pd.concat([df, create_dataframe('s2')], axis=1)
        if 's3' not in df:
            df = pd.concat([df, create_dataframe('s3')], axis=1)

        # Drop duplicate routes and keep first
        # df = df.loc[~df.drop(['label', 'priority'], axis=1, level=2).duplicated(keep='last')]

        # Increment index
        df.index = (i for i in range(len(df)))

        # Determine number of segment per route
        segments = df.columns.get_level_values(0).unique()
        for index, row in df.iterrows():
            s = [row[s].type.action for s in segments]
            i = [i for i in s if not pd.isna(i)]
            se = ['s' + str(segment + 1) for segment in range(len(i))]
            df.loc[index, 'length'] = len(se)

        # Set length to integer
        df['length'] = df['length'].astype(int)

        # Add dataframe to dictionary
        routes[id] = df

        # log.info('Routes found')
        # print(routes[id].to_string())

        end = timer()
        log.info('Build routes in {0} sec'.format(round(end - start, 2)))

    # Update hedging capacity (USD margined)
    def update_synthetic_cash(id):

        account = Account.objects.get(id=id)

        # Get value of hedge for all currencies
        # Get value of margin allocated to hedge positions (USD margined)
        # Get value of target cash allocation in the portfolio

        hedge = account.get_hedge_total(prices)

        if 'hedge_margin' in positions[id]:
            hedge_margin = positions[id]['hedge_margin'].sum()
        else:
            hedge_margin = 0

        cash_target = balances[id].loc[account.get_codes_stable(), ('target', 'value')].mean()
        capacity = cash_target - (hedge + hedge_margin)

        log.info('Hedge          {0}'.format(round(hedge, 2)))
        log.info('Hedge margin   {0}'.format(round(hedge_margin, 2)))
        log.info('Cash target    {0}'.format(round(cash_target, 2)))
        log.info('Hedge capacity {0} sUSD'.format(round(capacity, 2)))

        # Create keys
        if id not in synthetic_cash:
            synthetic_cash[id] = {}
            synthetic_cash[id]['capacity'] = {}
            synthetic_cash[id]['ratio'] = {}

        synthetic_cash[id]['capacity'] = capacity
        synthetic_cash[id]['ratio'] = capacity / cash_target

    # Determine order size and transfer informations
    def size_orders(id):

        # Get leverage of a derivative market
        def get_leverage_position(symbol, wallet):

            market = Market.objects.get(exchange=exchange, symbol=symbol, default_type=wallet)
            instrument_id = market.response['id']
            positions = account.get_fund_latest().positions
            return float([p['leverage'] for p in positions[wallet] if p['instrument'] == instrument_id][0])

        # Return total absolute to buy/sell
        def get_delta(code):
            return abs(balances[id].loc[code, ('delta', 'value')].fillna(0)[0])

        # Return value of available currency
        def get_free(segment, code, wallet):

            if segment.type.source == 'margin':

                # Keep available margin in the wallet to maintain desired leverage
                total = balances[id].loc[(code, wallet), ('wallet', 'total_value')] * float(account.leverage)
                used = positions[id].loc[(positions[id].index.get_level_values('margined') == code) &
                                         (positions[id].index.get_level_values('wallet') == wallet)].sum().dollar_value

                free = max(0, (total - abs(used)))
                return free

            elif segment.type.source == 'spot':

                free = balances[id].loc[(code, wallet), ('wallet', 'free_value')]

                if Currency.objects.get(code=code).stable_coin:
                    return free

                else:
                    delta = abs(balances[id].loc[(code, wallet), ('delta', 'value')])
                    return min(free, delta)

        # Return value to close
        def to_close(segment):

            base = segment.market.base
            quote = segment.market.quote
            wallet = segment.market.wallet

            delta = abs(balances[id].loc[(base, wallet), ('delta', 'value')])
            position = abs(positions[id].loc[base, quote, wallet].dollar_value[0])
            return min(position, delta)

        # Update rows
        def update_row(index, label, order_value, margin_value):

            # Convert dollar values in orders quantity
            def to_quantity():

                if segment.market.type == 'spot':
                    code = get_code()
                    # Convert order value to currency
                    price = prices['spot'][code]['ask']
                    order_qty = order_value / price

                    return order_qty, None

                elif segment.market.type == 'derivative':

                    # Convert order value to currency
                    price = prices['spot'][segment.market.base]['ask']
                    order_qty = order_value / price

                    # Convert margin value to currency
                    price_m = prices['spot'][segment.market.margined]['ask']
                    margin_qty = margin_value / price_m

                    return order_qty, margin_qty

            # Return the currency code to sell or margin
            def get_code():

                if segment.market.type == 'spot':
                    if segment.type.action == 'buy_base':
                        return segment.market.quote
                    elif segment.type.action == 'sell_base':
                        return segment.market.base
                else:
                    return segment.market.margined

            # Return reduction ratio
            def get_reduction_ratio():

                # Limit funds that should be sold or allocated as margin
                # to the asset quantity held in the wallet (spot, margin)
                if segment.type.id == 1:
                    if segment.type.source in ['spot', 'margin']:

                        code = get_code()

                        # Select available funds
                        if segment.type.transfer:
                            free = balances[id].loc[(code, segment.funds.wallet)].wallet.free_quantity
                        else:
                            free = balances[id].loc[(code, segment.market.wallet)].wallet.free_quantity

                        # Select quantity
                        if segment.market.type == 'spot':
                            quantity = order_qty
                        elif segment.market.type == 'derivative':
                            quantity = margin_qty

                        # Qty can be 0 is get_free() return 0
                        if not quantity:
                            return 0
                        else:
                            ratio = min(free, quantity) / quantity
                            return ratio

                    else:
                        return 1
                else:
                    # For next segments transfer quantity and trade quantity are updated
                    # by update_transfer() after the asset is bought (or margin released) in segment 1
                    return 1

            # Convert order_qty to contract
            def to_contract():

                market = Market.objects.get(exchange=exchange,
                                            symbol=segment.market.symbol,
                                            default_type=segment.market.wallet
                                            )

                # COIN-margined see https://www.binance.com/en/futures/trading-rules/quarterly
                contract_value = float(market.response['info']['contractSize'])  # Select USD value of 1 contract
                cont = order_value / contract_value

                # Round down to the nearest integer
                return int(cont)

            segment = routes[id].loc[index, label]

            # Convert values in dollar to currency quantity
            order_qty, margin_qty = to_quantity()

            # Compare quantity to available funds
            # and return the reduction ratio
            ratio = get_reduction_ratio()

            order_value *= ratio
            order_qty *= ratio
            routes[id].loc[index, (label, 'trade', 'order_value')] = order_value
            routes[id].loc[index, (label, 'trade', 'order_qty')] = order_qty

            # Enter margin and contract
            if segment.market.type == 'derivative':

                margin_value *= ratio
                margin_qty *= ratio
                routes[id].loc[index, (label, 'trade', 'margin_value')] = margin_value
                routes[id].loc[index, (label, 'trade', 'margin_qty')] = margin_qty

                if segment.market.margined == segment.market.base:
                    routes[id].loc[index, (label, 'trade', 'cont')] = to_contract()

            # Enter transfer informations
            if segment.type.transfer:

                # The asset transferred come from a spot or a margin wallet
                # and it should be sold or allocated to a position in segment 1
                if segment.type.id == 1:
                    if segment.type.source in ['spot', 'margin']:

                        # Select asset quantity and wallet
                        asset = segment.funds.code
                        from_wallet = segment.funds.wallet

                        if segment.market.type == 'spot':
                            quantity = order_qty  # sold amount
                        elif segment.market.type == 'derivative':
                            quantity = margin_qty

                        routes[id].loc[index, (label, 'transfer', 'asset')] = asset
                        routes[id].loc[index, (label, 'transfer', 'quantity')] = quantity
                        routes[id].loc[index, (label, 'transfer', 'from_wallet')] = from_wallet
                        routes[id].loc[index, (label, 'transfer', 'to_wallet')] = segment.market.wallet

                # Transfer information is added by update_transfer()
                # after the asset is bought (released) in segment n-1
                else:

                    if label == 's2':
                        prev = routes[id].loc[index, 's1']
                    if label == 's3':
                        prev = routes[id].loc[index, 's2']

                    # Asset was release by closing a position
                    if prev.market.type == 'derivative':
                        asset = prev.market.margined
                        quantity = prev.trade.margin_qty

                        # Remove fees
                        order = prev.trade.order_qty
                        fees = order * 0.9996 if asset == prev.market.base else order * 0.9995
                        quantity -= fees

                        routes[id].loc[index, (label, 'transfer', 'asset')] = asset
                        routes[id].loc[index, (label, 'transfer', 'quantity')] = quantity
                        routes[id].loc[index, (label, 'transfer', 'from_wallet')] = prev.market.wallet
                        routes[id].loc[index, (label, 'transfer', 'to_wallet')] = segment.market.wallet

                    else:
                        # If the asset that should be transferred was initially bought in spot
                        # in the previous segment then the quantity to be transferred is update
                        # by update_transfer() after the trade is executed
                        pass

        # Return True if market is coin-margined.
        def is_coin_margined(segment):
            if Currency.objects.get(code=segment.market.margined).stable_coin:
                return False
            else:
                return True

        # Return True if market is usd-margined
        def is_usd_margined(segment):
            if Currency.objects.get(code=segment.market.margined).stable_coin:
                return True
            else:
                return False

        # Compensate margin used and open value if a position is coin-margined
        # Margin add extra exposure so reduce it if open_long else increase it.
        def compensate_margin(segment, open, margin, close=None):

            leverage1 = float(account.leverage)

            # Asset exposure is position value plus (minus) margin used
            if segment.type.action == 'open_long':
                total = open + margin
            elif segment.type.action == 'open_short':
                total = open - margin

            # Determine the compensation ratio (<1 if open_long else >1)
            # and apply it to open and margin of the position to open
            ratio = open / total
            open_comp = open * ratio
            margin_comp = margin * ratio

            # If a position need to be closed in the first segment
            # recalculate the close value based on the new margin
            if close:
                close_comp = margin * leverage1

            log.info('Compensate {1} margin by a ratio of {0}'.format(round(ratio, 2), segment.market.symbol))

            # If the new margin should be increased (open_short)
            if margin_comp > margin:

                # Then determine the reduction ratio
                reduction_ratio = margin / margin_comp

                log.info('Adjust margin used to available funds by {0}'.format(round(reduction_ratio, 2)))

                # and adjust margin value and open value to funds available
                margin_comp *= reduction_ratio  # = margin
                open_comp *= reduction_ratio

                # Recalculate the close value
                if close:
                    close_comp = margin_comp * leverage1

            if close:
                return open_comp, margin_comp, close_comp
            else:
                return open_comp, margin_comp

        # Limit spot buying if a new hedge is added with a buy and capacity is reached
        def limit_buy(code, buy, close=None):

            shorts = account.get_shorts(prices, code)
            balance = account.get_balance(prices, code)

            # Short positions are larger than coin balance
            if shorts > balance:

                # Determine hedge added by the buy
                max_added = shorts - balance
                hedge_added = min(buy, max_added)

                # Determine margin added to account capacity
                # if hedge is allocated to USD-margined position
                margins = []

                # Select short position opened for the code and sort dataframe
                # so that USDT-margined positions with hedge are at the top
                pos = positions[id].loc[positions[id].side == 'sell'].loc[code, :]
                pos.sort_index(level='margined', ascending=False, axis=0, inplace=True)
                for index, row in pos.iterrows():
                    if Currency.objects.get(code=index[5]).stable_coin:
                        if row.hedge_position_ratio < 1:

                            # Determine position value not allocated to hedge
                            pos_capacity = row.net_value * (1 - row.hedge_position_ratio)

                            if hedge_added < pos_capacity:
                                margin = hedge_added / row.leverage
                                margins.append(margin)
                                break

                            else:
                                # If the new hedge is larger than hedge capacity
                                # of the position then keep the margin and loop
                                # to the next position
                                margin = pos_capacity / row.leverage
                                margins.append(margin)

                # Get total margin needed for the hedge and total capacity used
                margin = sum(margins)
                capacity_used = hedge_added + margin

                if capacity_used > synthetic_cash[id]['capacity']:
                    reduction_ratio = synthetic_cash[id]['capacity'] / capacity_used

                    log.warning('Limit spot buy for {0} by a ratio of {1}'.format(code, reduction_ratio))

                    buy *= reduction_ratio
                    if close:
                        close *= reduction_ratio

            if close is not None:
                return buy, close
            else:
                return buy

        start = timer()
        account = Account.objects.get(id=id)
        routes[id].sort_index(axis=0, inplace=True)

        # Iterate through routes and set trade quantity
        for index, route in routes[id].iterrows():

            if 's1' in route:
                base1 = route.s1.market.base
                quote1 = route.s1.market.quote
                wall1 = route.s1.market.wallet
                symb1 = route.s1.market.symbol

            if 's2' in route:
                base2 = route.s2.market.base
                quote2 = route.s2.market.quote

            if 's3' in route:
                base3 = route.s3.market.base
                quote3 = route.s3.market.quote

            # Determine trades quantity
            ###########################

            if route.s1.type.source == 'close_position':

                leverage1 = float(account.leverage)
                close = to_close(route.s1)
                margin_released = close / leverage1

                # One segment
                if pd.isna(route.s2.type.action):
                    update_row(index, 's1', close, margin_released)

                # Two segments
                elif pd.isna(route.s3.type.action):
                    delta2 = get_delta(base2)

                    if route.s2.type.action in ['buy_base', 'sell_base']:
                        margin_released = min(margin_released, delta2)
                        close = margin_released * leverage1  # recalculate close of segment 1

                        # Select currency to buy in spot and test hedge capacity
                        code = base2 if route.s2.type.action == 'buy_base' else quote2
                        margin_released, close = limit_buy(code, margin_released, close)

                        update_row(index, 's1', close, margin_released)
                        update_row(index, 's2', margin_released, None)

                    elif route.s2.type.action in ['open_long', 'open_short']:
                        leverage2 = float(account.leverage)
                        required = delta2 / leverage2
                        margin_released = min(margin_released, required)
                        open = margin_released * leverage2
                        close = margin_released * leverage1  # recalculate close of segment 1

                        # Compensate margin
                        if is_coin_margined(route.s2):
                            pass
                            # open, margin_released, close = compensate_margin(route.s2, open, margin_released, close)

                        update_row(index, 's1', close, margin_released)
                        update_row(index, 's2', open, margin_released)

                # Tree segments
                else:
                    delta3 = get_delta(base3)
                    leverage1 = float(account.leverage)

                    if route.s3.type.action in ['buy_base', 'sell_base']:
                        margin_released = min(margin_released, delta3)
                        close = margin_released * leverage1  # recalculate close of segment 1

                        # Select currency to buy in spot and test hedge capacity
                        code = base3 if route.s3.type.action == 'buy_base' else quote3
                        margin_released, close = limit_buy(code, margin_released, close)

                        update_row(index, 's1', close, margin_released)
                        update_row(index, 's2', margin_released, None)  # Gateway
                        update_row(index, 's3', margin_released, None)

                    elif route.s3.type.action in ['open_long', 'open_short']:
                        leverage3 = float(account.leverage)
                        required = delta3 / leverage3
                        margin_released = min(margin_released, required)
                        open = margin_released * leverage3
                        close = margin_released * leverage1  # recalculate close of segment 1

                        # Compensate margin
                        if is_coin_margined(route.s3):
                            pass
                            # open, margin_released, close = compensate_margin(route.s3, open, margin_released, close)

                        update_row(index, 's1', close, margin_released)
                        update_row(index, 's2', margin_released, None)  # Gateway
                        update_row(index, 's3', open, margin_released)

            elif route.s1.type.source == 'close_hedge':

                account = Account.objects.get(id=id)
                quote1 = route.s1.market.quote
                leverage1 = float(account.leverage)

                # Select hedge capacity and reduce it by 5% to close hedge
                # a bit than what is really necessary (prevent ping-pong)
                capacity = synthetic_cash[id]['capacity']
                offset = capacity * 0.05
                capacity += offset
                capacity = abs(capacity)

                log.info('Determine value to close short for {0}'.format(base1))

                # Get position value
                open = abs(positions[id].loc[base1, quote1, wall1].value[0])

                # Get hedge level as min(balance, shorts)
                hedge = account.get_hedge(base1)

                # Get hedge ratio (shorts / balance)
                hedge_ratio = account.get_hedge_ratio(base1)

                log.info('Hedge ratio for {0} is {1}'.format(base1, round(hedge_ratio, 2)))

                # Currency is fully hedged
                if hedge_ratio > 1:

                    # Determine short that need to be closed before reaching the hedge
                    short = hedge * (hedge_ratio - 1)

                    log.info('{0} USD of short sell to close first'.format(round(short, 2)))

                else:
                    short = 0

                # Determine total value that should be closed
                total = short + capacity

                # Determine what can be closed
                close = min(open, total)

                # If hedge is reached
                if close > short:

                    # Determine value of hedge closed
                    close_hedge = close - short

                    log.info('Additional hedge closed for {0}'.format(round(close_hedge, 2)))

                    if is_usd_margined(route.s1):
                        # Get position value allocated to hedge in the position
                        hedge_position = positions[id].loc[route.s1.market.base,
                                                           route.s1.market.quote,
                                                           route.s1.market.wallet].hedge_position[0]

                        # Determine ratio of hedge closed in this position
                        close_ratio = close_hedge / hedge_position

                        # Get margin allocated to a hedge if position is usd-margined
                        hedge_margin = positions[id].loc[route.s1.market.base,
                                                         route.s1.market.quote,
                                                         route.s1.market.wallet].hedge_margin[0]

                        # Determine margin released
                        margin_release = hedge_margin * close_ratio

                        log.info('Additional margin released for {0}'.format(round(margin_release, 2)))

                        # Determine hedge capacity released
                        total = close_hedge + margin_release
                        to_release = min(total, capacity)

                        # Determine close value from to_release so that
                        # the value of closed short + margin = to_release
                        margin = to_release / leverage1
                        ratio = margin / (to_release + margin)
                        close_hedge = to_release * ratio

                        log.info('Final hedge value closed is {0}'.format(round(close_hedge, 2)))
                        log.info('Final margin value released is {0}'.format(round(close_hedge / leverage1, 2)))
                        log.info('Total capacity released is {0}'.format(round(to_release, 2)))

                        # Finally determine total value to close_short (short + close_hedge)
                        close = short + close_hedge

                else:
                    log.info('No hedge will be reached by closing {0} {1}'.format(symb1, wall1))

                margin_released = close / leverage1

                # Update segment
                update_row(index, 's1', close, margin_released)

            elif route.s1.type.source in ['spot', 'margin']:

                # Select funds
                code = route.s1.funds.code
                wallet = route.s1.funds.wallet
                free = get_free(route.s1, code, wallet)

                # One segment
                if pd.isna(route.s2.type.action):
                    delta1 = get_delta(base1)

                    if route.s1.type.action in ['buy_base', 'sell_base']:
                        used = min(free, delta1)
                        # Select currency to buy in spot and test hedge capacity
                        code = base1 if route.s1.type.action == 'buy_base' else quote1
                        used = limit_buy(code, used)

                        update_row(index, 's1', used, None)

                    elif route.s1.type.action in ['open_long', 'open_short']:
                        leverage1 = float(account.leverage)
                        margin_required = delta1 / leverage1
                        used = min(free, margin_required)
                        open = used * leverage1

                        if is_coin_margined(route.s1):
                            # Compensate margin used and open value if a position is coin-margined
                            # Margin add extra exposure so reduce it if open_long else increase it.
                            pass
                            # open, used = compensate_margin(route.s1, open, used)

                        update_row(index, 's1', open, used)

                # Two segments
                elif pd.isna(route.s3.type.action):
                    delta2 = get_delta(base2)

                    if route.s2.type.action in ['buy_base', 'sell_base']:
                        used = min(free, delta2)

                        # Select currency to buy in spot and test hedge capacity
                        code = base2 if route.s2.type.action == 'buy_base' else quote2
                        used = limit_buy(code, used)

                        update_row(index, 's1', used, None)  # Gateway
                        update_row(index, 's2', used, None)

                    elif route.s2.type.action in ['open_long', 'open_short']:
                        leverage2 = float(account.leverage)
                        required = delta2 / leverage2
                        used = min(free, required)
                        open = used * leverage2

                        if is_coin_margined(route.s2):
                            # Compensate margin used and open value if a position is coin-margined
                            # Margin add extra exposure so reduce it if open_long else increase it.
                            pass
                            # open, used = compensate_margin(route.s2, open, used)

                        update_row(index, 's1', used, None)  # Gateway
                        update_row(index, 's2', open, used)

        routes[id].sort_index(axis=1, inplace=True)

        end = timer()
        log.info('Size orders and transfer in {0} sec'.format(round(end - start, 2)))

    # Limit short position to avoid lack of funds
    def limit_new_short(id):

        start = timer()

        account = Account.objects.get(id=id)
        capacity = synthetic_cash[id]['capacity']

        if capacity > 0:

            for index, route in routes[id].iterrows():

                # Determine number of segments and create a list of labels
                segments = ['s' + str(i) for i in range(1, route.length[0] + 1)]

                for segment in segments:
                    if route[segment].type.action == 'open_short':

                        # Select base currency
                        base = route[segment].market.base

                        # Determine threshold above which a short position isn't a hedge but a short sell.
                        # Threshold represent the maximum hedging value of a currency
                        max_hedge = account.get_max_hedge(prices, base)

                        # If more hedge can be added to the currency then determine ratio of the new short that
                        # becomes a hedge and test if the hedge (+ it's margin if USD margined) bypass capacity
                        if max_hedge > 0:

                            # Select desired value to short
                            open_short = route[segment].trade.order_value
                            if open_short:

                                # Determine hedge value and ratio
                                hedge = min(open_short, max_hedge)
                                hedge_ratio = hedge / open_short

                                # Select initial_margin if the position is USD-margined
                                # and determine the margin allocated to the hedge
                                if Currency.objects.get(code=route[segment].market.margined).stable_coin:
                                    initial_margin = route[segment].trade.margin_value
                                    hedge_margin = initial_margin * hedge_ratio
                                else:
                                    hedge_margin = 0

                                capacity_used = hedge + hedge_margin

                                # Limit the short if the hedge and it's margin overpass capacity
                                if capacity_used > capacity:

                                    # Calculate reduction ratio so that capacity_used stay lower than capacity.
                                    # Keep a margin of 5% to prevent ping-pong
                                    ratio = capacity / capacity_used
                                    offset = ratio * 0.05
                                    ratio -= offset

                                    log.warning('Capacity used must be limited in route {0} by ratio of {1})'.
                                                format(index, round(ratio, 3)))

                                    # Minimize hedge to the ideal value
                                    hedge_min = hedge * ratio

                                    if hedge_min < max_hedge:
                                        # Calculate the reduction ratio based on hedge_min and open_short
                                        # to avoid additional max_hedge - hedge_min being added to the position
                                        ratio = hedge_min / open_short
                                        log.warning('Capacity used must be limited in route {0} by ratio of {1})'.
                                                    format(index, round(ratio, 3)))

                                    # Finally apply reduction ratio
                                    hedge *= ratio
                                    open_short *= ratio
                                    hedge_margin *= ratio

                                    capacity_used = hedge + hedge_margin
                                    hedge_ratio = hedge / open_short

                                    routes[id].loc[index, (segment, 'hedge', 'capacity')] = capacity
                                    routes[id].loc[index, (segment, 'hedge', 'max_hedge')] = max_hedge
                                    routes[id].loc[index, (segment, 'hedge', 'hedge')] = hedge
                                    routes[id].loc[index, (segment, 'hedge', 'hedge_ratio')] = hedge_ratio
                                    routes[id].loc[index, (segment, 'hedge', 'hedge_margin')] = hedge_margin
                                    routes[id].loc[index, (segment, 'hedge', 'capacity_used')] = capacity_used
                                    routes[id].loc[index, (segment, 'hedge', 'reduction_ratio')] = ratio

                                    # Apply reduction ratio to trades values in all segments
                                    for s in segments:
                                        routes[id].loc[index, (s, 'trade', 'order_value')] *= ratio
                                        routes[id].loc[index, (s, 'trade', 'margin_value')] *= ratio
                                        routes[id].loc[index, (s, 'trade', 'order_qty')] *= ratio
                                        routes[id].loc[index, (s, 'trade', 'margin_qty')] *= ratio

                                    # Escape segments loop and test the nest route
                                    break
                                else:
                                    log.info('Hedge added in route {0}, {1} capacity used'.format(index,
                                                                                                  round(capacity_used,
                                                                                                        2)))
                        else:
                            log.info('No hedge added in route {0}, {1} is fully hedged'.format(index, base))

            routes[id].sort_index(axis=1, inplace=True)

            end = timer()
            log.info('Limit new shorts in {0} sec'.format(round(end - start, 2)))

        else:
            log.info('Limit to open short not applied because capacity is negative')

    # Validate orders of our routes
    def validate_orders(id):

        start = timer()

        account = Account.objects.get(id=id)

        # Get side of a trade
        def get_side():

            if action in ['open_long', 'close_short', 'buy_base']:
                return 'buy'
            else:
                return 'sell'

        # Get latest price
        def get_price(market):
            if account.limit_order:
                if exchange.has['createLimitOrder']:

                    # Select hourly price if limit order
                    price = market.get_candle_price_last()

                    # Add/remove tolerance
                    if side == 'buy':
                        price += price * float(account.limit_price_tolerance)
                    elif side == 'sell':
                        price -= price * float(account.limit_price_tolerance)
                    return price

                else:
                    raise Exception('Limit order not supported')

            else:
                if exchange.has['createMarketOrder']:
                    # Select spot price to validate MIN_NOTIONAL
                    return prices['spot'][market.base.code]['ask']
                else:
                    raise Exception('Market order not supported')

        # Test MIN_NOTIONAL
        def test_min_notional(market, amount, price, quote_order_qty=False):

            if quote_order_qty:
                cost = amount
            else:
                cost = amount * price

            min_notional = limit_cost(market, cost)

            if market.exchange.exid == 'binance':

                # If market is spot and if condition is applied to MARKET order
                if market.type == 'spot':
                    if market.response['info']['filters'][3]['applyToMarket']:
                        if min_notional:
                            return True, None
                    else:
                        return True, None

                # If market is USDT margined and if verification fails set reduce_only = True
                elif not min_notional:
                    if market.type == 'derivative':
                        if market.margined.code == 'USDT':
                            if action in ['close_long', 'close_short']:
                                return True, True  # Reduce only = True
                else:
                    return True, None
            else:
                if min_notional:
                    return True, None

            # In last resort return False
            return False, None

        for index, route in routes[id].iterrows():

            # Create a list with our segments
            segments = ['s' + str(i) for i in range(1, route.length[0] + 1)]
            for i, s in enumerate(segments):

                action = route[s].type.action
                symbol = route[s].market.symbol
                wallet = route[s].market.wallet
                order_qty = route[s].trade.order_qty
                amount = order_qty
                quote_order_qty = False

                if i > 0:
                    # Set valid flag to False if previous trade not valid
                    if not routes[id].loc[index, 's' + str(i)].trade.valid:
                        routes[id].loc[index, (s, 'trade', 'valid')] = False
                        continue

                market = Market.objects.get(exchange=exchange, symbol=symbol, default_type=wallet)
                price = get_price(market)
                side = get_side()

                # Set quote_order_qty flag
                if route[s].market.type == 'spot':
                    if action == 'buy_base':
                        quote_order_qty = True

                # Use contract size if coin-margined
                if market.type == 'derivative':
                    if market.derivative == 'perpetual':
                        if market.margined == market.base:
                            amount = route[s].trade.cont

                amount = format_decimal(counting_mode=exchange.precision_mode,
                                        precision=market.precision['amount'],
                                        n=amount
                                        )

                if limit_amount(market, amount) or quote_order_qty:

                    # MIN_NOTIONAL
                    min_notional, reduce_only = test_min_notional(market, amount, price, quote_order_qty)
                    if min_notional:

                        # Set parameters
                        params = {}
                        if reduce_only:
                            params['reduceonly'] = True
                        if quote_order_qty:
                            params['quoteOrderQty'] = amount

                        # Insert trade informations
                        routes[id].loc[index, (s, 'trade', 'valid')] = True
                        routes[id].loc[index, (s, 'trade', 'params')] = str(params) if params else np.nan
                        routes[id].loc[index, (s, 'trade', 'side')] = side
                        routes[id].loc[index, (s, 'trade', 'order_qty')] = amount
                        if account.limit_order:
                            routes[id].loc[index, (s, 'trade', 'price')] = price

                        # Insert contract
                        if market.type == 'derivative':
                            if market.derivative == 'perpetual':
                                if market.margined == market.base:
                                    routes[id].loc[index, (s, 'trade', 'cont')] = amount
                                    routes[id].loc[index, (s, 'trade', 'order_qty')] = order_qty

                    else:
                        routes[id].loc[index, (s, 'trade', 'valid')] = False
                        routes[id].loc[index, (s, 'trade', 'error')] = 'min_notional'
                else:
                    routes[id].loc[index, (s, 'trade', 'valid')] = False
                    routes[id].loc[index, (s, 'trade', 'error')] = 'limit_amount'

        routes[id].sort_index(axis=1, inplace=True)

        end = timer()
        log.info('Validate trades in {0} sec'.format(round(end - start, 2)))

    # Drop routes with invalid trade
    def drop_routes(id):

        invalid = []
        for index, route in routes[id].iterrows():

            # Determine number of segments
            instructions = [route[s].type.action for s in ['s1', 's2', 's3']]
            instructions = [i for i in instructions if not pd.isna(i)]

            # Get valid flags of our segments
            segments = ['s' + str(s + 1) for s in range(len(instructions))]
            trades = [route[s].trade.valid for s in segments]

            if False in trades:
                invalid.append(index)

        # log.info('Dropped routes')
        # print(routes[id].iloc[invalid].to_string())

        # Drop invalid routes
        routes[id] = routes[id].drop(invalid)

        # log.info('Valid routes')
        # print(routes[id].to_string())

        # Drop unused columns
        routes[id].drop('valid', axis=1, level=2, inplace=True)
        routes[id].drop('error', axis=1, level=2, inplace=True)

        routes[id].sort_index(axis=1, inplace=True)

    # Calculate routes cost
    def calculate_cost(id, market, bids, asks):

        # Get bids or asks
        def get_depth(df):

            if df.type.action in ['sell_base', 'close_long', 'open_short']:
                return bids
            elif df.type.action in ['buy_base', 'open_long', 'close_short']:
                return asks

        # Get average price distance from best bid (ask)
        def get_distance(depth, quantity):

            book = depth

            if not pd.isna(quantity):
                # Iterate through depth until desired amount is available
                for i, b in enumerate(book):
                    if b[1] > quantity:
                        if i == 0:
                            return 0
                        else:
                            book = book[:i]  # select the first n elements needed
                            break

                # select prices and sum total quantity needed
                prices = [p[0] for p in book]
                qty = sum([q[1] for q in book])

                # weight each element and multiply prices by weights and sum
                weights = [q[1] / qty for q in book]
                average_price = sum([a * b for a, b in zip(prices, weights)])

                # Calculate distance in % to the best bid or to the best ask
                distance = abs(100 * (average_price / book[0][0] - 1))

                return distance

        # Get bid-ask spread
        def get_spread():

            spread = asks[0][0] - bids[0][0]
            spread_pct = spread / asks[0][0]

            return spread_pct * 100

        start = timer()

        # Iterate through routes and set cost
        for index, route in routes[id].iterrows():

            # Determine number of segments
            segments = ['s' + str(i) for i in range(1, route.length[0] + 1)]

            for segment in segments:

                # If market of the segment is the market of the asyncio loop
                if route[segment].market.symbol == market.symbol:
                    if route[segment].market.wallet == market.default_type:
                        depth = get_depth(route[segment])
                        distance = get_distance(depth, route[segment].trade.order_qty)
                        spread = get_spread()

                        # Set costs
                        routes[id].loc[index, (segment, 'cost', 'spread')] = spread
                        routes[id].loc[index, (segment, 'cost', 'distance')] = distance
                        routes[id].loc[index, (segment, 'cost', 'total')] = spread + distance

            # Set total cost of the route
            if all(['cost' in route[s] for s in segments]):
                costs = [route[s].cost.total for s in segments]
                if not any(np.isnan(costs)):
                    routes[id].loc[index, ('best', 'cost', '')] = sum(costs)
                else:
                    pass
                    # print('Route with id', index, 'is not ready')

        routes[id].sort_index(axis=1, inplace=True)

    # Calculate daily return
    def calculate_return(id):

        start = timer()

        for index, route in routes[id].iterrows():

            # Determine number of segments
            segments = ['s' + str(i) for i in range(1, route.length[0] + 1)]

            for segment in segments:
                if route[segment].market.type == 'derivative':
                    if route[segment].market.derivative == 'perpetual':

                        market = Market.objects.get(exchange=exchange,
                                                    symbol=route[segment].market.symbol,
                                                    default_type=route[segment].market.wallet
                                                    )
                        funding = float(market.funding_rate['lastFundingRate'])
                        order_value = route[segment].trade.order_value
                        action = route[segment].type.action

                        if action in ['open_long', 'open_short']:

                            ret = order_value * funding

                            if action == 'open_long':
                                ret = -ret

                            # Calculate return over the next 7 days
                            ret *= 24 / float(exchange.funding_rate_freq) * 7

                            routes[id].loc[index, (segment, 'market', 'rate')] = funding
                            routes[id].loc[index, ('best', 'return', '')] = ret

        end = timer()
        log.info('Calculate return in {0} sec'.format(round(end - start, 2)))

    # Sort routes by priority and cost
    def sort_routes(id):

        # Sort routes by cost
        if 'best' in routes[id]:
            if 'cost' in routes[id].best:
                if 'return' in routes[id].best:
                    routes[id].sort_values([('s1', 'type', 'priority'),
                                            ('best', 'cost')], ascending=[True, True], inplace=True)

    # Update df_markets with order status after an order is placed
    def update_markets_df(id, orderid):

        order = Order.objects.get(orderid=orderid)

        # Log order status
        # if order.status == 'closed':
        #     log.info('Place order filled')
        # elif order.status == 'open':
        #     log.info('Place order pending')

        log.info('Update market dataframe')

        # Construct index
        idx = (order.market.base.code,
               order.market.quote.code,
               'default' if order.market.default_type is None else order.market.default_type,
               order.market.symbol,
               order.market.type
               )

        if order.market.type == 'derivative':
            lst = list(idx)
            lst.extend([order.market.derivative, order.market.margined.code])
            idx = tuple(lst)

        # Add/update order informations
        markets[id].loc[idx, ('order', 'id')] = order.id
        markets[id].loc[idx, ('order', 'type')] = order.route_type
        markets[id].loc[idx, ('order', 'amount')] = order.amount
        markets[id].loc[idx, ('order', 'status')] = order.status
        markets[id].loc[idx, ('order', 'filled')] = order.filled

        log.info('Update market dataframe OK')

    # Update the latest fund object of an account after a trade is executed
    def update_fund_object(id, orderids):

        log.info('Update fund object')

        # Select wallet of markets where trades occurred
        wallets = list(set([order.market.default_type for order in Order.objects.filter(orderid__in=orderids)]))

        if wallets:
            for wallet in wallets:
                create_fund.run(id, wallet=wallet)
        else:
            create_fund.run(id, wallet='default')

        log.info('Update fund object done')

    # Update open orders and return a list of orders with new trades
    def update_orders(pk):

        # Fetch open orders and update order objects
        account = Account.objects.get(pk=pk)
        open_orders = account.get_pending_order_ids()

        if open_orders:

            log.info('Update open orders')

            tasks = [update_order_id.si(pk, orderid) for orderid in open_orders]  # create a list of task
            result = group(*tasks).apply_async(queue='slow')  # execute tasks in parallel

            while not result.ready():
                time.sleep(0.5)

            # Update complete
            if result.successful():

                log.info('Open orders update success')

                # Return a list of ids for orders with new trade
                res = result.get(disable_sync_subtasks=False)
                orderids = [orderid for orderid in res if orderid is not None]
                return orderids

            else:
                log.error('Open orders update failed')

    # Collect latest prices
    def collect_prices(prices, market, bids, asks):

        p = dict(ask=asks[0][0], bid=bids[0][0])

        # Assign spot price of base
        if market.type == 'spot':
            if market.quote.code == exchange.dollar_currency:
                prices['spot'][market.base.code] = p

        # Collect market price
        prices[market.default_type][market.symbol] = p

    # Place an order to the best route for every source currency
    # and update df_markets when an order is placed
    def trade(id):

        # Update transfer quantity and trade size in segment n
        # after an asset is bought or released in segment n-1.
        def update_transfer():

            # Update the asset quantity that should be transferred in segment n+1
            # after a trade is executed to buy a currency in segment n (bridge)
            if route[segment].type.id < route.length[0]:
                next = 's' + str(route[segment].type.id + 1)
                if route[next].type.transfer:

                    if response['status'] == 'closed':

                        # Get the quantity we just bought
                        if route[segment].type.action == 'buy_base':
                            bought = response['filled']
                        elif route[segment].type.action == 'sell_base':
                            bought = response['filled'] * response['average']

                        # Update transfer quantity
                        routes[id].iloc[0, (next, 'transfer', 'quantity')] = bought

                        # Update trade quantity
                        if route[next].market.type == 'derivative':
                            routes[id].iloc[0, (next, 'trade', 'margin_qty')] = bought
                        else:
                            routes[id].iloc[0, (next, 'trade', 'order_qty')] = bought

                        log.info('Update transfer and trade quantity in segment n+1 with {0} {1}'.format(
                            round(bought, 2),
                            route[next].transfer.asset))

        print('\n', balances[id].to_string(), '\n')
        print('\n', routes[id].to_string(), '\n')
        print('\n', positions[id].to_string(), '\n')

        # Select the best route
        route = routes[id].iloc[0]
        if not route.empty:

            start = timer()
            log.info('Trade route {0}'.format(route.name))

            # Loop through all segments
            length = route.length[0]
            segments = ['s' + str(i) for i in range(1, length + 1)]

            for i, segment in enumerate(segments):

                log.info('Trade segment {0}/{1}'.format(i + 1, length))

                # Transfer funds
                if route[segment].type.transfer:
                    res = transfer(id, route[segment])
                    if not res:
                        return

                # Create an order object
                orderid = Account.objects.get(id=id).create_order(route, segment)
                if orderid:

                    log.info('Order object created')

                    # Place order
                    response = place_order.run(id, orderid)
                    if response:

                        log.info('Order placed')
                        print(route[segment])

                        # Update order object
                        order_create_update(id, response)
                        update_transfer()

                    else:
                        log.warning('Order placement failed')
                        return
                else:
                    log.warning('Order object creation failed')
                    return

            end = timer()
            elapsed = end - start
            log.info(
                '{0} trade(s) complete in {1} sec for route {2}'.format(length, round(elapsed, 2), int(route.name)))

            # Trades success
            return True

        else:
            # Rebalance complete
            return False

    # Test if routes are ready to trade
    def has_routes(id):

        if has_dataframes(id):
            if 'best' in routes[id]:
                if 'cost' in routes[id].best:
                    if not any(np.isnan(routes[id].best.cost)):
                        return True

        print(routes[id].to_string())

    # Return True if dataframes are created
    def has_dataframes(id):
        status = [id in dic for dic in [balances, positions, markets, synthetic_cash, routes]]
        if all(status):
            return True
        else:
            return False

    # Return True if price of all markets are collected
    def has_prices():
        for k, v in prices.items():
            for m in v.keys():
                if not prices[k][m]['ask']:
                    return

        return True

    # Create dictionaries
    def dictionaries(id, rebuild=None):

        # Create dataframes if they are not in dictionaries for account id
        if has_prices() and (rebuild or not has_dataframes(id)):
            start = timer()
            log.info('Dictionaries creation')

            # Select account
            account = Account.objects.get(id=id)

            # Update objects
            create_fund.run(id)
            update_positions.run(id)

            # Create dataframes
            balances[id], positions[id] = account.create_dataframes(prices)
            markets[id] = create_markets(id)
            update_synthetic_cash(id)

            # Create routes dataframe for the account
            create_routes(id)

            # Build dataframe
            size_orders(id)
            limit_new_short(id)
            validate_orders(id)
            calculate_return(id)
            drop_routes(id)

            end = timer()
            elapsed = end - start
            log.info('Dictionaries created in {0} sec'.format(round(elapsed, 2)))

            # Signal dataframes are created
            return True

    # Return objects of accounts to be updated
    def get_accounts(updated=None):
        accounts = Account.objects.filter(strategy=strategy,
                                          exchange=exchange,
                                          trading=True
                                          )
        if updated is not None:
            accounts = accounts.filter(updated=updated)

        return accounts

    # Receive websocket streams of book depth
    async def watch_book(client, market, i, j):

        log.info('Start market loop')

        wallet = market.default_type
        symbol = market.symbol

        while True:
            try:
                ob = await client.watch_order_book(symbol)  # , limit=account.exchange.orderbook_limit)
                if ob:

                    # Capture current depth
                    bids, asks = cumulative_book(ob)
                    # print(datetime.now(), i, j, market.default_type[:4], market.symbol, bids[0][0])

                    # Collect prices
                    collect_prices(prices, market, bids, asks)

                    # Get accounts that need an update
                    accounts = get_accounts(updated=False)
                    if accounts.exists():

                        for account in accounts:
                            id = account.id

                            # Create dictionaries when prices are collected
                            if has_prices() and not has_dataframes(id):
                                dictionaries(id)

                            # Dataframes are created
                            elif has_dataframes(id):

                                if not routes[id].empty:

                                    # Start timer
                                    start = timer()

                                    # Update costs and sort routes
                                    calculate_cost(id, market, bids, asks)
                                    sort_routes(id)

                                    # End timer
                                    # print(wallet, symbol, round(timer() - start, 2), 'sec')

                                else:

                                    log.info('Route not found for account {0}'.format(id))

                                    print(balances[id].to_string())
                                    account.updated = True
                                    account.save()
                                    continue

                        if i == j == 0:

                            for account in get_accounts(updated=False):

                                id = account.id
                                if has_dataframes(id):
                                    if has_routes(id):

                                        # Trade the best route
                                        res = trade(id)
                                        if res:

                                            # Construct new dataframes
                                            dictionaries(id, rebuild=True)

                                            # Update objects of open orders and return a list if trade detected
                                            orderids = update_orders(id)

                                            if orderids:
                                                log.info('Trades detected')
                                                print(orderids)

                                                # Update df_markets
                                                [update_markets_df(id, orderid) for orderid in orderids]

                                                # Update df_positions if a trade occurred on a derivative market
                                                update_positions.run(id, orderids)

                                                # Update the latest fund object and df_account
                                                update_fund_object(id, orderids)

                                        elif res is False:

                                            account.updated = True
                                            account.save()

                                            log.info('Rebalance OK for account {0}'.format(id))
                                            continue
                                        else:
                                            log.warning('Rebalance failed')
                                            print(routes[id].to_string())
                                            continue
                                    else:
                                        log.info('Routes are not complete yet...')
                                else:
                                    log.info('Dataframe are not created yet...', market=market, account=account)
                    else:

                        log.info('Closing stream {0}'.format(symbol))
                        break
                else:
                    print('NO order_book\t', symbol, wallet)
                    print('wait')

                # print('wait\t', symbol, wallet)
                await client.sleep(1000)

            except Exception as e:
                # print('exception', str(e))
                traceback.print_exc()
                raise e  # uncomment to break all loops in case of an error in any one of them
                # break  # you can break just this one loop if it fails

    # Configure websocket client for wallet
    async def wallet_loop(loop, i, wallet):

        log.info('Start wallet loop')
        # EventLoopDelayMonitor(interval=1)

        client = getattr(ccxtpro, exchange.exid)({'enableRateLimit': True, 'asyncio_loop': loop, })

        if exchange.default_types:
            client.options['defaultType'] = wallet

        # Filter markets to monitor
        mks = Market.objects.filter(exchange=exchange,
                                    default_type=wallet,
                                    base__code__in=codes,
                                    quote__code__in=codes,
                                    excluded=False,
                                    active=True
                                    ).exclude(derivative='future')

        # Filter updated markets
        mks = [m for m in mks if m.is_updated()]

        log.info('Found {0} markets'.format(len(mks)), wallet=wallet)

        # Create dictionary structure for spot prices in (usd)
        for market in mks:

            wallet = market.default_type
            base = market.base.code
            symbol = market.symbol

            # Create a key for spot
            if 'spot' not in prices:
                prices['spot'] = {}
                prices['spot'][exchange.dollar_currency] = {}
                prices['spot'][exchange.dollar_currency]['ask'] = 1

            # Create nested dictionaries for codes
            if market.type == 'spot':
                if base not in prices[wallet]:
                    prices[wallet][base] = {}
                    prices[wallet][base]['ask'] = {}

            # Create a key wallets
            if wallet not in prices:
                prices[wallet] = {}

            # Create nested dictionaries for symbols
            if symbol not in prices[wallet]:
                prices[wallet][symbol] = {}
                prices[wallet][symbol]['ask'] = {}

        # [print(m.default_type, m.symbol) for m in markets]

        ws_loops = [watch_book(client, market, i, j) for j, market in enumerate(mks)]

        await asyncio.gather(*ws_loops)
        await client.close()

    # Run main asyncio loop
    async def main(loop):
        log.info('Start main loop')
        wallet_loops = [wallet_loop(loop, i, wallet) for i, wallet in enumerate(exchange.get_default_types())]
        await asyncio.gather(*wallet_loops)

    class EventLoopDelayMonitor:

        def __init__(self, loop=None, start=True, interval=1, logger=None):
            self._interval = interval
            self._log = logger or logging.getLogger(__name__)
            self._loop = loop or asyncio.get_event_loop()
            if start:
                self.start()

        def run(self):
            self._loop.call_later(self._interval, self._handler, self._loop.time())

        def _handler(self, start_time):
            latency = (self._loop.time() - start_time) - self._interval
            self._log.error('EventLoop delay %.4f', latency)
            if not self.is_stopped():
                self.run()

        def is_stopped(self):
            return self._stopped

        def start(self):
            self._stopped = False
            self.run()

        def stop(self):
            self._stopped = True

    # Select objects
    ################

    strategy = Strategy.objects.get(id=id)
    exchange = strategy.exchange
    exid = exchange.exid

    if exchange.status != 'ok':
        log.error('Exchange {0} status error'.format(exid))
        return

    # Get instructions
    allocations_new = [strategy.get_allocations()]
    allocations_old = [strategy.get_allocations(n=strategy.get_offset())]

    # Select codes for strategy assets
    codes_new = sum([list(l[list(l.keys())[0]].keys()) for l in allocations_new], [])
    codes_old = sum([list(l[list(l.keys())[0]].keys()) for l in allocations_old], [])
    codes = list(set(codes_old + codes_new))

    # Add margin stablecoins
    stablecoins = exchange.get_stablecoins()
    codes_margined_stable = list(set(Market.objects.filter(exchange=exchange,
                                                           margined__code__in=stablecoins
                                                           ).exclude(margined__code=exchange.dollar_currency
                                                                     ).values_list('margined__code', flat=True)))
    codes += codes_margined_stable

    if codes:

        log.info('Strategy {1} has {0} codes'.format(len(codes), strategy.id), codes=codes)
        accounts = get_accounts()

        if accounts:

            accounts = accounts.filter(updated=False)
            if accounts:

                # Create empty dictionaries
                balances, positions, routes, markets, synthetic_cash, prices = [dict() for i in range(6)]

                log.info('Create asyncio loops')

                # Run asyncio loops
                loop = asyncio.get_event_loop()
                # loop.set_debug(True)
                gp = asyncio.wait([main(loop)])

                log.info('Establish WS connection')
                loop.run_until_complete(gp)

            else:
                log.info("Strategy {0}'s accounts are updated".format(strategy.id))
                return

        else:
            log.warning('Strategy {0} has no valid account'.format(strategy.id))
            return

    else:
        log.warning('Strategy {0} has no code to monitor'.format(strategy.id))
        return


@shared_task(name='Update account', base=BaseTaskWithRetry)
def run():
    # Create a list of chains
    strategies = Strategy.objects.filter(production=True)
    chains = [chain(
        update_accounts.s(strategy.id).set(queue='slow')
    ) for strategy in strategies]

    result = group(*chains).delay()
