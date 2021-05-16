from __future__ import absolute_import, unicode_literals

import asyncio
import time
import traceback
from itertools import accumulate
from pprint import pprint

import ccxt
import ccxtpro
import numpy as np
import pandas as pd
import structlog
from celery import chain, group, shared_task, Task
from django.core.exceptions import ObjectDoesNotExist

from capital.error import *
from capital.methods import *
from marketsdata.models import Market, Currency, Exchange
from strategy.models import Strategy
from trading import methods
from trading.models import Account, Order, Fund, Position

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
                    responses = client.fetchOpenOrders(instrument_id)
                    account.exchange.update_credit('fetchOpenOrders', default_type)

                    if responses:

                        for response in responses:
                            methods.order_create_update(account, response)

        # fetch others exchanges orders
        ###############################
        else:

            # Check credit and fetch open orders
            if account.exchange.has_credit():
                responses = client.fetchOpenOrders()
                account.exchange.update_credit('fetchAllOpenOrders', default_type)

            for response in responses:
                methods.order_create_update(account, response)

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
            methods.order_create_update(account, response)

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
def place_order(account_id, object_id):
    log.info('Placing order {0}'.format(object_id))
    account = Account.objects.get(id=account_id)

    try:
        # Select object by it's primary key
        order = Order.objects.get(id=object_id)

    except ObjectDoesNotExist:
        raise Exception('Placing order {0} failed, cannot select object'.format(object_id))

    else:

        # Get client
        client = account.exchange.get_ccxt_client(account)

        # Set default_type is necessary
        if order.market.default_type:
            client.options['defaultType'] = order.market.default_type

        args = dict(
            symbol=order.market.symbol,
            type=order.type,
            side=order.side,
            amount=float(order.amount),
            params=dict(clientOrderId=object_id)  # Set primary key as clientOrderId
        )

        # Set reduce-only
        if 'close' in order.action:
            args['params'] = args['params']['reduceOnly'] = True

        print('\nargs\n')
        pprint(args)

        # Set limit price
        if account.limit_order:
            args['price'] = order.price

        # Check API credit
        if account.exchange.has_credit():

            # Place order
            try:
                response = client.create_order(**args)

            except ccxt.InsufficientFunds:
                log.error('Placing order {0} failed, margin insufficient'.format(object_id))

            else:
                account.exchange.update_credit('create_order', order.market.default_type)

                if response['id']:

                    # Check if it's our order
                    if float(response['clientOrderId']) == object_id:

                        log.bind(clientOrderId=object_id)
                        log.info('Placing order {0} done'.format(object_id))

                        # Select Binance datetime
                        if account.exchange.exid == 'binance':
                            if order.market.type == 'spot':
                                response['timestamp'] = response['info']['transactTime']
                            else:
                                response['timestamp'] = response['info']['updateTime']

                        # Update order object
                        order.orderid = response['id']
                        order.price_average = response['average']
                        order.fee = response['fee']
                        order.cost = response['cost']
                        order.filled = response['filled']
                        order.price = response['price']
                        order.status = response['status']
                        order.timestamp = response['timestamp']
                        order.datetime = convert_timestamp_to_datetime(response['timestamp'] / 1000,
                                                                       datetime_directive_binance_order)
                        order.response = response
                        order.save()

                        return order.id

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


# Fetch balance and create fund object
@shared_task(base=BaseTaskWithRetry)
def create_fund(account_id, wallet=None):
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    log.bind(account=account.name)

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
    def create_fund_object(total, free, used, derivative):

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

    # If a wallet is specified select the latest object
    # and update a specific default_type then return
    if wallet:

        # Default is the label assigned to JSON key when default_type = None
        if wallet != 'default':
            client.options['defaultType'] = wallet

            if account.exchange.has_credit(wallet):
                response = client.fetchBalance()
                account.exchange.update_credit('fetchBalance', wallet)

        else:
            if account.exchange.has_credit():
                response = client.fetchBalance()
                account.exchange.update_credit('fetchBalance')

        t, u, f = create_dict(response)

        latest = account.get_fund_latest()
        latest.total[wallet] = t
        latest.used[wallet] = u
        latest.free[wallet] = f

        latest.derivative[wallet] = get_derivative(response, wallet)
        latest.save()
        log.info('Latest fund object has been updated for wallet {0}'.format(wallet))
        return

    log.info('Create fund object')
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

                derivative[default_type] = get_derivative(response, default_type)

        create_fund_object(total, free, used, derivative)

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

            create_fund_object(total, free, used, derivative)


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
def update_order_id(account_id, order_id):
    account = Account.objects.get(id=account_id)
    log.info('Update order {0}'.format(order_id))

    try:
        # First select object
        order = Order.objects.get(account=account, orderid=order_id)

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
                    params = dict(instrument_id=order.market.info['instrument_id'], order_id=order_id)

                # Check credit and insert order
                if account.exchange.has_credit():
                    response = client.fetchOrder(id=order_id, symbol=order.market.symbol)  # , params=params)
                    account.exchange.update_credit('fetchOrder', order.market.default_type)

                    # Return order.orderid if new trade occurred
                    dic = methods.order_create_update(account, response)
                    return dic

            else:
                raise Exception('Methode fetchOrder is not supported by {0}'.format(account.exchange.name))
        else:
            log.warning('Order is not open but {0}'.format(order.status))

        return False


# Create, update or delete objects
@shared_task(name='Trading_____Update position', base=BaseTaskWithRetry)
def update_positions(account_id, orderids=None):
    account = Account.objects.get(id=account_id)

    log.info('Update positions')
    client = account.exchange.get_ccxt_client(account)

    if orderids:
        # Create a list of derivative markets open orders belong to
        markets = list(set([order.market for order in Order.objects.filter(orderid__in=orderids) if
                            order.market.type == 'derivative']))

    # Create/update object of an open position
    def create_update(market, defaults):

        # create search arguments
        args = dict(exchange=account.exchange, account=account, market=market)

        try:
            Position.objects.get(**args)

        except Position.DoesNotExist:
            args.update(defaults)
            Position.objects.create(**args)
            log.info('Update positions {0}'.format(market.symbol), action='create')

        else:
            Position.objects.update_or_create(**args, defaults=defaults)
            log.info('Update positions {0}'.format(market.symbol), action='update')

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

                    size = float(position['notionalValue'])
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

    log.info('Update positions done')


# Transfer fund between wallets
@shared_task(base=BaseTaskWithRetry)
def transfer(account_id, code, amount, from_wallet, to_wallet):

    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    # Check credit and transfer fund
    if account.exchange.has_credit():
        try:
            log.info('Transfer {0} {1} from {2} to {3}'.format(amount, code, from_wallet, to_wallet))
            client.transfer(code, amount, from_wallet, to_wallet)

        except Exception as e:

            account.exchange.update_credit('transfer', 'spot')
            log.error('Unable to transfer fund')
            # print('exception', str(e))
            traceback.print_exc()
            raise e

        else:
            account.exchange.update_credit('transfer', 'spot')
            return True


global accounts, codes


@shared_task()
def trade(exid, strategy_id):
    # Select exchange and run checks
    exchange = Exchange.objects.get(exid=exid)
    strategy = Strategy.objects.get(id=strategy_id)

    log.bind(exid=exid, strategy=strategy.name)

    if exchange.status != 'ok':
        log.error('Exchange {0} status error'.format(exid))
        return

    # Create a dataframes with markets
    def create_df_markets():

        # Select markets to build dataframe
        markets = Market.objects.filter(exchange=exchange, base__code__in=codes, excluded=False, active=True)

        df_markets = pd.DataFrame()

        # Loop through codes
        for code in codes:

            # Loop through markets
            for market in markets.filter(base__code=code):
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
                        funding = 0

                    # Construct dataframe and normalize rows
                    df = pd.DataFrame(np.nan, index=indexes, columns=cols)

                    # Fill funding rate and latest price
                    df['funding', 'rate'] = funding
                    df['price', 'close'] = market.get_candle_price_last()

                    # Fill order status and route type with nan
                    df['route', 'type'] = np.nan
                    df['order', 'status'] = np.nan
                    df['order', 'id'] = np.nan

                    df_markets = pd.concat([df, df_markets], axis=0)  # .groupby(level=[0, 1, 2, 3, 4, 5, 6]).mean()

        # Sort indexes and columns
        df_markets.sort_index(axis=0, inplace=True)
        df_markets.sort_index(axis=1, inplace=True)

        print(df_markets.to_string())
        return df_markets

    # Create a dataframe with available routes
    def create_routes(id, df_account, df_positions):

        log.info('Create routes dataframe')

        # Buy
        #####

        # Create a list of currency to long and to short
        codes_short = list(df_account[df_account[('target', 'quantity')] < 0].index.get_level_values('code').unique())
        codes_long = list(df_account[df_account[('target', 'quantity')] > 0].index.get_level_values('code').unique())

        # Create a list of currencies to buy : open long (derivative or spot) and close short (derivative)
        codes_open_long = list(df_account[(df_account[('delta', 'value')] < 0) & (
                df_account[('target', 'quantity')] > 0)].index.get_level_values('code').unique())
        codes_close_short = list(df_account[(df_account[('delta', 'value')] < 0) & (
                df_account[('position', 'quantity')] < 0)].index.get_level_values('code').unique())

        # Sell
        ######

        # Create a list of currencies to sell : close long (derivative) and open short (derivative)
        codes_close_long = list(df_account[(df_account[('delta', 'value')] > 0) & (
                df_account[('position', 'quantity')] > 0)].index.get_level_values('code').unique())
        codes_open_short = list(df_account[(df_account[('delta', 'value')] > 0) & (
                df_account[('target', 'quantity')] < 0)].index.get_level_values('code').unique())

        # Create a list of currencies we should sell in spot markets
        codes_sell_spot = list(df_account[(df_account[('delta', 'value')] > 0) & (
                df_account[('wallet', 'total_quantity')] > 0)].index.get_level_values('code').unique())

        # Market candidates
        ###################

        # Create a list of spot markets where a currency could be sold as base
        mk_spot_sell_base = [dic_markets[id][dic_markets[id].index.isin([sell], level='base')
                                             & dic_markets[id].index.isin(['spot'], level='type')].index.tolist()
                             for sell in codes_sell_spot]

        # Create a list of spot markets where a currency could be sold as quote
        mk_spot_sell_quote = [dic_markets[id][dic_markets[id].index.isin([sell], level='quote')
                                              & dic_markets[id].index.isin(['spot'], level='type')].index.tolist()
                              for sell in codes_sell_spot]

        # Create a list of markets to open a short
        mk_deri_open_short = [dic_markets[id][dic_markets[id].index.isin([short], level='base')
                                              & dic_markets[id].index.isin(['derivative'], level='type')].index.tolist()
                              for short in codes_open_short]

        # Create a list of markets to buy spot or and open a long
        mk_spot_buy = [dic_markets[id][dic_markets[id].index.isin([buy], level='base')
                                       & dic_markets[id].index.isin(['spot'], level='type')].index.tolist()
                       for buy in codes_open_long]

        mk_deri_open_long = [dic_markets[id][dic_markets[id].index.isin([buy], level='base')
                                             & dic_markets[id].index.isin(['derivative'], level='type')].index.tolist()
                             for buy in codes_open_long]

        # Create a list of candidates for a hedge
        mk_deri_hedge = [dic_markets[id][dic_markets[id].index.isin([sell], level='base')
                                         & dic_markets[id].index.isin(['derivative'], level='type')].index.tolist()
                         for sell in codes_sell_spot]

        # Market with a position
        ########################

        # Create a list of markets with an open position
        if not df_positions.empty:
            mk_opened_long = [i for i, p in df_positions.iterrows() if p['side'] == 'buy']
            mk_opened_short = [i for i, p in df_positions.iterrows() if p['side'] == 'sell']
        else:
            mk_opened_long = []
            mk_opened_short = []

        # Unnest lists and remove duplicate candidates
        mk_spot_buy = list(set(sum(mk_spot_buy, [])))
        mk_spot_sell_base = list(set(sum(mk_spot_sell_base, [])))
        mk_spot_sell_quote = list(set(sum(mk_spot_sell_quote, [])))
        mk_deri_open_short = list(set(sum(mk_deri_open_short, [])))
        mk_deri_open_long = list(set(sum(mk_deri_open_long, [])))
        mk_deri_hedge = list(set(sum(mk_deri_hedge, [])))

        print('currency to long', codes_long)
        print('currency to short', codes_short)
        print('currency to buy (open long):', codes_open_long)
        print('currency to buy (close short):', codes_close_short)
        print('currency to sell (close long):', codes_close_long)
        print('currency to sell (open short):', codes_open_short)
        print('currency to sell (spot):', codes_sell_spot)

        for i in mk_spot_buy:
            print('market buy spot:', i[3])

        for i in mk_spot_sell_base:
            print('market sell spot as base:', i[3])

        for i in mk_spot_sell_quote:
            print('market sell spot as quote:', i[3])

        for i in mk_deri_open_short:
            print('market open short:', i[3])

        for i in mk_deri_open_long:
            print('market open long:', i[3])

        for i in mk_opened_long:
            print('market with position long:', i[3])

        for i in mk_opened_short:
            print('market with position short:', i[3])

        for i in mk_deri_hedge:
            print('market for a hedge:', i[3])

        routes = []

        # [0] : base
        # [1] : quote
        # [2] : default_type
        # [3] : symbol
        # [4] : type
        # [5] : derivative
        # [6] : margined

        # List of routes
        ################

        # [0] : market (or code) source
        # [1] : market destination
        # [2] : route type (normal, yield, margin or default)
        # [3] : first action
        # [4] : second action

        # Close long position
        #####################

        for long in mk_opened_long:

            # Position should be reduced
            if long[0] in codes_close_long:

                # Direct
                ########

                # Margined currency is a desired currency
                if long[6] in codes_open_long:
                    routes.append([long, None, 'normal', 'close_long', None])

                # Margined currency is a desired currency
                # Allow to keep short open and continue earning funding rate if longs pay shorts
                if long[6] in codes_close_short:
                    routes.append([long, None, 'yield', 'close_long', None])

                # Deri -> spot
                ##############

                # Loop through spot markets where a currency could be bought on spot
                for spot in mk_spot_buy:

                    # Margined currency could buy a desired currency on spot
                    if long[6] == spot[1]:
                        routes.append([long, spot, 'normal', 'close_long', 'buy'])

                # Deri -> deri
                ##############

                # Loop through derivative markets for open buy
                for deri in mk_deri_open_long:

                    # Margin currency in common
                    if long[6] == deri[6]:

                        # Close long and open long on a different market
                        routes.append([long, deri, 'normal', 'close_long', 'open_long'])

                # Loop through derivative markets for open short
                for deri in mk_deri_open_short:

                    # Margin currency in common
                    if long[1] == deri[6]:

                        # Close long and open short on the same market
                        if long[2] == deri[2] and long[3] == deri[3]:
                            routes.append([long, None, 'normal', 'close_long', 'open_short'])

                        else:
                            routes.append([long, deri, 'normal', 'close_long', 'open_short'])

                # In case no route is found
                routes.append([long, None, 'default', 'close_long', None])

        # Close short position
        ######################

        for short in mk_opened_short:

            # Position should be reduced
            if short[0] in codes_close_short:

                # Direct
                ########

                # Margined currency is a desired currency
                if short[6] in codes_open_long:
                    routes.append([short, None, 'normal', 'close_short', None])

                # Margined currency is a desired currency
                # Allow to keep short open and continue earning funding rate if longs pay shorts
                if short[6] in codes_close_short:
                    routes.append([short, None, 'yield', 'close_short', None])

                # Deri -> spot
                ##############

                # Loop through spot markets where a currency could be bought on spot
                for spot in mk_spot_buy:

                    # Margined currency could buy a desired currency on spot
                    if short[6] == spot[1]:
                        routes.append([short, spot, 'normal', 'close_short', 'buy'])

                # Deri -> deri
                ##############

                # Loop through derivative markets for open a buy
                for deri in mk_deri_open_long:

                    # Margin currency in common
                    if short[6] == deri[6]:

                        # Close short and open long on the same market
                        if short[2] == deri[2] and short[3] == deri[3]:
                            routes.append([short, None, 'normal', 'close_short', 'open_long'])

                        # Close short and open long on different markets
                        else:
                            routes.append([short, deri, 'normal', 'close_short', 'open_long'])

                # Loop through derivative markets for open a short
                for deri in mk_deri_open_short:

                    # Margin currency in common
                    if short[1] == deri[6]:

                        # Open short on a different market
                        routes.append([short, deri, 'normal', 'close_short', 'open_short'])

                # In case no route is found
                routes.append([short, None, 'default', 'close_short', None])

        # Use a currency (that should be sold) as margin
        ################################################

        for code in codes_sell_spot:

            # Loop through markets where a long could be opened
            for mk_deri_long in mk_deri_open_long:

                # Code is margin currency
                if code == mk_deri_long[6]:

                    # Code is quote currency (USD-margined)
                    if code != mk_deri_long[0]:
                        routes.append([code, mk_deri_long, 'margin', 'open_long', None])

            # Loop through markets where a short could be opened
            for mk_deri_short in list(set(mk_deri_open_short + mk_deri_hedge)):

                # Code is margin currency
                if code == mk_deri_short[6]:

                    # Code is base currency (COIN-margined)
                    if code == mk_deri_short[0]:
                        routes.append([code, mk_deri_short, 'margin', 'hedge', None])

                    # Code is quote currency (USD-margined)
                    else:
                        routes.append([code, mk_deri_short, 'margin', 'open_short', None])

        # Sell currency as quote (spot)
        ###############################

        for spot_sell in mk_spot_sell_quote:

            # Base currency is a desired currency
            if spot_sell[0] in codes_open_long:
                routes.append([spot_sell, None, 'normal', 'sell_quote', None])

            # Base currency is a desired currency
            # Allow to keep short open and continue earning funding rate if longs pay shorts
            if spot_sell[0] in codes_close_short:
                routes.append([spot_sell, None, 'yield', 'sell_quote', None])

        # Sell currency as base (spot)
        ##############################

        for spot_sell in mk_spot_sell_base:

            # Quote currency is a desired currency
            if spot_sell[1] in codes_open_long:
                routes.append([spot_sell, None, 'normal', 'sell_base', None])

            # Quote currency is a desired currency
            # Allow to keep short open and continue earning funding rate if longs pay shorts
            if spot_sell[1] in codes_close_short:
                routes.append([spot_sell, None, 'yield', 'sell_base', None])

            # Loop through markets where a currency could be bought on spot
            for spot_buy in mk_spot_buy:

                # If quote is an intermediary currency
                if spot_sell[1] == spot_buy[1]:
                    routes.append([spot_sell, spot_buy, 'normal', 'sell_base', 'buy'])

            # Spot -> deri
            ##############

            # Loop through markets where a long could be opened
            for deri_buy in mk_deri_open_long:

                # Code could be margined to open long
                if spot_sell[1] == deri_buy[6]:
                    routes.append([spot_sell, deri_buy, 'normal', 'sell_base', 'open_long'])

            # Loop through markets where a short could be opened
            for short in mk_deri_open_short:

                # Code could be margined to open short
                if spot_sell[1] == short[6]:
                    routes.append([spot_sell, short, 'normal', 'sell_base', 'open_short'])

        # Create dataframe
        df_routes = pd.DataFrame()
        names = ['base_s', 'quote_s', 'symbol_s', 'wallet_s', 'base_d', 'quote_d', 'symbol_d', 'wallet_d']

        # Insert routes into dataframe
        for r in routes:

            route_type = r[2]
            action_first = r[3]
            action_second = r[4]

            # If 1st element is a market or a currency
            if route_type == 'margin':
                code_src = r[0]
                wallet_src = 'spot'
                quote_src, symbol_src = [np.nan for i in range(2)]
            else:
                code_src = r[0][0]
                quote_src = r[0][1]
                symbol_src = r[0][3]
                wallet_src = r[0][2]

            # If 2nd element is a destination market
            if r[1] is not None:
                code_dst = r[1][0]
                quote_dst = r[1][1]
                symbol_dst = r[1][3]
                wallet_dst = r[1][2]
            else:
                code_dst, quote_dst, symbol_dst, wallet_dst = [np.nan for i in range(4)]

            # Construct an index
            index = [code_src, quote_src, symbol_src, wallet_src, code_dst, quote_dst, symbol_dst, wallet_dst]

            indexes = pd.MultiIndex.from_tuples([index], names=names)
            columns = pd.MultiIndex.from_product([['route'], ['type']], names=['level_1', 'level_2'])

            # Create a dataframe with route type
            df = pd.DataFrame([[route_type]], index=indexes, columns=columns)

            # Add actions
            df.loc[indexes, ('action', 'first')] = action_first
            df.loc[indexes, ('action', 'second')] = action_second

            # Finally concatenate dataframe
            df_routes = pd.concat([df, df_routes], axis=0)

        df_routes.sort_index(axis=0, inplace=True)
        df_routes.sort_index(axis=1, inplace=True)

        log.info('Create routes dataframe done')
        return df_routes

        # Select row corresponding to market an order is placed and update order details

    # Update df_markets with best bid and ask at every iteration
    def update_markets_bid_ask(id, wallet, symbol, code, quote, bids, asks):

        dic_markets[id].loc[(code, quote, wallet, symbol), ('price', 'bid')] = bids[0][0]
        dic_markets[id].loc[(code, quote, wallet, symbol), ('price', 'ask')] = asks[0][0]
        dic_markets[id].sort_index(axis=0, inplace=True)  # Prevent past lexsort depth PerformanceWarning

    # Update df_routes with amount and costs at every iteration
    def update_routes_cost(id, wallet, symbol, base, quote, bids, asks):

        # Sort dataframe to avoid warning when df.index.is_lexsorted() == False
        dic_routes[id].sort_index(axis=0, inplace=True)

        # Select route indexes where market is a source (or a destination) of an existing route
        indexes_src = dic_routes[id].loc[(dic_routes[id].index.get_level_values(2) == symbol) & (
                dic_routes[id].index.get_level_values(3) == wallet)].index
        indexes_dst = dic_routes[id].loc[(dic_routes[id].index.get_level_values(6) == symbol) & (
                dic_routes[id].index.get_level_values(7) == wallet)].index

        # Update df_routes dataframe
        def update(side, value, code, depth, funding=None):

            # Convert trade value from US. Dollar to currency amount
            spot = dic_accounts[id].loc[(code, wallet), 'price'][0]
            amount = value / spot

            # Get best price and distance % for the desired amount
            best_price, distance = calculate_distance(depth, amount)
            spread = calculate_spread(bids, asks)
            cost = distance + spread

            # Select delta quantity
            delta_qty = dic_accounts[id].loc[(code, wallet), ('delta', 'quantity')]

            dic_routes[id].loc[route, (side, 'amount')] = amount
            dic_routes[id].loc[route, (side, 'distance')] = distance
            # dic_routes[id].loc[route, (side, 'best price')] = best_price
            dic_routes[id].loc[route, (side, 'spread')] = spread
            dic_routes[id].loc[route, (side, 'cost')] = cost
            dic_routes[id].loc[route, (side, 'quantity %')] = abs(amount / delta_qty)

            if funding is not None:
                dic_routes[id].loc[route, (side, 'funding')] = funding

        # Return trade value and depth book
        def get_value_n_depth(route, route_type, action_1, action_2):

            base_s, quote_s, symbol_s, wallet_s, base_d, quote_d, symbol_d, wallet_d = [route[i] for i in range(8)]

            # Route with type margin have one of these three instructions
            if route_type == 'margin':

                if action_1 == 'hedge':
                    # Get value to hedge and multiply by 2 (currency isn't sold but margined)
                    delta = dic_accounts[id].loc[(base_d, wallet_d), ('delta', 'value')]  # delta is positive
                    trade_value = delta * 2
                    depth = bids

                elif action_1 == 'open_short':
                    # Get max value to use as margin and get value to short
                    expos = dic_accounts[id].loc[(base_s, wallet_s), ('exposure', 'value')]
                    delta = dic_accounts[id].loc[(base_d, wallet_d), ('delta', 'value')]  # delta is positive
                    trade_value = min(expos, delta)
                    depth = bids

                elif action_1 == 'open_long':
                    # Get max value to use as margin and get value to long
                    expos = dic_accounts[id].loc[(base_s, wallet_s), ('exposure', 'value')]
                    delta = dic_accounts[id].loc[(base_d, wallet_d), ('delta', 'value')]  # delta is negative
                    trade_value = min(expos, abs(delta))
                    depth = asks

            # Rest of the routes can have one or two instructions
            # Instruction 1 is executed on market source, instruction 2
            # is executed on market destination
            else:

                # Start with instruction 1 (release fund)
                #########################################

                if action_1 in ['sell_quote']:

                    # Get exposure value (quote) and delta value (base)
                    expos = dic_accounts[id].loc[(quote_s, wallet_s), ('exposure', 'value')]
                    delta = dic_accounts[id].loc[(base_s, wallet_s), ('delta', 'value')]  # negative
                    trade_value = min(abs(delta), expos)
                    depth = asks

                elif action_1 in ['close_short']:

                    # Get delta value (base)
                    delta = dic_accounts[id].loc[(base_s, wallet_s), ('delta', 'value')]  # negative
                    trade_value = abs(delta)
                    depth = asks

                elif action_1 in ['sell_base', 'close_long']:

                    # Get exposure value and delta
                    expos = dic_accounts[id].loc[(base_s, wallet_s), ('exposure', 'value')]
                    delta = dic_accounts[id].loc[(base_s, wallet_s), ('delta', 'value')]
                    trade_value = min(delta, expos)
                    depth = bids

                # Continue with instruction 2 (allocate fund)
                #############################################

                if not pd.isna(action_2):

                    if action_2 in ['buy', 'open_long']:
                        delta = dic_accounts[id].loc[(base_d, wallet_d), ('delta', 'value')]
                        desired_value = abs(delta)  # delta is negative
                        depth = asks

                    elif action_2 in ['open_short']:
                        delta = dic_accounts[id].loc[(base_d, wallet_d), ('delta', 'value')]
                        desired_value = delta
                        depth = bids

                    # Determine the maximum value to trade through this route
                    trade_value = min(trade_value, desired_value)

            return trade_value, depth

        # Iterate through routes for which market is among source markets
        for route in indexes_src:

            # select route type and actions
            route_type = dic_routes[id].loc[route, ('route', 'type')]
            action_1 = dic_routes[id].loc[route, ('action', 'first')]
            action_2 = dic_routes[id].loc[route, ('action', 'second')]

            # Get trade value and depth book and update
            trade_value, depth = get_value_n_depth(route, route_type, action_1, action_2)
            update('source', trade_value, base, depth)

        # Iterate through routes for which market is among destination markets
        for route in indexes_dst:

            # select route_type and actions
            route_type = dic_routes[id].loc[route, ('route', 'type')]
            action_1 = dic_routes[id].loc[route, ('action', 'first')]
            action_2 = dic_routes[id].loc[route, ('action', 'second')]

            # Get trade value and depth book
            trade_value, depth = get_value_n_depth(route, route_type, action_1, action_2)

            # In case nothing could be traded in source market then drop route
            if pd.isna(trade_value):
                log.warning('Drop route')
                dic_routes[id].drop([route], inplace=True)
                continue

            # Get latest funding rate and update dataframe with route cost
            funding = dic_markets[id].loc[(base, quote, wallet, symbol), ('funding', 'rate')][0]
            update('destination', trade_value, base, depth, funding)

        # Sum source and destination routes costs
        #########################################

        # Return route cost for market source or destination
        def get_cost(route, side):
            if side in route.index.get_level_values('level_1'):
                if 'cost' in route[side].index:
                    if not pd.isna(route[side]['cost']):

                        cost = route[side]['cost']
                        route_type = route['route']['type']
                        action_1 = route['action']['first']
                        action_2 = route['action']['second']

                        # Add funding rate
                        if side == 'destination':
                            if 'funding' in route['destination'].index:
                                funding = float(route['destination']['funding']) * 3

                                # Test is route action requires to negate funding rate
                                if (route_type == 'margin' and action_1 in ['hedge', 'open_short']) \
                                        or (action_2 == 'open_short'):

                                    # Negate funding rate to favor route
                                    funding = - funding

                                cost += funding

                            else:
                                print('\nFunding column not found\n')
                                cost = np.nan

                        return cost

        # Create a new column with total route cost
        if 'source' in dic_routes[id].droplevel('level_2', axis=1).columns:

            # Iterate through our routes
            for index, route in dic_routes[id].iterrows():

                # If route type is margin get cost from destination market
                if route['route']['type'] == 'margin':
                    cost = get_cost(route, 'destination')

                else:

                    # Route has only one instruction
                    if pd.isna(route['action']['second']):
                        cost = get_cost(route, 'source')

                    # Route has only two instructions
                    else:
                        cost_src = get_cost(route, 'source')
                        cost_dst = get_cost(route, 'destination')

                        # Sum source and destination costs
                        if (cost_src and cost_dst) is not None:
                            cost = cost_src + cost_dst
                        else:
                            cost = np.nan

                # Create a new column with total route cost
                dic_routes[id].loc[index, ('route', 'cost')] = cost

    # Update df_markets with order status after an order is placed
    def update_markets_df(id, client_order_id):

        order = Order.objects.get(id=client_order_id)
        log.info('Update df_markets', order=order.id)

        # Construct index
        idx = (order.market.base.code,
               order.market.quote.code,
               'default' if order.market.default_type is None else order.market.default_type,
               order.market.symbol,
               order.market.type,
               order.market.derivative,
               order.market.margined.code
               )

        # Add/update order informations
        dic_markets[id].loc[idx, ('order', 'id')] = order.id
        dic_markets[id].loc[idx, ('order', 'type')] = order.route_type
        dic_markets[id].loc[idx, ('order', 'amount')] = order.amount
        dic_markets[id].loc[idx, ('order', 'status')] = order.status
        dic_markets[id].loc[idx, ('order', 'filled')] = order.filled

    # Update the latest fund object after a trade is executed
    def update_fund_object(account, order_ids):

        log.info('Update fund object')
        print(Order.objects.filter(id__in=order_ids))

        # Select wallet of markets where trades occurred
        wallets = list(set([order.market.default_type for order in Order.objects.filter(orderid__in=order_ids)]))

        print(order_ids, wallets)

        if wallets:
            for wallet in wallets:
                create_fund.run(account.id, wallet=wallet)
        else:
            create_fund.run(account.id, wallet='default')

        log.info('Update fund object done')

    # Update open orders and return a list of orders with new trades
    def update_orders(account):

        # Fetch open orders and update order objects
        open_orders = account.get_pending_order_ids()

        if open_orders:

            tasks = [update_order_id.si(account.id, orderid) for orderid in open_orders]  # create a list of task
            result = group(*tasks).apply_async(queue='slow')  # execute tasks in parallel

            while not result.ready():
                time.sleep(0.5)

            # Update complete
            if result.successful():

                # Return a list of ids for orders with new trade
                res = result.get(disable_sync_subtasks=False)

                log.info('Open orders updated')
                return res

            else:
                log.error('Open orders update failed')
        else:
            log.info('Open order not found')

    # return cumulative orderbook
    def cumulative_book(ob):

        asks = ob['asks']
        bids = ob['bids']
        asks_p = [a[0] for a in asks]
        bids_p = [a[0] for a in bids]
        cum_a = list(accumulate([a[1] for a in asks]))
        cum_b = list(accumulate([a[1] for a in bids]))
        return [[bids_p[i], cum_b[i]] for i, a in enumerate(bids)], [[asks_p[i], cum_a[i]] for i, a in enumerate(asks)]

    # Calculate the best price available for the desired quantity and it's distance to best bid/ask
    def calculate_distance(depth, amount):

        # Iterate through depth until desired amount is available
        for i, b in enumerate(depth):

            if b[1] > amount:
                if i == 0:
                    return depth[0][0], 0
                else:
                    depth = depth[:i]  # select the first n elements needed
                    break

        # select prices and sum total quantity needed
        prices = [p[0] for p in depth]
        qty = sum([q[1] for q in depth])

        # weight each element and multiply prices by weights and sum
        weights = [q[1] / qty for q in depth]
        best_price = sum([a * b for a, b in zip(prices, weights)])

        # Calculate distance in % to the best bid or to the best ask
        distance = abs(100 * (best_price / depth[0][0] - 1))

        return best_price, distance

    # Calculate bid-ask spread
    def calculate_spread(bids, asks):

        spread = asks[0][0] - bids[0][0]
        spread_pct = spread / asks[0][0]

        return spread_pct * 100

    # Place an order to the best route for every source currency
    # and update df_markets when an order is placed
    def trade(account):

        id = account.id
        log.bind(account=account.name)

        # Wait route cost
        #################

        if not any(dic_routes[id].columns.isin([('route', 'cost')])):
            return

        if any(pd.isna(dic_routes[id]['route']['cost'].array)):
            return

        # Execute trade logic
        #####################

        log.info('Trading...')

        # Sort routes by source currency and cost, then reorder columns
        dic_routes[id] = dic_routes[id].sort_values([('route', 'cost')], ascending=True)
        dic_routes[id] = dic_routes[id].sort_index(level=[0], ascending=[True])
        dic_routes[id] = dic_routes[id].sort_index(axis=1)

        print('\n')
        print(dic_accounts[account.id].to_string())
        print('\n')
        print(dic_routes[account.id].to_string())
        print('\n')
        print(dic_markets[account.id].to_string())
        print('\n')

        # Create a list of base currencies from source markets (.i.e ['ETH', 'BTC'])
        codes = list(set(dic_routes[id].index.get_level_values(0)))

        for code in codes:

            log.bind(code=code)

            # Select markets which use this route and check orders status
            markets = dic_markets[id].xs(code, level='base', axis=0)
            status = list(markets['order']['status'])

            # Abort trade execution if an order is open for this code
            if 'open' in status:
                log.info('Order pending')
                continue

            # Abort trade execution if an order is closed for this code
            elif 'close' in status:
                log.info('Order filled')
                continue

            # Select best route
            else:

                # Loop through df_routes and execute trade logic
                for index, route in dic_routes[id].xs(code, level='base_s', axis=0).iterrows():

                    # Select symbol, wallet and market to trade
                    route_type = route['route']['type']
                    action_first = route['action']['first']
                    symbol = index[0]
                    wallet = index[1]
                    market = Market.objects.get(exchange=exchange, symbol=symbol, default_type=wallet)

                    # Set default side to source
                    side = 'source'

                    # Loop to the next route if there is no fund available to hedge
                    if route_type == 'hedge':
                        free = dic_accounts[id].loc[(code, wallet), ('wallet', 'free_quantity')]
                        if pd.isna(free):
                            log.warning('Trading impossible due to lack of resource')
                            continue

                    # Transfer fund
                    elif action_first == 'transfer_base':

                        code = index[0]
                        amount = ''
                        res = transfer(account.id, )
                        if res:
                            # Change side to destination after transfer is complete
                            side = 'destination'
                        else:
                            continue

                    # Create object and place order
                    ###############################

                    log.info('Trading {0} market'.format(side))

                    # Select amount and action
                    amount = route[side]['amount']
                    action = route[side]['source']

                    # Convert amount to contract quantity
                    if market.type == 'derivative':
                        amount = methods.amount_to_contract(market, amount)

                    # Create an order object and return it's primary key
                    object_id = account.create_order(market, route_type, action, amount)

                    # If object is created
                    if object_id:

                        # Place an order, update object and return a client order id
                        client_order_id = place_order.run(account.id, object_id)

                        # if order is placed
                        if client_order_id:

                            # Update markets_df with order status
                            update_markets_df(id, client_order_id)
                            log.info('df_markets updated, break index loop for {0}'.format(code))
                            break

                        else:
                            log.error('Trading failed no response from exchange')

                    else:
                        # Break index loop
                        log.warning('Trade failed, break index loop for {0}'.format(code))
                        break

        log.info('Trade complete')

    # Asyncio loops
    ###############

    # Receive websocket streams of book depth
    async def watch_book(client, market, i, j):

        wallet = market.default_type
        symbol = market.symbol
        base = market.base.code
        quote = market.quote.code
        loop = 0

        while True:
            try:
                ob = await client.watch_order_book(symbol)  # , limit=account.exchange.orderbook_limit)
                if ob:
                    loop += 1
                    if loop == 10:
                        pass
                        # break

                    # Capture current depth
                    bids, asks = cumulative_book(ob)

                    # Update markets and routes dataframes
                    ######################################

                    for account in accounts:
                        # Update best bid/ask in df_markets
                        update_markets_bid_ask(account.id, wallet, symbol, base, quote, bids, asks)
                        # Update cost in df_routes for routes that use this market
                        update_routes_cost(account.id, wallet, symbol, base, quote, bids, asks)

                    if i == j == 0:

                        # Execute trades logic
                        ######################

                        for account in accounts:

                            log.bind(account=account.name)

                            # Place an order to the best route for every source currency
                            # and update df_markets when an order is placed
                            trade(account)

                            # Update objects of open orders and return a list
                            # of ids when a new trade is detected
                            order_ids = update_orders(account)

                            if order_ids:
                                print('\norder_ids', order_ids, '\n')

                                log.info('Trades detected')
                                # Update df_markets
                                [update_markets_df(account.id, order_id) for order_id in order_ids]

                                # Update df_positions if a trade occurred on a derivative market
                                update_positions.run(account.id, order_ids)
                                dic_positions[account.id] = account.create_df_positions()

                                # Update the latest fund object and df_account
                                update_fund_object(account, order_ids)
                                dic_accounts[account.id] = account.create_df_account()

                                # Update df_routes
                                dic_routes[account.id] = create_routes(account.id,
                                                                       dic_accounts[account.id],
                                                                       dic_positions[account.id]
                                                                       )

                else:
                    print('wait')

                await client.sleep(5000)

            except Exception as e:
                # print('exception', str(e))
                traceback.print_exc()
                raise e  # uncomment to break all loops in case of an error in any one of them
                # break  # you can break just this one loop if it fails

    # Configure websocket client for wallet
    async def wallet_loop(loop, i, wallet):

        client = getattr(ccxtpro, exchange.exid)({'enableRateLimit': True, 'asyncio_loop': loop, })

        if exchange.default_types:
            client.options['defaultType'] = wallet

        # Filter markets to monitor
        markets = Market.objects.filter(exchange=exchange, default_type=wallet,
                                        base__code__in=codes, excluded=False, active=True)

        log.info('Found {0} markets'.format(len(markets)), wallet=wallet)

        ws_loops = [watch_book(client, market, i, j) for j, market in enumerate(markets) if market.is_updated()]

        await asyncio.gather(*ws_loops)
        await client.close()

    # Run main asyncio loop
    async def main(loop):
        wallet_loops = [wallet_loop(loop, i, wallet) for i, wallet in enumerate(exchange.get_default_types())]
        await asyncio.gather(*wallet_loops)

    # Get codes to monitor and accounts to trade with
    #################################################

    instructions = [strategy.get_instructions()]
    codes = list(set(sum([list(l[list(l.keys())[0]].keys()) for l in instructions], [])))
    if not codes:
        log.warning('No valid strategy found')
        return

    log.info('Found {0} codes'.format(len(codes)), codes=codes)

    # Create dictionaries of dataframes
    ###################################

    dic_accounts, dic_positions, dic_routes, dic_markets = [dict() for i in range(4)]
    accounts = Account.objects.filter(strategy=strategy, exchange=exchange, trading=True)
    if not accounts:
        log.warning('No trading account found')
        return

    log.info('Found {0} accounts'.format(len(accounts)), accounts=[a.name for a in accounts])

    for account in accounts:
        # df_accounts
        create_fund.run(account.id)
        dic_accounts[account.id] = account.create_df_account()

        # df_positions
        update_positions.run(account.id)
        dic_positions[account.id] = account.create_df_positions()

        # df_markets
        dic_markets[account.id] = create_df_markets()

        # df_routes
        dic_routes[account.id] = create_routes(account.id,
                                               dic_accounts[account.id],
                                               dic_positions[account.id]
                                               )

    log.info('Dictionaries created')

    # Create and execute loop
    #########################

    loop = asyncio.get_event_loop()
    gp = asyncio.wait([main(loop)])  # , watch_direct_trades()])
    loop.run_until_complete(gp)


@shared_task(name='Trading_____Trade with accounts')
def trade_exchanges():
    exchanges = Exchange.objects.filter(exid='binance')

    tasks = [trade.s(exchange.exid, strategy.id) for exchange in exchanges
             for strategy in Strategy.objects.filter(exchange=exchange) if strategy.production]

    res = group(*tasks).apply_async(queue='slow')

    while not res.ready():
        time.sleep(0.5)

    if res.successful():
        log.info('Trading complete on {0} exchange'.format(len(tasks)))
