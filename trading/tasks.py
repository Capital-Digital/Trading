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
            args['params']['reduceOnly'] = True

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
                log.error('Invalid order', e=e, object_id=object_id)

            except ccxt.InsufficientFunds:
                log.error('Insufficient funds to place order', object_id=object_id)

            else:
                account.exchange.update_credit('create_order', order.market.default_type)

                if response['id']:

                    # Check if it's our order
                    if float(response['clientOrderId']) == object_id:

                        # Select Binance datetime
                        if account.exchange.exid == 'binance':
                            if order.market.type == 'spot':
                                response['timestamp'] = float(response['info']['transactTime'])
                            else:
                                response['timestamp'] = float(response['info']['updateTime'])

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

                        log.info('Order is placed', clientOrderId=object_id)
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

        args = dict(
            account=account,
            exchange=account.exchange,
            dt=dt
        )

        obj, created = Fund.objects.update_or_create(**args, defaults=kwargs)

        if created:
            log.info('Fund object created')
        else:
            log.info('Fund object updated')

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
                pprint(response)
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

    log.info('Fund object create/update')
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
                    size_cont = float(position['positionAmt'])
                    size = float(position['positionAmt'])
                    side = 'buy' if size > 0 else 'sell'

                    # calculate position value in USDT
                    value = size * market.contract_value * float(position['markPrice'])

                    defaults = dict(
                        size=size,
                        size_cont=size_cont,
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

                    size = float(position['notionalValue'])  # curr amount
                    size_cont = float(position['positionAmt'])  # contract amount
                    side = 'buy' if size > 0 else 'sell'

                    # calculate position value in USDT
                    value = size_cont * market.contract_value

                    defaults = dict(
                        size=size,
                        size_cont=size_cont,
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
def transfer(account_id, index, route):
    # Get quantity to transfer
    if route['instruction']['source'] == 'transfer':
        quantity = route['destination']['quantity']
        to_wallet = index[11]  # destination

    elif route['instruction']['source'] == 'transfer int':
        quantity = route['inter']['quantity']
        to_wallet = index[6]  # intermediate

    else:
        return

    # Get coin and wallets
    code = index[2]  # margin
    from_wallet = index[4]
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    # Check credit and transfer fund
    if account.exchange.has_credit():
        try:
            log.info('Transfer {0} {1} from {2} to {3}'.format(round(quantity, 4), code, from_wallet, to_wallet))
            client.transfer(code, quantity, from_wallet, to_wallet)

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

    # Measure hedge ratio and limit trade value
    def check_hedge(id, instruction, code, value):

        # Hedged currency is a currency hold in a wallet with a short position equivalent to synthetic cash.
        # The collateral needed to maintain the short position can't be allocated somewhere else like cash.
        # For this reason the hedge capacity should stay lower or equal to the cash allocation of the portfolio.

        if instruction == 'open_short':

            # Select codes with positive wallet balance
            codes = list(set((dic_accounts[id].loc[:, ('wallet', 'total_value')] > 0).index.get_level_values(0)))

            # If code that will be shorted has a balance value
            # greater than 0 consider it's potentially hedged
            if code in codes:

                # Select indexes of short positions and balance of shorted coins
                index = dic_accounts[id][dic_accounts[id].xs(('position', 'value'), axis=1) < 0].index
                balance = dic_accounts[id].loc[index.get_level_values(0), ('wallet', 'total_value')].sum()

                # Select shorted value and determine current hedge value
                short = abs(dic_accounts[id].loc[index, ('position', 'value')].sum())
                hedge = min(balance, short)

                # Select codes of stablecoins
                stablecoins = [c for c in list(set(dic_accounts[id].index.get_level_values(0)))
                               if Currency.objects.get(code=c).stable_coin]

                # Calculate cash available and target allocation
                cash_free = dic_accounts[id].loc[stablecoins, ('wallet', 'free_value')].sum()
                cash_target = dic_accounts[id].loc[stablecoins, ('target', 'value')].mean()

                # Calculate hedge ratio and capacity
                ratio = hedge / cash_target
                capacity = cash_target - hedge

                if capacity < 0:

                    # When hedge capacity is full give the opportunity to trade available cash
                    value = min(value, cash_free)
                    return value

                else:

                    # Else limit trade value to max(capacity, cash_free)
                    capacity = max(capacity, cash_free)
                    return min(value, capacity)

        else:
            return value

    # Create a dataframes with markets
    def create_df_markets():

        # Select markets to build dataframe
        markets = Market.objects.filter(exchange=exchange, base__code__in=codes, excluded=False, active=True)
        markets = markets.exclude(derivative='future')

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

                    df_markets = pd.concat([df, df_markets], axis=0)  # .groupby(level=[0, 1, 2, 3, 4, 5, 6]).mean()

        # Sort indexes and columns
        df_markets.sort_index(axis=0, inplace=True)
        df_markets.sort_index(axis=1, inplace=True)

        return df_markets

    # Create a dataframe with available routes
    def create_routes(id, df_account, df_positions):

        log.info('Create routes dataframe')

        # Currencies
        ############

        # Create a list of currencies with free balance > 0
        codes_free = list(
            df_account[(df_account[('wallet', 'free_value')] > 0)].index.get_level_values('code').unique())

        # Select stablecoins with free balance in spot
        stablecoins = [s for s in [c[1] for c in dic_markets[id].index.tolist() if c[2] == 'spot']
                       if Currency.objects.get(code=s).stable_coin]

        # Create a list of currencies for the desired allocations
        codes_short = list(df_account[df_account[('target', 'quantity')] < 0].index.get_level_values('code').unique())
        codes_long = list(df_account[df_account[('target', 'quantity')] > 0].index.get_level_values('code').unique())

        # Create a list of currencies to buy and sell
        codes_sell = list(df_account[(df_account[('delta', 'value')] > 0)].index.get_level_values('code').unique())
        codes_buy = list(df_account[(df_account[('delta', 'value')] < 0)].index.get_level_values('code').unique())
        codes_buy = [c for c in codes_buy if c not in stablecoins]

        # Keep only stablecoins with available balance
        stablecoins = [s for s in stablecoins if s in codes_free]

        # Markets
        #########

        # Create a list of markets with an open position to close
        mk_close_long = [i for i, p in df_positions.iterrows() if p['side'] == 'buy' and i[0] in codes_sell]
        mk_close_short = [i for i, p in df_positions.iterrows() if p['side'] == 'sell' and i[0] in codes_buy]
        mk_close = mk_close_long + mk_close_short

        # Create a list of markets available to open
        mk_candidates = [mk for mk in dic_markets[id].index.tolist() if mk not in mk_close]
        mk_candidates_spot = [mk for mk in mk_candidates if mk[4] == 'spot']
        mk_candidates_open_long = [mk for mk in mk_candidates if mk[0] in codes_buy and mk[4] == 'derivative']
        mk_candidates_open_short = [mk for mk in mk_candidates if mk[0] in codes_sell and mk[4] == 'derivative']
        mk_candidates_open = mk_candidates_open_long + mk_candidates_open_short

        # Create a list of spot markets with free balance > 0
        mk_spot_base = [mk for mk in mk_candidates if mk[4] == 'spot' and mk[0] in codes_free]
        mk_spot_quote = [mk for mk in mk_candidates if mk[4] == 'spot' and mk[1] in codes_free]
        mk_spot = list(set(mk_spot_base + mk_spot_quote))

        # Wallets
        #########

        # Create a list of wallet with derivative markets
        wallets_deri = list(set([mk[2] for mk in mk_candidates if mk[4] == 'derivative']))
        wallets_spot = list(set([mk[2] for mk in mk_candidates if mk[4] == 'spot']))

        print('Currencies', codes)
        print('Currencies free', codes_free)
        print('Allocations long', codes_long)
        print('Allocations short', codes_short)
        print('Instructions buy', codes_buy)
        print('Instructions sell', codes_sell)
        print('Wallets deri', wallets_deri)

        for i in mk_close_long:
            print('Market close long:', i[3], i[2])

        for i in mk_close_short:
            print('Market close short:', i[3], i[2])

        for i in mk_candidates:
            print('Candidates:', i[3], i[2])

        for i in mk_candidates_open_long:
            print('Candidates open long:', i[3], i[2])

        for i in mk_candidates_open_short:
            print('Candidates open short:', i[3], i[2])

        for i in mk_candidates_spot:
            print('Candidates spot:', i[3], i[2])

        for i in mk_spot:
            print('Market spot:', i[3], i[2])

        routes = []

        # Market list structure
        # 0 'code',
        # 1 'quote',
        # 2 'default_type',
        # 3 'symbol',
        # 4 'type',
        # 5 'derivative',
        # 6 'margined'

        # Loop through derivative market to close
        #########################################

        for mk in mk_close:

            # Set instruction
            mk_instr = 'close_long' if mk in mk_close_long else 'close_short'

            # Margined currency is a desired currency
            if mk[6] in codes_buy:
                routes.append(dict(type='derivative',
                                   source_inst=mk_instr,
                                   source_base=mk[0],
                                   source_quot=mk[1],
                                   source_marg=mk[6],
                                   source_symb=mk[3],
                                   source_wall=mk[2]
                                   ))

            # Iterate through available markets
            for candidate in mk_candidates:

                if candidate[4] == 'spot':

                    if mk[6] == candidate[1]:
                        # Margined currency could buy a desired base currency
                        if candidate[0] in codes_buy:
                            candidate_instr = 'buy_base'

                    elif mk[6] == candidate[0]:
                        # Margined currency could buy a desired quote currency
                        if candidate[1] in codes_buy:
                            candidate_instr = 'sell_base'

                elif candidate[4] == 'derivative':

                    if mk[6] == candidate[6]:

                        # Margined currency could open long or close short
                        if candidate in mk_candidates_open_long:
                            candidate_instr = 'open_long'
                        elif candidate in mk_candidates_open_short:
                            candidate_instr = 'open_short'

                # If a candidate was found
                if 'candidate_instr' in locals():
                    routes.append(dict(type='derivative',
                                       source_inst=mk_instr,
                                       source_base=mk[0],
                                       source_quot=mk[1],
                                       source_marg=mk[6],
                                       source_symb=mk[3],
                                       source_wall=mk[2],
                                       destination_inst=candidate_instr,
                                       destination_base=candidate[0],
                                       destination_quot=candidate[1],
                                       destination_marg=candidate[6],
                                       destination_symb=candidate[3],
                                       destination_wall=candidate[2]
                                       ))
                    del candidate_instr

        # Loop through available currencies in spot
        ###########################################

        for mk in mk_spot:

            # Base currency should be sold
            if mk[0] in codes_sell:

                # Quote currency is a desired currency
                if mk[1] in codes_buy:
                    routes.append(dict(type='spot',
                                       source_inst='sell_base',
                                       source_base=mk[0],
                                       source_quot=mk[1],
                                       source_marg=mk[6],
                                       source_symb=mk[3],
                                       source_wall=mk[2]
                                       ))

                # Iterate through available markets
                for candidate in mk_candidates:

                    # Market is a spot
                    if candidate[4] == 'spot':

                        # Quote currency is a quote in this market
                        if mk[1] == candidate[1]:

                            # Quote currency could buy a desired currency
                            if candidate[0] in codes_buy:
                                candidate_instr = 'buy_base'

                    # Market is a derivative
                    elif candidate[4] == 'derivative':

                        # Quote currency bought is margin compatible
                        if mk[1] == candidate[6]:

                            # Margined currency could open long or close short
                            if candidate in mk_candidates_open_long:
                                candidate_instr = 'open_long'
                            elif candidate in mk_candidates_open_short:
                                candidate_instr = 'open_short'

                    if 'candidate_instr' in locals():
                        routes.append(dict(type='spot',
                                           source_inst='sell_base',
                                           source_base=mk[0],
                                           source_quot=mk[1],
                                           source_marg=mk[6],
                                           source_symb=mk[3],
                                           source_wall=mk[2],
                                           destination_inst=candidate_instr,
                                           destination_base=candidate[0],
                                           destination_quot=candidate[1],
                                           destination_marg=candidate[6],
                                           destination_symb=candidate[3],
                                           destination_wall=candidate[2]
                                           ))
                        del candidate_instr

            # Quote currency should be sold
            if mk[1] in codes_sell:

                # Base currency is a desired currency
                if mk[0] in codes_buy:
                    routes.append(dict(type='spot',
                                       source_inst='buy_base',
                                       source_base=mk[0],
                                       source_quot=mk[1],
                                       source_marg=mk[6],
                                       source_symb=mk[3],
                                       source_wall=mk[2]
                                       ))

                # Iterate through available markets
                for candidate in mk_candidates:

                    # Market is a spot
                    if candidate[4] == 'spot':

                        # Base currency bought is a quote in this market
                        if mk[0] == candidate[1]:

                            # Base currency bought could buy a desired currency
                            if candidate[0] in codes_buy:
                                candidate_instr = 'buy_base'

                    # Market is a derivative
                    elif candidate[4] == 'derivative':

                        # Base currency bought is margin compatible
                        if mk[0] == candidate[6]:

                            # Margined currency could open long or close short
                            if candidate in mk_candidates_open_long:
                                candidate_instr = 'open_long'
                            elif candidate in mk_candidates_open_short:
                                candidate_instr = 'open_short'

                    if 'candidate_instr' in locals():
                        routes.append(dict(type='spot',
                                           source_inst='buy_base',
                                           source_base=mk[0],
                                           source_quot=mk[1],
                                           source_marg=mk[6],
                                           source_symb=mk[3],
                                           source_wall=mk[2],
                                           destination_inst=candidate_instr,
                                           destination_base=candidate[0],
                                           destination_quot=candidate[1],
                                           destination_marg=candidate[6],
                                           destination_symb=candidate[3],
                                           destination_wall=candidate[2]
                                           ))
                        del candidate_instr

        # Loop through stablecoins in spot
        ##################################

        for wallet in wallets_spot:

            codes_spot = list(df_account[(df_account[('wallet', 'free_value')] > 0)
                                         & (df_account.index.get_level_values('wallet') == wallet)
                                         ].index.get_level_values('code').unique())
            stablecoins = [c for c in codes_spot if Currency.objects.get(code=c).stable_coin]

            for stable in stablecoins:

                # Iterate through derivative markets
                for candidate in [c for c in mk_candidates if c[4] == 'derivative']:

                    # Stablecoin is margin compatible
                    if stable == candidate[6]:

                        # Margined currency could open long or close short
                        if candidate in mk_candidates_open_long:
                            candidate_instr = 'open_long'
                        elif candidate in mk_candidates_open_short:
                            candidate_instr = 'open_short'

                        if 'candidate_instr' in locals():
                            routes.append(dict(type='stablecoin',
                                               source_inst='transfer',
                                               source_base=np.nan,
                                               source_quot=np.nan,
                                               source_marg=stable,
                                               source_symb=np.nan,
                                               source_wall=wallet,
                                               destination_inst=candidate_instr,
                                               destination_base=candidate[0],
                                               destination_quot=candidate[1],
                                               destination_marg=candidate[6],
                                               destination_symb=candidate[3],
                                               destination_wall=candidate[2]
                                               ))
                            del candidate_instr

        # Loop through available margin
        ###############################

        for wallet in wallets_deri:
            code_free_margin = list(df_account[(df_account[('wallet', 'free_value')] > 0)
                                               & (df_account.index.get_level_values('wallet') == wallet)
                                               ].index.get_level_values('code').unique())

            for code in code_free_margin:

                # Check if margin can be released in spot
                # or if it can serve in another derivative market
                for candidate in mk_candidates_spot + mk_candidates_open:

                    if candidate[4] == 'spot':

                        # Release free margin if it can buy a desired currency in spot
                        if code == candidate[1]:

                            if candidate[0] in codes_buy:
                                candidate_instr = 'buy_base'
                        elif code == candidate[0]:
                            if candidate[1] in codes_buy:
                                candidate_instr = 'sell_base'

                    elif candidate[4] == 'derivative':

                        # Allocate free margin if another derivative market is margin compatible
                        if code == candidate[6]:

                            if candidate in mk_candidates_open_long:
                                candidate_instr = 'open_long'
                            elif candidate in mk_candidates_open_short:
                                candidate_instr = 'open_short'

                    if 'candidate_instr' in locals():

                        # If a transfer is needed
                        if wallet != candidate[2]:
                            routes.append(dict(type='free margin',
                                               source_inst='transfer',
                                               source_base=np.nan,
                                               source_quot=np.nan,
                                               source_marg=code,
                                               source_symb=np.nan,
                                               source_wall=wallet,
                                               destination_inst=candidate_instr,
                                               destination_base=candidate[0],
                                               destination_quot=candidate[1],
                                               destination_marg=candidate[6],
                                               destination_symb=candidate[3],
                                               destination_wall=candidate[2]
                                               ))
                        else:
                            routes.append(dict(type='free margin',
                                               source_inst=candidate_instr,
                                               source_base=candidate[0],
                                               source_quot=candidate[1],
                                               source_marg=candidate[6],
                                               source_symb=candidate[3],
                                               source_wall=wallet
                                               ))
                        del candidate_instr

                # Check if margin can be allocated in another derivative market non-margin compatible
                for candidate in mk_candidates_open:

                    # Margin isn't compatible
                    if code != candidate[6]:

                        # Search spot markets where free margin could be sold for the desired margin currency
                        for spot in mk_candidates_spot:

                            if code == spot[1]:
                                if candidate[6] == spot[0]:
                                    spot_inst = 'buy_base'
                            elif code == spot[0]:
                                if candidate[6] == spot[1]:
                                    spot_inst = 'sell_base'

                            if candidate in mk_candidates_open_long:
                                candidate_instr = 'open_long'
                            elif candidate in mk_candidates_open_short:
                                candidate_instr = 'open_short'

                            if 'spot_inst' in locals():
                                if 'candidate_instr' in locals():
                                    routes.append(dict(type='free margin',
                                                       source_inst='transfer int',
                                                       source_base=np.nan,
                                                       source_quot=np.nan,
                                                       source_marg=code,
                                                       source_symb=np.nan,
                                                       source_wall=wallet,

                                                       intermediate_inst=spot_inst,
                                                       intermediate_symb=spot[3],
                                                       intermediate_wall=spot[2],

                                                       destination_inst=candidate_instr,
                                                       destination_base=candidate[0],
                                                       destination_quot=candidate[1],
                                                       destination_marg=candidate[6],
                                                       destination_symb=candidate[3],
                                                       destination_wall=candidate[2]
                                                       ))
                                    del candidate_instr, spot_inst

                            elif 'candidate_instr' in locals():
                                del candidate_instr

        # Create dataframe
        df_routes = pd.DataFrame()
        names = ['base_s', 'quote_s', 'margin_s', 'symbol_s', 'wallet_s',
                 'symbol_i', 'wallet_i',
                 'base_d', 'quote_d', 'margin_d', 'symbol_d', 'wallet_d']

        # Insert routes into dataframe
        for i, r in enumerate(routes):

            # Fill dictionary with np.nan if necessary
            if 'intermediate_symb' not in r.keys():
                r['intermediate_inst'], r['intermediate_symb'], r['intermediate_wall'] = [np.nan for i in range(3)]

            if 'destination_symb' not in r.keys():
                r['destination_symb'], r['destination_wall'], r['destination_inst'], \
                r['destination_base'], r['destination_quot'], r['destination_marg'] = [np.nan for i in range(6)]

            # Construct an index
            index = [r['source_base'],
                     r['source_quot'],
                     r['source_marg'],
                     r['source_symb'],
                     r['source_wall'],

                     r['intermediate_symb'],
                     r['intermediate_wall'],

                     r['destination_base'],
                     r['destination_quot'],
                     r['destination_marg'],
                     r['destination_symb'],
                     r['destination_wall']
                     ]

            indexes = pd.MultiIndex.from_tuples([index], names=names)
            columns = pd.MultiIndex.from_product([['route'], ['type']], names=['level_1', 'level_2'])

            # Create a dataframe with route type
            df = pd.DataFrame([[r['type']]], index=indexes, columns=columns)

            # Add actions
            df.loc[indexes, ('instruction', 'source')] = r['source_inst']
            df.loc[indexes, ('instruction', 'inter')] = r['intermediate_inst']
            df.loc[indexes, ('instruction', 'destination')] = r['destination_inst']

            # Add route id
            df.loc[indexes, ('route', 'id')] = i
            df.loc[indexes, ('route', 'id')] = df.loc[indexes, ('route', 'id')].astype(int)

            # Finally concatenate dataframe
            df_routes = pd.concat([df, df_routes], axis=0)

        # df_routes.sort_index(axis=0, level=[0, 1], inplace=True)
        df_routes.sort_index(axis=1, level=[0, 1], inplace=True)

        # print(df_routes.to_string())
        return df_routes

    # return cumulative orderbook
    def cumulative_book(ob):

        asks = ob['asks']
        bids = ob['bids']
        asks_p = [a[0] for a in asks]
        bids_p = [a[0] for a in bids]
        cum_a = list(accumulate([a[1] for a in asks]))
        cum_b = list(accumulate([a[1] for a in bids]))
        return [[bids_p[i], cum_b[i]] for i, a in enumerate(bids)], [[asks_p[i], cum_a[i]] for i, a in enumerate(asks)]

    # Update df_markets with best bid and ask at every iteration
    def update_markets_bid_ask(id, wallet, symbol, code, quote, bids, asks):

        dic_markets[id].loc[(code, quote, wallet, symbol), ('price', 'bid')] = bids[0][0]
        dic_markets[id].loc[(code, quote, wallet, symbol), ('price', 'ask')] = asks[0][0]
        dic_markets[id].sort_index(axis=0, inplace=True)  # Prevent past lexsort depth PerformanceWarning

    # Update df_routes with amount and costs at every iteration
    def update_routes_cost(id, wallet, symbol, base, quote, bids, asks):

        # Sort dataframe to avoid warning when df.index.is_lexsorted() == False
        dic_routes[id].sort_index(axis=0, level=[0, 1], inplace=True)
        dic_routes[id].sort_index(axis=1, level=[0, 1], inplace=True)

        # Select index of routes where market is a source, an intermediary or a destination
        indexes_src = dic_routes[id].loc[(dic_routes[id].index.get_level_values('symbol_s') == symbol) & (
                dic_routes[id].index.get_level_values('wallet_s') == wallet)].index
        indexes_int = dic_routes[id].loc[(dic_routes[id].index.get_level_values('symbol_i') == symbol) & (
                dic_routes[id].index.get_level_values('wallet_i') == wallet)].index
        indexes_dst = dic_routes[id].loc[(dic_routes[id].index.get_level_values('symbol_d') == symbol) & (
                dic_routes[id].index.get_level_values('wallet_d') == wallet)].index

        # Select row with it's indice and update columns of the dataframe
        def update_cost(position, value, code, depth, row):

            # Convert trade value from USD to currency amount
            # Amount can be different in source, intermediate and destination market
            spot = dic_accounts[id].loc[(code, wallet), 'price'][0]
            quantity = value / spot

            # Get best price and distance % for the desired amount
            best_price, distance = calculate_distance(depth, quantity)
            spread = calculate_spread(bids, asks)
            cost = distance + spread

            # Use name of the Pandas series as index
            idx = row.name

            # Favor (penalise) cost of a route if instruction is to open short (long)
            instruction = row['instruction'][position]
            if instruction in ['open_long', 'open_short']:

                # Return funding rate is market is perp
                funding = get_funding(base, quote, wallet, symbol)
                if funding:
                    if instruction == 'open_short':
                        cost -= funding * 10
                    else:
                        cost += funding * 10

                    # log.info('Add funding column')
                    dic_routes[id].loc[idx, (position, 'funding')] = funding

            # Select delta quantity
            delta_qty = dic_accounts[id].loc[(code, wallet), ('delta', 'quantity')]

            # Update columns
            dic_routes[id].loc[idx, (position, 'quantity')] = quantity
            dic_routes[id].loc[idx, (position, 'value')] = value
            dic_routes[id].loc[idx, (position, 'distance')] = distance
            dic_routes[id].loc[idx, (position, 'spread')] = spread
            dic_routes[id].loc[idx, (position, 'cost')] = cost
            dic_routes[id].loc[idx, (position, 'quantity %')] = abs(quantity / delta_qty)

            # print(dic_routes[id].loc[idx, :])

        # Return funding rate
        def get_funding(base, quote, wallet, symbol):

            market = dic_markets[id].loc[(base, quote, wallet, symbol)]
            if market.index.get_level_values('derivative') == 'perpetual':
                funding = market['funding']['rate'][0]
                return funding

        # Return trade value and depth book
        def get_value_n_depth(route, inter):

            instruction_src = route['instruction']['source']
            instruction_int = route['instruction']['inter']
            instruction_dst = route['instruction']['destination']

            base_s, quote_s, margin_s, symbol_s, wallet_s, symbol_i, wallet_i, \
            base_d, quote_d, margin_d, symbol_d, wallet_d = [route.name[i] for i in range(12)]

            # Return depth side for an instruction
            def get_depth(instruction):

                if instruction in ['sell_base', 'close_long', 'open_short']:
                    return bids
                elif instruction in ['buy_base', 'open_long', 'close_short']:
                    return asks

            # First instruction is to release a position
            ############################################

            if route['route']['type'] == 'derivative':

                # Select value of the position to release
                posit = abs(dic_accounts[id].loc[(base_s, wallet_s), ('position', 'value')])  # positive or negative
                delta_s = abs(dic_accounts[id].loc[(base_s, wallet_s), ('delta', 'value')])

                # Instruction is to buy margin (measure delta on base)
                if pd.isna(instruction_dst):
                    inst = instruction_src
                    value = min(posit, delta_s)
                    code = base_s

                # Instruction is to buy margin then trade spot or derivative
                else:
                    inst = instruction_dst
                    if inst in ['buy_base', 'open_long', 'open_short']:
                        code = base_d  # measure delta on the currency to buy
                    elif inst == 'sell_base':
                        code = base_d  # measure delta on the currency to sell

                    delta_d = abs(dic_accounts[id].loc[(code, wallet_d), ('delta', 'value')])
                    delta = min(delta_s, delta_d)
                    value = min(posit, delta)

            # First instruction is to sell spot
            ###################################

            elif route['route']['type'] == 'spot':

                # Select value of the currency to sell
                balance_code = quote_s if instruction_src == 'buy_base' else base_s
                balance = dic_accounts[id].loc[(balance_code, wallet_s), ('wallet', 'free_value')]  # positive

                # Select delta value of the currency to buy
                delta_code_s = base_s if instruction_src == 'buy_base' else quote_s
                delta_s = abs(dic_accounts[id].loc[(delta_code_s, wallet_s), ('delta', 'value')])

                # Instruction is to trade spot only
                if pd.isna(instruction_dst):
                    inst = instruction_src
                    value = min(balance, delta_s)
                    code = delta_code_s

                # Route instruction is to trade spot then derivative
                else:
                    inst = instruction_dst
                    if inst in ['buy_base', 'open_long', 'open_short']:
                        code = base_d  # measure delta on the currency to buy
                    elif inst == 'sell_base':
                        code = base_d  # measure delta on the currency to sell

                    delta_d = abs(dic_accounts[id].loc[(code, wallet_d), ('delta', 'value')])
                    delta = min(delta_s, delta_d)
                    value = min(balance, delta)

            # First instruction is transfer stablecoin
            ##########################################

            elif route['route']['type'] == 'stablecoin':

                # Select value of the stablecoin to use as margin
                balance = dic_accounts[id].loc[(margin_s, wallet_s), ('wallet', 'free_value')]  # positive

                # Instruction is to trade derivative
                inst = instruction_dst
                code = base_d

                delta_d = abs(dic_accounts[id].loc[(code, wallet_d), ('delta', 'value')])
                value = min(balance, delta_d)

            # First instruction is to trade free margin
            ###########################################

            elif route['route']['type'] == 'free margin':

                # Select free margin value in source
                free_margin = dic_accounts[id].loc[(margin_s, wallet_s), ('wallet', 'free_value')]  # positive

                # Trade source market if 2nd instruction is nan
                if pd.isna(instruction_dst):
                    inst = instruction_src
                    code = base_s
                    wallet = wallet_s

                else:
                    inst = instruction_dst
                    code = base_d
                    wallet = wallet_d

                delta = abs(dic_accounts[id].loc[(code, wallet), ('delta', 'value')])
                value = min(free_margin, delta)

            depth = get_depth(inst) if not inter else get_depth(instruction_int)  # inter if spot gateway

            # Check hedge capacity
            value = check_hedge(id, inst, code, value)

            return value, depth

        # Iterate through route's positions and update dataframe
        def update(position, route):

            # Iterate through routes
            for index, row in dic_routes[id].iterrows():
                if pd.Index(index).equals(pd.Index(route)):
                    if row['instruction'][position] not in ['transfer', 'transfer int']:
                        route_id = row['route']['id']

                        # Get trade value and depth book
                        inter = True if position == 'inter' else False  # Select depth of intermediate
                        trade_value, depth = get_value_n_depth(row, inter)

                        # Update columns in dataframe
                        update_cost(position, trade_value, base, depth, row)

        # Iterate through indexes the market belong
        for route in indexes_src:
            update('source', route)
        for route in indexes_int:
            update('inter', route)
        for route in indexes_dst:
            update('destination', route)

        # Sum routes costs
        ##################

        # Check columns and return route cost
        def get_cost(route, position):

            instruction = route['instruction'][position]

            if not pd.isna(instruction):
                if instruction not in ['transfer', 'transfer int']:
                    if position in route.index.get_level_values('level_1'):
                        if 'cost' in route[position].index:
                            if not pd.isna(route[position]['cost']):
                                return route[position]['cost']
                else:
                    return np.nan
            else:
                return np.nan

        # Create a new column with total route cost
        for index, route in dic_routes[id].iterrows():

            dic_routes[id].sort_index(axis=0, level=[0, 1], inplace=True)
            dic_routes[id].sort_index(axis=1, level=[0, 1], inplace=True)

            costs = [get_cost(route, position) for position in ['source', 'inter', 'destination']]

            if all(costs):
                # Remove nan
                costs = [c for c in costs if not pd.isna(c)]
                cost = sum(costs)
                # Create a new column with total route cost
                dic_routes[id].loc[index, ('route', 'cost')] = cost

        # print(dic_routes[id].to_string())
        # dic_routes[id].drop((value_source == 0).index, inplace=True)

    # Update df_markets with order status after an order is placed
    def update_markets_df(id, client_order_id):

        order = Order.objects.get(id=client_order_id)

        # Log order status
        if order.status == 'closed':
            log.info('Order is filled')
        elif order.status == 'open':
            log.info('Order is pending')

        log.info('Update market dataframe', order=order.id)

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
        dic_markets[id].loc[idx, ('order', 'id')] = order.id
        dic_markets[id].loc[idx, ('order', 'type')] = order.route_type
        dic_markets[id].loc[idx, ('order', 'amount')] = order.amount
        dic_markets[id].loc[idx, ('order', 'status')] = order.status
        dic_markets[id].loc[idx, ('order', 'filled')] = order.filled

        print(dic_markets[id].to_string())

    # Update the latest fund object after a trade is executed
    def update_fund_object(account, order_ids):

        log.info('Update fund object')

        # Select wallet of markets where trades occurred
        wallets = list(set([order.market.default_type for order in Order.objects.filter(orderid__in=order_ids)]))

        if wallets:
            for wallet in wallets:
                create_fund.run(account.id, wallet=wallet)
        else:
            create_fund.run(account.id, wallet='default')

        log.info('Update fund object done')

    # Update df_account free_value after a transfer
    def update_account_free_value(id, index, route):

        log.info('Update wallets balance')

        # Get quantity
        value = route['destination']['value']
        quantity = route['destination']['quantity']
        instruction = route['instruction']['source']

        if instruction == 'transfer':
            to_wallet = index[11]  # intermediary (spot)
        elif instruction == 'transfer int':
            to_wallet = index[6]  # destination

        # Get coin and wallets
        code = index[2]  # margin
        from_wallet = index[4]

        print('\n', dic_accounts[id].to_string())

        dic_accounts[id].loc[(code, from_wallet)]['wallet']['free_value'] -= value  # source wallet
        dic_accounts[id].loc[(code, from_wallet)]['wallet']['free_quantity'] -= quantity  # source wallet

        dic_accounts[id].loc[(code, to_wallet)]['wallet']['free_value'] += value  # destination wallet
        dic_accounts[id].loc[(code, to_wallet)]['wallet']['free_quantity'] += quantity  # destination wallet

        print('\n', dic_accounts[id].to_string())

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
                res = [r for r in res if r is not None]

                log.info('Open orders updated')
                return res

            else:
                log.error('Open orders update failed')
        else:
            log.info('Open order not found')

    # Place an order to the best route for every source currency
    # and update df_markets when an order is placed
    def trade(account):

        id = account.id
        log.bind(account=account.name)

        # Wait route cost
        #################

        if not any(dic_routes[id].columns.isin([('route', 'cost')])):
            print('Route cost nan')
            return

        if any(pd.isna(dic_routes[id]['route']['cost'].array)):
            print('No route cost')
            return

        # Return True if an order is open
        def is_order(index, route):

            # Select base from destination if base source is nan
            if pd.isna(index[0]):
                code = index[7]

            # Select quote from source
            elif pd.isna(route['instruction']['source']) == 'sell_base':
                code = index[1]

            # Select base from source
            else:
                code = index[0]

            # Select markets which use this route and check orders status
            markets = dic_markets[id].xs(code, level='base', axis=0)
            status = list(markets['order']['status'])

            # Abort trade execution if an order is open or closed for this code
            if status == 'open':
                log.info('Order is pending')
                return True

            elif status == 'closed':
                log.info('Order closed')
                return True

            else:
                return False

        # Return trade data
        def get_data(index, route):

            # Create a list with symbol to trade and wallet
            if route['instruction']['source'] == 'transfer':
                position = 'destination'
                market = [index[10], index[11]]

            elif route['instruction']['source'] == 'transfer int':
                position = 'inter'
                market = [index[5], index[6]]

            else:
                position = 'source'
                market = [index[3], index[4]]

            # Select variables
            quantity = route[position]['quantity']
            value = route[position]['value']
            instruction = route['instruction'][position]

            # Set trade side
            if instruction in ['open_long', 'close_short', 'buy_base']:
                side = 'buy'
            elif instruction in ['open_short', 'close_long', 'sell_base']:
                side = 'sell'

            return market + [quantity, value, instruction, side]

        # Convert currency to contract if necessary
        def convert(market, quantity):

            # Convert quantity to contract
            if market.type == 'derivative':
                amount = methods.amount_to_contract(market, quantity)
                log.info('Trade {0} contracts or {2} {1}'.format(round(amount, 4), market.symbol, round(quantity, 4)))

            else:
                amount = quantity
                log.info('Trade {0} {1} on {2}'.format(round(amount, 6), market.base, market.type))

            return amount

        # Format and return price
        def get_price(market, side):

            # Limit price order
            if account.limit_order:
                if exchange.has['createLimitOrder']:

                    price = market.get_candle_price_last()

                    # Add or remove tolerance
                    if side == 'buy':
                        price = price + price * float(account.limit_price_tolerance)
                    elif side == 'sell':
                        price = price - price * float(account.limit_price_tolerance)
                    return price

                else:
                    raise Exception('Limit order not supported')

            # Market order
            else:
                if exchange.has['createMarketOrder']:
                    # Return a price to validate MIN_NOTIONAL
                    return market.get_candle_price_last()
                else:
                    raise Exception('Market order not supported')

        # Check MIN_NOTIONAL condition
        def check_min_notional(market, instruction, amount, price):

            # Create a dictionary for trade options
            options = dict()

            # Test condition for min_notional
            min_notional = methods.limit_cost(market, amount, price)

            if market.exchange.exid == 'binance':

                # If market is spot and if condition is applied to MARKET order
                if market.type == 'spot':
                    if market.response['info']['filters'][3]['applyToMarket']:
                        if not min_notional:
                            return False

                # If market is USDT margined and if verification fails set reduce_only = True
                elif not min_notional:

                    if not market.type == 'derivative':
                        return False
                    elif not market.margined.code == 'USDT':
                        return False
                    elif instruction not in ['close_long', 'close_short']:
                        return False

                    else:
                        log.info('Set reduce_only = True')
                        options['reduce_only'] = True

            else:
                if not min_notional:
                    return False

            # Return dictionary
            return [True, options]

        # Execute trade logic
        #####################

        log.info('Trading')

        # Sort routes by source currency and cost, then reorder columns
        dic_routes[id] = dic_routes[id].sort_values([('route', 'cost')], ascending=True)
        # dic_routes[id] = dic_routes[id].sort_index(level=[0], ascending=[True])
        dic_routes[id] = dic_routes[id].sort_index(axis=1)

        print('\n')
        print(dic_accounts[account.id].to_string())
        print('\n')
        print(dic_routes[account.id].to_string())
        print('\n')
        print(dic_markets[account.id].to_string())
        print('\n')

        # Loop through the best routes
        for index, route in dic_routes[id].iterrows():

            # Check open order
            ##################

            if is_order(index, route):
                continue

            else:
                # Transfer funds
                ################

                res = transfer(account.id, index, route)
                if res:
                    update_account_free_value(account.id, index, route)

                # Select and verify trade data
                ##############################

                symbol, wallet, quantity, value, instruction, side = get_data(index, route)
                market = Market.objects.get(exchange=exchange, symbol=symbol, default_type=wallet)

                # Determine price
                price = get_price(market, side)

                if account.limit_order:

                    # Format price decimal
                    price = methods.format_decimal(counting_mode=exchange.precision_mode,
                                                   precision=market.precision['price'],
                                                   n=price
                                                   )
                    # Check price conditions
                    if not methods.limit_price(market, price):
                        continue

                # Format amount decimal
                amount = methods.format_decimal(counting_mode=exchange.precision_mode,
                                                precision=market.precision['amount'],
                                                n=quantity
                                                )
                # Check amount conditions
                if not methods.limit_amount(market, float(amount)):
                    continue

                # Check cost condition
                min_notional, option = check_min_notional(market, instruction, amount, price)
                if not min_notional:
                    continue

                # Create an order object and return it's primary key
                object_id = account.create_order(market, instruction, convert(market, quantity))

                # created
                if object_id:

                    # Place an order return client order id
                    client_order_id = place_order.run(account.id, object_id)

                    # placed
                    if client_order_id:

                        # Update markets_df with order status
                        update_markets_df(id, client_order_id)
                        break

                    else:
                        log.error('Trading failed no response from exchange')
                        break

                else:
                    # Continue index loop
                    log.warning('Order object creation failed, continue')
                    continue

        log.info('Trade complete')

    # Create dictionaries
    def create_dictionaries(account_id):

        log.info('Dictionaries creation')
        # df_accounts
        create_fund.run(account_id)
        dic_accounts[account_id] = account.create_df_account()

        # df_positions
        update_positions.run(account_id)
        dic_positions[account_id] = account.create_df_positions()

        # df_markets
        dic_markets[account_id] = create_df_markets()

        # df_routes
        dic_routes[account_id] = create_routes(account_id,
                                               dic_accounts[account_id],
                                               dic_positions[account_id]
                                               )
        log.info('Dictionaries created')
        return dic_accounts[account_id], dic_positions[account_id], dic_markets[account_id], dic_routes[account_id]

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
                            if trade(account):

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
                                    dic_accounts[account.id], dic_positions[account.id], dic_markets[account.id], \
                                    dic_routes[account.id] = create_dictionaries(account.id)

                            else:
                                # Break loop is trade return None
                                break

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

        ws_loops = [watch_book(client, market, i, j) for j, market in enumerate(markets) if market.is_updated()
                    and market.derivative != 'future']

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
        dic_accounts[account.id], dic_positions[account.id], dic_markets[account.id], dic_routes[
            account.id] = create_dictionaries(account.id)

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
