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
            amount=float(order.amount),
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
                        log.info('Order placed successfully')
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
def create_fund(account_id, wallet=None):
    """
    Used to create (update) a fund object. Fund object contains JSON data of wallets in a trading account. Wallet
    like spot, derivative wallets like Binance future and Binance delivery wallets.
    Fund object is created hourly and is updated after an order is passed.
    """
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
    def create_fund_object(total, free, used, margin_assets):

        kwargs = dict(
            account=account,
            exchange=account.exchange,
            balance=sum(value['value'] for key in total.keys() for value in total[key].values()),
            margin_assets=margin_assets,
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

    # Create a dictionary of margin assets
    def get_margin_assets(response, default_type=None):

        if account.exchange.exid == 'binance':
            if default_type in ['future', 'delivery']:
                return [i for i in response['info']['assets'] if float(i['walletBalance']) > 0]

        elif account.exchange.exid == 'bybit':
            return [v for k, v in response['info']['result'].items() if v['wallet_balance'] > 0]

        else:
            return

    # Create a dictionary of position
    def update_positions(response, default_type=None):

        if account.exchange.exid == 'binance':
            if default_type in ['future', 'delivery']:

                for position in response['info']['positions']:
                    initial_margin = float(position['initialMargin'])
                    if initial_margin > 0:

                        defaults = dict(
                            initial_margin=initial_margin,
                            maint_margin=float(position['maintMargin']),
                            order_initial_margin=float(position['openOrderInitialMargin'])
                        )

                        obj, created = Position.objects.update_or_create(exchange=account.exchange,
                                                                         market__default_type=default_type,
                                                                         market__response__id=position['symbol']
                                                                         , defaults=defaults)
                        if created:
                            log.warning('New position created for {0}'.format(position['symbol']))

        elif account.exchange.exid == 'bybit':
            raise Exception('Missing')

        else:
            raise Exception('Missing')

    # Create empty dictionaries
    total, used, free, margin_assets = [dict() for _ in range(4)]

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

        latest.margin_assets[wallet] = get_margin_assets(response, wallet)
        latest.save()
        log.info('Latest fund object has been updated for {0}'.format(wallet))

        update_positions(response, wallet)
        log.info('Position updated for {0}'.format(wallet))

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

                margin_assets[default_type] = get_margin_assets(response, default_type)
                update_positions(response, default_type)

        create_fund_object(total, free, used, margin_assets)

    else:

        default_type = 'default'

        if account.exchange.has_credit():
            response = client.fetchBalance()
            account.exchange.update_credit('fetchBalance')

            t, u, f = create_dict(response)

            total[default_type] = t
            used[default_type] = u
            free[default_type] = f

            margin_assets[default_type] = get_margin_assets(response)

            create_fund_object(total, free, used, margin_assets)


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
                    orderid = methods.order_create_update(account, response)
                    return orderid

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

    log.info('Update positions done')


# Transfer fund between wallets
@shared_task(base=BaseTaskWithRetry)
def transfer(account_id, index, route):
    # Get coin and source wallets
    code = index[0]
    from_wallet = index[1]

    # Get destination market
    if not pd.isna(route['gateway']['action']):
        to_wallet = index[8]
        to = 'gateway'
    elif not pd.isna(route['destination']['action']):
        to_wallet = index[13]
        to = 'destination'

    quantity = route[to]['quantity']
    value = route[to]['value']

    if value < 5:
        log.info('Transfer value too small')
        return

    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    if account.exchange.has_credit():
        try:

            log.info('Transfer {0} {1} from {2} to {3}'.format(round(quantity, 1), code, from_wallet, to_wallet))
            response = client.transfer(code, quantity, from_wallet, to_wallet)

        except Exception as e:

            account.exchange.update_credit('transfer', 'spot')
            log.error('Unable to transfer fund')
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

    # Calculate the average price and distance from best bid (ask)
    def get_price_n_distance(depth, amount):

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
        average_price = sum([a * b for a, b in zip(prices, weights)])

        # Calculate distance in % to the best bid or to the best ask
        distance = abs(100 * (average_price / depth[0][0] - 1))

        return average_price, distance

    # Calculate bid-ask spread
    def get_spread(bids, asks):

        spread = asks[0][0] - bids[0][0]
        spread_pct = spread / asks[0][0]

        return spread_pct * 100

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

        markets = dic_markets[id].index.tolist()

        # Create a list of currencies with free balance > 0
        codes_free = list(df_account[(df_account[('wallet', 'free_value')] > 0)]
                          .index.get_level_values('code').unique())

        codes_free_spot_base = [code for code in [mk[0] for mk in markets if mk[2] == 'spot'] if code in codes_free]
        codes_free_spot_quote = [code for code in [mk[1] for mk in markets if mk[2] == 'spot'] if code in codes_free]
        codes_free_spot = list(set(codes_free_spot_base + codes_free_spot_quote))

        # Create a list of currencies to buy and sell
        codes_sell = list(df_account[(df_account[('delta', 'value')] > 0)].index.get_level_values('code').unique())
        code_sell_spot = [code for code in codes_free_spot if code in codes_sell]
        codes_buy = list(df_account[(df_account[('delta', 'value')] < 0)].index.get_level_values('code').unique())

        # Prevent buying stablecoin if the value of hedge positions is greater than new cash allocation
        if get_hedge_capacity(id) < 0:
            stablecoins = account.exchange.get_stablecoins()
            codes_buy = [c for c in codes_buy if c not in stablecoins]

        # Markets
        #########

        # Create a list of markets with an open position to close
        mk_close_long = [i for i, p in df_positions.iterrows() if p['side'] == 'buy' and i[0] in codes_sell]
        mk_close_short = [i for i, p in df_positions.iterrows() if p['side'] == 'sell' and i[0] in codes_buy]
        mk_close = mk_close_long + mk_close_short

        mk_close_hedge = dic_positions[id].loc[dic_positions[id]['hedge'] == True, :].index

        # Create a list of markets available to open
        mk_candidates = [mk for mk in markets]
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

        print('Currencies', codes)
        print('Instructions buy', codes_buy)
        print('Instructions sell', codes_sell)
        print('Wallet derivative', wallets_deri)
        print('Wallet spot', wallets_spot)

        for i in mk_close_long:
            print('Market close long:', i[3], i[2])

        for i in mk_close_short:
            print('Market close short:', i[3], i[2])

        for i in mk_close_hedge:
            print('Market close hedge:', i[3], i[2])

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

        # Find available routes
        def find_routes(label, wallet, code=None, market=None):

            if code:
                source = dict(
                    priority=2,
                    transfer=False,
                    type=label,
                    currency=code,
                    wallet=wallet
                )

            elif market:

                code = market[6]
                source = dict(
                    priority=2,
                    transfer=False,
                    type=label,
                    source_marg=code,
                    source_inst='close_long' if market in mk_close_long else 'close_short',
                    source_base=market[0],
                    source_quot=market[1],
                    source_symb=market[3],
                    source_wall=market[2]
                )

                # Margin is a desired currency
                if code in codes_buy:
                    routes.append(source)

            for candidate in mk_candidates:

                if candidate[4] == 'spot':

                    if candidate[0] in codes_buy:
                        if candidate[0] != code:
                            candidate_instr = 'buy_base'
                    if candidate[1] in codes_buy:
                        if candidate[1] != code:
                            candidate_instr = 'sell_base'

                elif candidate[4] == 'derivative':

                    if candidate in mk_candidates_open_long:
                        candidate_instr = 'open_long'
                    elif candidate in mk_candidates_open_short:
                        candidate_instr = 'open_short'

                    # Prevent close->open same base
                    if market:
                        if candidate[0] == market[0]:
                            del candidate_instr
                            continue

                if 'candidate_instr' in locals():

                    destination = dict(destination_inst=candidate_instr,
                                       destination_base=candidate[0],
                                       destination_quot=candidate[1],
                                       destination_marg=candidate[6],
                                       destination_symb=candidate[3],
                                       destination_wall=candidate[2]
                                       )

                    # Add gateway
                    def add_gateway(code, code_needed):
                        for market in mk_candidates_spot:

                            gateway = dict()

                            if code == market[1]:
                                if market[0] == code_needed:
                                    gateway['gateway_inst'] = 'buy_base'
                            elif code == market[0]:
                                if market[1] == code_needed:
                                    gateway['gateway_inst'] = 'sell_base'

                            if 'gateway_inst' in gateway:
                                gateway['gateway_symb'] = market[3]
                                gateway['gateway_wall'] = market[2]

                                return gateway

                        log.warning('No gateway between {0} and {1}'.format(code, code_needed))
                        return

                    # Margin isn't compatible with code (can't open position)
                    if candidate_instr in ['open_long', 'open_short'] and code != candidate[6]:
                        gateway = add_gateway(code, candidate[6])
                        if not gateway:
                            continue

                    # Quote isn't compatible with code (can't buy base)
                    if candidate_instr == 'buy_base' and code != candidate[1]:
                        gateway = add_gateway(code, candidate[1])
                        if not gateway:
                            continue

                    # Base isn't compatible with code (can't buy quote)
                    if candidate_instr == 'sell_base' and code != candidate[0]:
                        gateway = add_gateway(code, candidate[0])
                        if not gateway:
                            continue

                    del candidate_instr
                    route = {**source, **destination}

                    # Set transfer flag
                    if 'gateway' in locals():
                        if gateway:
                            if code:
                                if wallet != gateway['gateway_wall']:
                                    route['transfer'] = True

                                    route.update(gateway)
                            del gateway

                    else:
                        if code:
                            if wallet != destination['destination_wall']:
                                route['transfer'] = True

                    routes.append(route)

        routes = []

        # Market structure
        # 0 'code',
        # 1 'quote',
        # 2 'default_type',
        # 3 'symbol',
        # 4 'type',
        # 5 'derivative',
        # 6 'margined'

        # Create routes to close positions
        ##################################

        for market in mk_close:
            find_routes('close position', market[2], market=market)

        # Create routes for currencies in spot
        ######################################

        for wallet in wallets_spot:
            for code in code_sell_spot:
                find_routes('spot', wallet, code=code)

        # Create routes for available margin
        #####################################

        for wallet in wallets_deri:
            margin = list(df_account[(df_account[('wallet', 'free_value')] > 0)
                                     & (df_account.index.get_level_values('wallet') == wallet)
                                     ].index.get_level_values('code').unique())

            for code in margin:
                find_routes('margin', wallet, code)

        # Create routes to close hedge
        ##############################

        if get_hedge_capacity(id) < 0:
            for mk in mk_close_hedge:
                routes.append(dict(type='close hedge',
                                   priority=1,
                                   transfer=False,
                                   source_inst='close_short',
                                   source_base=mk[0],
                                   source_quot=mk[1],
                                   source_marg=mk[6],
                                   source_symb=mk[3],
                                   source_wall=mk[2]
                                   ))

        # Create dataframe
        df_routes = pd.DataFrame()
        names = ['currency', 'wallet',
                 'base_s', 'quote_s', 'margin_s', 'symbol_s', 'wallet_s',
                 'symbol_g', 'wallet_g',
                 'base_d', 'quote_d', 'margin_d', 'symbol_d', 'wallet_d']

        # Insert routes into dataframe
        for i, r in enumerate(routes):

            # Fill dictionary with np.nan if necessary
            if 'currency' not in r.keys():
                r['currency'], r['wallet'] = [np.nan for i in range(2)]

            if 'source_symb' not in r.keys():
                r['source_symb'], r['source_wall'], r['source_inst'], \
                r['source_base'], r['source_quot'], r['source_marg'] = [np.nan for i in range(6)]

            if 'gateway_inst' not in r.keys():
                r['gateway_inst'], r['gateway_symb'], r['gateway_wall'] = [np.nan for i in range(3)]

            if 'destination_symb' not in r.keys():
                r['destination_symb'], r['destination_wall'], r['destination_inst'], \
                r['destination_base'], r['destination_quot'], r['destination_marg'] = [np.nan for i in range(6)]

            # Construct an index
            index = [r['currency'],
                     r['wallet'],

                     r['source_base'],
                     r['source_quot'],
                     r['source_marg'],
                     r['source_symb'],
                     r['source_wall'],

                     r['gateway_symb'],
                     r['gateway_wall'],

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
            df.loc[indexes, ('source', 'action')] = r['source_inst']
            df.loc[indexes, ('gateway', 'action')] = r['gateway_inst']
            df.loc[indexes, ('destination', 'action')] = r['destination_inst']

            # Add route parameters
            df.loc[indexes, ('route', 'id')] = i
            df.loc[indexes, ('route', 'id')] = df.loc[indexes, ('route', 'id')].astype(int)
            df.loc[indexes, ('route', 'priority')] = r['priority']
            df.loc[indexes, ('route', 'priority')] = df.loc[indexes, ('route', 'priority')].astype(int)
            df.loc[indexes, ('route', 'transfer')] = r['transfer']

            # Finally concatenate dataframe
            df_routes = pd.concat([df, df_routes], axis=0)

        # df_routes.sort_index(axis=0, level=[0, 1], inplace=True)
        df_routes.sort_index(axis=1, level=[0, 1], inplace=True)

        return df_routes

    # Return cumulative orderbook
    def cumulative_book(ob):

        asks = ob['asks']
        bids = ob['bids']
        asks_p = [a[0] for a in asks]
        bids_p = [a[0] for a in bids]
        cum_a = list(accumulate([a[1] for a in asks]))
        cum_b = list(accumulate([a[1] for a in bids]))
        return [[bids_p[i], cum_b[i]] for i, a in enumerate(bids)], [[asks_p[i], cum_a[i]] for i, a in enumerate(asks)]

    # Return hedging capacity
    def get_hedge_capacity(id):

        # Get current synthetic cash
        hedge = dic_positions[id]['synthetic_cash'].sum()
        hedge_usd_margin = dic_positions[id]['hedge_margin_value'].sum()

        # Get target allocation of cash
        cash_target = dic_accounts[id].loc[account.exchange.get_stablecoins(), ('target', 'value')].mean()

        # Calculate hedge capacity
        capacity = cash_target - (hedge + hedge_usd_margin)

        log.info('Hedge capacity is {0}'.format(round(capacity), 0))

        return capacity

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

        # Select index of routes where market is a source, a gateway or a destination
        indexes_src = dic_routes[id].loc[(dic_routes[id].index.get_level_values('symbol_s') == symbol) & (
                dic_routes[id].index.get_level_values('wallet_s') == wallet)].index
        indexes_gat = dic_routes[id].loc[(dic_routes[id].index.get_level_values('symbol_g') == symbol) & (
                dic_routes[id].index.get_level_values('wallet_g') == wallet)].index
        indexes_dst = dic_routes[id].loc[(dic_routes[id].index.get_level_values('symbol_d') == symbol) & (
                dic_routes[id].index.get_level_values('wallet_d') == wallet)].index

        # Select row with it's indice and update columns of the dataframe
        def update_cost(position, value, code, depth, row):

            # Convert trade value to currency amount
            spot = dic_accounts[id].loc[(code, wallet), 'price'][0]
            quantity = value / spot

            # Get average price and distance from best ask (bid) for the desired quantity
            average_price, distance = get_price_n_distance(depth, quantity)
            spread = get_spread(bids, asks)
            cost = distance + spread

            # Favor (penalise) cost of a route if action is to open short (long)
            action = row[position]['action']
            if action in ['open_long', 'open_short']:
                funding = get_funding(base, quote, wallet, symbol)
                if funding:
                    if action == 'open_short':
                        cost -= funding * 10
                    else:
                        cost += funding * 10

                    dic_routes[id].loc[row.name, (position, 'funding')] = funding

            # Select delta quantity
            quantity_desired = dic_accounts[id].loc[(code, wallet), ('delta', 'quantity')]

            # Update columns
            dic_routes[id].loc[row.name, (position, 'quantity')] = quantity
            dic_routes[id].loc[row.name, (position, 'value')] = value
            dic_routes[id].loc[row.name, (position, 'distance')] = distance
            dic_routes[id].loc[row.name, (position, 'spread')] = spread
            dic_routes[id].loc[row.name, (position, 'cost')] = cost
            dic_routes[id].loc[row.name, (position, 'quantity %')] = abs(quantity / quantity_desired)

            # print(dic_routes[id].loc[idx, :])

        # Return funding rate
        def get_funding(base, quote, wallet, symbol):

            market = dic_markets[id].loc[(base, quote, wallet, symbol)]
            if market.index.get_level_values('derivative') == 'perpetual':
                funding = market['funding']['rate'][0]
                return funding

        # Return trade value and depth book
        def get_value_n_depth(location, route):

            action_src = route['source']['action']
            action_gat = route['gateway']['action']
            action_dst = route['destination']['action']

            currency, wallet, \
            base_s, quote_s, margin_s, symbol_s, wallet_s, \
            symbol_g, wallet_g, \
            base_d, quote_d, margin_d, symbol_d, wallet_d = [route.name[i] for i in range(14)]

            # Return depth side for an action
            def get_depth(action):

                if action in ['sell_base', 'close_long', 'open_short']:
                    return bids
                elif action in ['buy_base', 'open_long', 'close_short']:
                    return asks

            # Return desired value to trade
            def get_delta():

                # Select desired code and wallet in destination
                if action_dst in ['buy_base', 'open_long', 'open_short']:
                    code = base_d
                elif action_dst == 'sell_base':
                    code = quote_d

                # Return delta of the desired currency
                return abs(dic_accounts[id].loc[(code, wallet_d), ('delta', 'value')])

            # Return capacity of margin account
            def get_margin_account_capacity(code, wallet, extra):

                positions = dic_positions[id]
                positions_value = abs(
                    positions[positions.index.get_level_values('margined') == code]['total_value']).sum()

                # Select margin balance and determine total capacity based on leverage
                margin_balance = dic_accounts[id].loc[(code, wallet), ('wallet', 'total_value')]
                if pd.isna(margin_balance):
                    margin_balance = 0
                total_capacity = (margin_balance + extra) * float(account.leverage)

                # Determine available capacity
                available_capacity = total_capacity - positions_value
                capacity = max(available_capacity, 0)

                return capacity

            # Return value of available margin
            def get_free_margin(code, wallet):

                # Select initial margin and margin balance
                initial_margin = dic_accounts[id].loc[(code, wallet), ('wallet', 'used_value')]
                margin_balance = dic_accounts[id].loc[(code, wallet), ('wallet', 'total_value')]

                # Determine free margin
                free_margin = margin_balance - initial_margin * 2
                return max(free_margin, 0)

            # Return value of margin released when a position is closed
            def get_margin_released(code, wallet, value):

                positions = dic_positions[id]
                position = positions[(positions.index.get_level_values('code') == code) &
                                     (positions.index.get_level_values('wallet') == wallet)]

                initial_margin = position['initial_margin'][0]
                total_value = abs(position['total_value'][0])

                return initial_margin * value / total_value

            # Return trade value
            def get_trade_value():

                # First action is to close a position
                #####################################

                if route['route']['type'] == 'close position':

                    # Get delta and max value to close in source market
                    delta = abs(dic_accounts[id].loc[(base_s, wallet_s), ('delta', 'value')])
                    total = abs(dic_accounts[id].loc[(base_s, wallet_s), ('position', 'value')])
                    value = min(total, delta)

                    # Get margin released
                    margin_released = get_margin_released(base_s, wallet_s, value)

                    if not pd.isna(action_dst):

                        # Return max value if currency is traded in spot
                        if action_dst in ['buy_base', 'sell_base']:
                            return min(margin_released, get_delta())

                        else:

                            # Else get capacity of destination account
                            extra = margin_released if currency != margin_d else 0
                            capacity = get_margin_account_capacity(margin_d, wallet_d, extra)

                            # Determine max value
                            value_d = min(capacity, get_delta())
                            return min(value, value_d)

                    else:
                        return value

                # First action is to trade a currency in spot wallet
                ####################################################

                elif route['route']['type'] == 'spot':

                    # Get delta, balance and determine max value
                    delta = abs(dic_accounts[id].loc[(currency, wallet), ('delta', 'value')])
                    total = dic_accounts[id].loc[(currency, wallet), ('wallet', 'free_value')]
                    value = min(total, delta)

                    # Return max value if currency is traded in spot
                    if action_dst in ['buy_base', 'sell_base']:
                        return min(value, get_delta())

                    else:

                        # Else get capacity of destination account
                        extra = value if currency != margin_d else 0
                        capacity = get_margin_account_capacity(margin_d, wallet_d, extra)

                        # Determine max value
                        value_d = min(capacity, get_delta())
                        return min(value, value_d)

                # First action is to trade a free currency or use it as margin
                ##############################################################

                elif route['route']['type'] == 'margin':

                    # Get delta and value to trade with free margin
                    delta = abs(dic_accounts[id].loc[(currency, wallet), ('delta', 'value')])
                    free = get_free_margin(currency, wallet)
                    value = min(free, delta)

                    # Return max value if margin is sold in spot
                    if action_dst in ['buy_base', 'sell_base']:
                        return min(value, get_delta())

                    else:

                        # Else get capacity of destination account
                        extra = value if currency != margin_d else 0
                        capacity = get_margin_account_capacity(margin_d, wallet_d, extra)

                        # Determine max value
                        value_d = min(capacity, get_delta())

                        # pprint(dict(
                        #     code=currency,
                        #     wallet=wallet,
                        #     delta=delta,
                        #     free=free,
                        #     value=value,
                        #     extra=extra,
                        #     capacity=capacity,
                        #     value_d=value_d
                        # ))

                        return min(value, value_d)

                # First instruction is to close hedge
                #####################################

                elif route['route']['type'] == 'close hedge':

                    # Select position value and value to close
                    total = abs(dic_accounts[id].loc[(base_s, wallet_s), ('position', 'value')])
                    to_released = abs(get_hedge_capacity(id))
                    return min(to_released, total)

                else:
                    print(route)
                    log.error('Unknown route')
                    raise Exception

            # Get trade value
            value = get_trade_value()

            # Get the right side of the book (bids or asks)
            if location == 'source':
                depth = get_depth(action_src)
            if location == 'gateway':
                depth = get_depth(action_gat)
            if location == 'destination':
                depth = get_depth(action_dst)

            # Limit hedging to avoid lack of funds
            if action_dst == 'open_short':

                positions = dic_positions[id]
                if base_d in positions.index.get_level_values('code'):
                    if quote_d in positions.index.get_level_values('quote'):
                        if wallet_d in positions.index.get_level_values('wallet'):
                            if symbol_d in positions.index.get_level_values('symbol'):
                                position = positions.loc[(base_d, quote_d, wallet_d, symbol_d)]

                                # Select hedged value and determine threshold
                                # the position becomes a sell (not only a hedge)
                                hedge = position['synthetic_cash'][0] if position['hedge'][0] else 0
                                threshold = position['balance_value'][0] - hedge

                                # If threshold > 0 then coins aren't fully hedged
                                if threshold > 0:

                                    # Get hedging value allowed on the account
                                    hedge_capacity = get_hedge_capacity(id)

                                    # If threshold < hedge capacity then there is no limitation because
                                    # everything above threshold isn't hedge (synthetic cash) but sell
                                    if threshold < hedge_capacity:
                                        value = value

                                    # Else limit value
                                    else:
                                        log.warning('Limit value to hedge capacity')
                                        value = min(value, hedge_capacity)

                                # If threshold == 0 then shorted value is greater than coin balance,
                                # thus newly opened short position aren't hedge (synthetic cash) but sell
                                else:
                                    value = value

            return value, depth

        # Iterate through route's positions and update dataframe
        def update(location, route):

            # Iterate through routes
            for index, row in dic_routes[id].iterrows():
                if pd.Index(index).equals(pd.Index(route)):
                    # Get trade value and depth book and update dataframe
                    value, depth = get_value_n_depth(location, row)
                    update_cost(location, value, base, depth, row)

        # Iterate through indexes the market belong
        for route in indexes_src:
            update('source', route)
        for route in indexes_gat:
            update('gateway', route)
        for route in indexes_dst:
            update('destination', route)

        # Sum routes costs
        ##################

        # Return source, gateway and destination costs
        def get_global_cost(route, location):

            action = route[location]['action']

            if not pd.isna(action):
                if location in route.index.get_level_values('level_1'):
                    if 'cost' in route[location].index:
                        if not pd.isna(route[location]['cost']):
                            return route[location]['cost']
                else:
                    return np.nan
            else:
                return np.nan

        # Create column with global cost
        for index, route in dic_routes[id].iterrows():

            dic_routes[id].sort_index(axis=0, level=[0, 1], inplace=True)
            dic_routes[id].sort_index(axis=1, level=[0, 1], inplace=True)

            costs = [get_global_cost(route, position) for position in ['source', 'gateway', 'destination']]

            if all(costs):
                costs = [c for c in costs if not pd.isna(c)]
                cost = sum(costs)
                dic_routes[id].loc[index, ('route', 'cost')] = cost

        # Drop routes with value == 0
        dic_routes[id] = dic_routes[id][dic_routes[id]['destination']['value'] != 0]

        # print(dic_routes[id].to_string())

    # Update df_markets with order status after an order is placed
    def update_markets_df(id, orderid):

        order = Order.objects.get(orderid=orderid)

        # Log order status
        if order.status == 'closed':
            log.info('Order is filled')
        elif order.status == 'open':
            log.info('Order is pending')

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
        dic_markets[id].loc[idx, ('order', 'id')] = order.id
        dic_markets[id].loc[idx, ('order', 'type')] = order.route_type
        dic_markets[id].loc[idx, ('order', 'amount')] = order.amount
        dic_markets[id].loc[idx, ('order', 'status')] = order.status
        dic_markets[id].loc[idx, ('order', 'filled')] = order.filled

        # print(dic_markets[id].to_string())

    # Update the latest fund object after a trade is executed
    def update_fund_object(account_id, orderids):

        log.info('Update fund object')

        # Select wallet of markets where trades occurred
        wallets = list(set([order.market.default_type for order in Order.objects.filter(orderid__in=orderids)]))

        if wallets:
            for wallet in wallets:
                create_fund.run(account_id, wallet=wallet)
        else:
            create_fund.run(account_id, wallet='default')

        log.info('Update fund object done')

    # Update df_account free_value after a transfer
    def update_account_free_value(id, index, route):

        log.info('Update wallets balance')
        # Get coin and source wallets
        code = index[0]
        from_wallet = index[1]

        # Get destination market
        if not pd.isna(route['gateway']['action']):
            to_wallet = index[8]
            to = 'gateway'
        elif not pd.isna(route['destination']['action']):
            to_wallet = index[13]
            to = 'destination'

        quantity = route[to]['quantity']
        value = route[to]['value']

        dic_accounts[id].loc[(code, from_wallet)]['wallet']['free_value'] -= value  # source wallet
        dic_accounts[id].loc[(code, from_wallet)]['wallet']['free_quantity'] -= quantity  # source wallet

        dic_accounts[id].loc[(code, to_wallet)]['wallet']['free_value'] += value  # destination wallet
        dic_accounts[id].loc[(code, to_wallet)]['wallet']['free_quantity'] += quantity  # destination wallet

        # print('\n', dic_accounts[id].to_string())

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

                log.info('Open orders successfully updated')

                # Return a list of ids for orders with new trade
                res = result.get(disable_sync_subtasks=False)
                orderids = [orderid for orderid in res if orderid is not None]
                return orderids

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
            return False

        if any(pd.isna(dic_routes[id]['route']['cost'].array)):
            print('No route cost')
            return False

        # Return True if an order is open
        def is_order(index, route):

            # Create filter for market
            if not pd.isna(route['source']['action']):
                value = index[2]
                level = 'base'

            elif not pd.isna(route['gateway']['action']):
                value = index[7]
                level = 'symbol'

            elif not pd.isna(route['destination']['action']):
                value = index[9]
                level = 'base'

            # Select markets which use this route and check orders status
            markets = dic_markets[id].xs(value, level=level, axis=0)
            status = list(markets['order']['status'])

            # Abort trade execution if an order is open or closed for this code
            if 'open' in status:
                log.info('Order is pending')
                return True

            elif 'closed' in status:
                log.info('Order closed')
                return True

            else:
                return False

        # Return trade data
        def get_data(index, route):

            # Create a list with symbol to trade and wallet
            if not pd.isna(route['source']['action']):
                symbol = index[5]
                wallet = index[6]
                location = 'source'

            elif not pd.isna(route['gateway']['action']):
                symbol = index[7]
                wallet = index[8]
                location = 'gateway'

            elif not pd.isna(route['destination']['action']):
                symbol = index[12]
                wallet = index[13]
                location = 'destination'

            quantity = route[location]['quantity']
            value = route[location]['value']
            action = route[location]['action']
            side = 'buy' if action in ['open_long', 'close_short', 'buy_base'] else 'sell'

            return symbol, wallet, quantity, value, action, side

        # Convert currency to contract if necessary
        def convert(market, quantity):
            return methods.amount_to_contract(market, quantity) if market.type == 'derivative' else quantity

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

            # Test condition for min_notional
            min_notional = methods.limit_cost(market, amount, price)

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
                            if instruction not in ['close_long', 'close_short']:
                                log.info('Set REDUCE_ONLY = True')
                                return True, dict(reduceonly=True)  # Dic of trade parameters
                else:
                    return True, None
            else:
                if min_notional:
                    return True, None

            # In last resort return False
            return False, None

        def log_trade(market, amount, quantity):

            desired = round(quantity, 4)
            if market.type == 'derivative':
                equivalent = round(methods.contract_to_amount(market, amount), 4)
                log.info(
                    'Trade {0} contracts at {1}, eq. {2} {3}, desired quantity was {4}'.format(
                        amount,
                        market.symbol,
                        equivalent,
                        market.base,
                        desired
                    ))
            else:
                log.info(
                    'Trade {0} {1}, desired quantity was {2}'.format(
                        round(amount, 4),
                        market.symbol,
                        desired
                    ))

        # Execute trade logic
        #####################

        log.info('Trading')

        # Sort routes by cost and reorder columns
        dic_routes[id] = dic_routes[id].sort_values([('route', 'cost')], ascending=True)
        dic_routes[id] = dic_routes[id].sort_index(axis=1)

        # Move close hedge first
        indexes = dic_routes[id].loc[dic_routes[id]['route']['type'] == 'close hedge'].index

        if not indexes.empty:
            log.info('Move close hedge first')
            print(indexes)
            idx = indexes[0]
            print(idx)
            dic_routes[id] = pd.concat([dic_routes[id].loc[idx], dic_routes[id].drop(idx)], axis=0)

        print('\n')
        print(dic_accounts[account.id].to_string())
        print('\n')
        print(dic_routes[account.id].to_string())
        print('\n')
        print(dic_markets[account.id].to_string())
        print('\n')

        # Loop through the best routes
        for index, route in dic_routes[id].iterrows():

            log.info('Route {0}'.format(route['route']['id']))

            # Check open order
            ##################

            if is_order(index, route):
                log.warning('an order is already open')
                continue

            else:

                # Verify data and trade
                #######################

                symbol, wallet, desired_quantity, value, instruction, side = get_data(index, route)
                market = Market.objects.get(exchange=exchange, symbol=symbol, default_type=wallet)
                price = get_price(market, side)

                # Transfer funds
                ################

                if route['route']['transfer']:
                    if transfer(account.id, index, route):
                        update_account_free_value(account.id, index, route)
                    else:
                        continue

                if account.limit_order:

                    # Format price decimal
                    price = methods.format_decimal(counting_mode=exchange.precision_mode,
                                                   precision=market.precision['price'],
                                                   n=price
                                                   )
                    # Check price conditions
                    if not methods.limit_price(market, price):
                        continue

                # Convert quantity to contract (if derivative) and format decimal
                amount = methods.format_decimal(counting_mode=exchange.precision_mode,
                                                precision=market.precision['amount'],
                                                n=convert(market, desired_quantity)
                                                )
                # Check amount conditions
                if methods.limit_amount(market, amount):

                    # Check cost condition
                    min_notional, params = check_min_notional(market, instruction, amount, price)
                    if min_notional:

                        order = dict(
                            market=market,
                            instruction=instruction,
                            side=side,
                            amount=amount,
                            price=price,
                            params=params
                        )

                        # Create an order object
                        pk = account.create_order(**order)
                        if pk:

                            log_trade(market, amount, desired_quantity)

                            # Place order
                            response = place_order.run(account.id, pk)
                            if response:

                                print(route)

                                # Update order object
                                methods.order_create_update(account, response)

                                # Update dataframes
                                update_markets_df(account.id, response['id'])

                                log.info('Trade complete')
                                return True

                            else:
                                log.warning('No response from exchange')
                                continue
                        else:
                            log.warning('No primary key received')
                            continue
                    else:
                        log.warning('MIN_NOTIONAL failed')
                        continue
                else:
                    log.warning('Limit amount failed')
                    continue

        log.info('Rebalance complete')

    # Create dictionaries
    def create_dictionaries(account_id):

        # Start by updating positions
        update_positions.run(account_id)
        dic_positions[account_id] = account.create_df_positions()

        # Then create account dataframe
        create_fund.run(account_id)
        dic_accounts[account_id] = account.create_df_account()

        # Create markets dataframe
        dic_markets[account_id] = create_df_markets()

        # Finally create routes
        dic_routes[account_id] = create_routes(account_id,
                                               dic_accounts[account_id],
                                               dic_positions[account_id]
                                               )

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

                            # Place an order to the best route
                            if trade(account):

                                # Update objects of open orders and return a list if trade detected
                                orderids = update_orders(account)

                                if orderids:
                                    log.info('Trades detected')
                                    print(orderids)

                                    # Update df_markets
                                    [update_markets_df(account.id, orderid) for orderid in orderids]

                                    # Update df_positions if a trade occurred on a derivative market
                                    update_positions.run(account.id, orderids)

                                    # Update the latest fund object and df_account
                                    update_fund_object(account.id, orderids)

                                # Construct new dataframes
                                dic_accounts[account.id], dic_positions[account.id], dic_markets[account.id], \
                                dic_routes[account.id] = create_dictionaries(account.id)

                            else:
                                log.warning('Trade return False')
                else:
                    print('wait')

                await client.sleep(15000)

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

    # Get codes to monitor
    ######################

    allocations_new = [strategy.get_allocations()]
    allocations_old = [strategy.get_allocations(n=strategy.get_offset())]

    codes_new = sum([list(l[list(l.keys())[0]].keys()) for l in allocations_new], [])
    codes_old = sum([list(l[list(l.keys())[0]].keys()) for l in allocations_old], [])
    codes = list(set(codes_old + codes_new))

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
