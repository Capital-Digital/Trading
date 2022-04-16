from __future__ import absolute_import, unicode_literals

import sys
import asyncio
import time
import traceback
from itertools import accumulate
from pprint import pprint
from django.db.models.query import QuerySet
from django.db.models import Q
import warnings
import ccxt
import numpy as np
import pandas as pd
import logging
import structlog
from structlog.processors import format_exc_info
from celery import chain, group, shared_task, Task
from capital.celery import app
from django.core.exceptions import ObjectDoesNotExist
from timeit import default_timer as timer

from billiard.process import current_process
from capital.celery import app

from capital.error import *
from capital.methods import *
from marketsdata.models import Market, Currency, Exchange
from strategy.models import Strategy
from trading.methods import *
from trading.models import Account, Order, Fund, Position, Transfer
import threading
import random

log = structlog.get_logger(__name__)
warnings.simplefilter(action='ignore', category=FutureWarning)


log.info('THREAD {0}'.format(threading.active_count()))


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


# Check all accounts credentials
@app.task(name='Trading_____Check accounts credentials')
def check_accounts_cred():
    for exchange in Exchange.objects.all():
        for account in Account.objects.filter(exchange=exchange):
            check_account_cred.delay(account.id)


# Check an account credential
@app.task(base=BaseTaskWithRetry, name='Trading_____Check account credentials')
def check_account_cred(account_id):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    log.bind(user=account.name)

    try:
        # Check credentials
        client.checkRequiredCredentials()

    except ccxt.AuthenticationError as e:
        account.valid_credentials = False
        log.warning('Account credentials are invalid')

    except Exception as e:
        log.warning("Account credential can't be checked: {0}".format(e))
        account.valid_credentials = False

    else:
        account.valid_credentials = True
        log.info('Account credentials are valid')

    finally:
        account.save()
        log.unbind('user')

###############################################


@app.task(name='Trading_Rebalance_accounts')
def rebalance_all(strategy_id):
    accounts = Account.objects.filter(strategy__id=strategy_id, active=True)
    for account in accounts:
        rebalance.delay(account.id)


@app.task(name='Trading_Rebalance_account')
def rebalance(account_id, sell_close=True):
    #
    account = Account.objects.get(id=account_id)

    log.info('')
    log.info('Rebalance account', account=account.name)
    log.info('*****************')

    # Calculate new delta
    account.get_delta()

    if sell_close:

        # Liberate resources
        account.sell_spot_all()
        account.close_short_all()

    # Determine available and desired resources
    bal_spot = account.balance.spot.free[account.quote].value
    bal_futu = account.balance.future.free[account.quote].value
    des_spot = account.to_buy_spot_value().sum()
    des_futu = account.to_open_short_value().sum()

    # Determine resource to transfer between account
    need_spot = max(0, des_spot - bal_spot)
    need_futu = max(0, des_futu - bal_futu)

    # Transfer
    if need_spot:
        free_futu = max(0, bal_futu - des_futu)
        transfer.delay(account_id, 'future', 'spot', free_futu)
    if need_futu:
        free_spot = max(0, bal_spot - des_spot)
        transfer.delay(account_id, 'spot', 'future', free_spot)

    # Determine account to allocate resource first
    spot = min(bal_spot, des_spot)
    futu = min(bal_futu, des_futu)

    log.info('')
    log.info('Allocate resources')
    log.info('******************')

    # if spot > futu:
    #     account.buy_spot_all()
    #     account.open_short_all()
    # else:
    #     account.open_short_all()
    #     account.buy_spot_all()

# Transfer
##########


# Move resource between accounts
@app.task(base=BaseTaskWithRetry, name='Trading_transfer')
def transfer(account_id, source, dest, quantity):
    #
    account = Account.objects.get(idd=account_id)
    client = account.exchange.get_ccxt_client(account)

    log.info('')
    log.info('Transfer')
    log.info('********')
    log.info('From {0} to {1}'.format(source, dest))
    log.info('-> {0} {1}'.format(round(quantity, 1), account.quote))

    return client.transfer(account.quote, quantity, source, dest)

# Place
#######


# Place a new order and return exchange response as signal
@app.task(base=BaseTaskWithRetry, name='Trading_place_order')
def place_order(account_id, action, code, clientid, order_type, price, reduce_only, side, size, symbol, wallet):
    #
    log.info(' ')
    log.info('Place order')
    log.info('***********')
    log.info('symbol {0}'.format(symbol))
    log.info('side {0}'.format(side))
    log.info('size {0}'.format(size))
    log.info('market {0}'.format(wallet))
    log.info('clientid {0}'.format(clientid))

    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    client.options['defaultType'] = wallet

    kwargs = dict(
        symbol=symbol,
        type=order_type,
        side=side,
        amount=size,
        price=price,
        params=dict(newClientOrderId=clientid)
    )

    # Set parameters
    if reduce_only:
        kwargs['params']['reduceOnly'] = True

    return client.create_order(**kwargs)


# Market sell spot account
@app.task(base=BaseTaskWithRetry, name='Trading_market_sell')
def market_sell(account_id):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    for code, amount in account.balances.spot.free.quantity.T.items():
        if code != account.quote:
            if not np.isnan(amount):

                log.info('Sell {0}'.format(code))

                # Construct symbol
                symbol = code + '/USDT'
                kwargs = dict(
                    symbol=symbol,
                    type='market',
                    side='sell',
                    amount=abs(amount)
                )

                response = client.create_order(**kwargs)
                log.info('Order status {0}'.format(response['status']))

    account.create_balances()


# Market close position
@app.task(base=BaseTaskWithRetry, name='Trading_market_close')
def market_close(account_id):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    client.options['defaultType'] = 'future'

    for code, value in account.balances.position.open.T.items():
        size = value['quantity']
        if not np.isnan(size):

            log.info('Close position {0}'.format(code))

            # Construct symbol
            symbol = code + '/USDT'
            if value['side'] == 'buy':
                side = 'sell'
            else:
                side= 'buy'

            kwargs = dict(
                symbol=symbol,
                type='market',
                side=side,
                amount=abs(size)
            )

            response = client.create_order(**kwargs)
            log.info('Order status {0}'.format(response['status']))

            account.create_balances()

# Update
#########


# Update open orders of all accounts (run periodically)
@app.task(name='Trading_____Update_accounts_orders')
def update_accounts_orders():
    #
    for account in Account.objects.filter(active=True, exchange__exid='binance', name='Principal'):
        update_account_orders.delay(account.id)


# Update all open orders of an account
@app.task(name='Trading_____Update_account_orders')
def update_account_orders(account_id):
    #
    account = Account.objects.get(id=account_id)

    orders = Order.objects.filter(account=account,
                                  status__in=['new', 'partially_filled']
                                  ).exclude(orderid__isnull=True)

    if orders.exists():
        for order in orders:
            fetch_order.delay(account_id, order.orderid)


# Fetch an open order by its orderid (then update corresponding object)
@app.task(name='Trading_____Fetch_order', base=BaseTaskWithRetry)
def fetch_order(account_id, order_id):
    #
    order = Order.objects.get(orderid=order_id)
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    log.info('Fetch order {0}'.format(order.clientid))

    # Set options
    client.options['defaultType'] = order.market.wallet
    responses = client.fetchOrder(id=order.orderid, symbol=order.market.symbol)
    update_order.delay(account_id, responses)


# Update an order object (then update the account balance)
@app.task(name='Trading_____Update_order')
def update_order(account_id, response):
    #
    account = Account.objects.get(id=account_id)
    filled_new = 0

    log.info(' ')
    log.info('Update object')
    log.info('************')

    try:

        orderid = response['id']
        clientid = response['info']['clientOrderId']
        status = response['info']['status']
        filled_total = response['filled']

        # Select order and update its status
        order = Order.objects.get(account=account, clientid=clientid)
        order.status = status.lower()
        order.orderid = orderid

        # Select attributes
        code = order.market.base.code
        wallet = order.market.wallet
        action = order.action

        log.info('clientid {0}'.format(clientid))
        log.info('code {0}'.format(code))
        log.info('wallet {0}'.format(wallet))
        log.info('status {0}'.format(status))
        log.info('action {0}'.format(action))
        log.info('filled total {0}'.format(filled_total))

        if filled_total:

            # Determine trade quantity since last update an update object
            filled_new = filled_total - order.filled
            order.filled = filled_total

            log.info('filled new {0}'.format(filled_new))

    except Exception as e:
        log.error('Exception {0}'.format(e.__class__.__name__))
        log.error('Exception {0}'.format(e))

    else:
        order.response = response
        order.save()

        return account_id, action, filled_new


# Cancellation
##############


# Cancel orders of all accounts
@app.task(name='Trading_____Cancel_accounts_orders')
def cancel_accounts_orders():
    #
    for account in Account.objects.filter(active=True, exchange__exid='binance', name='Principal'):
        cancel_account_orders.delay(account.id)


# Filter open orders from an account and cancel
@app.task(name='Trading_____Cancel_account_orders')
def cancel_account_orders(account_id):
    #
    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account, status__in=['new', 'partially_filled']
                                  ).exclude(orderid__isnull=True)
    if orders.exists():
        log.info('Cancel {0} orders(s)'.format(orders.count()))
        for order in orders:
            cancel_order.delay(account_id, order.orderid)
    else:
        log.info('Open order not found')


# Cancel an order by its orderid
@app.task(name='Trading_____Cancel_order', base=BaseTaskWithRetry)
def cancel_order(account_id, order_id):
    #
    order = Order.objects.get(orderid=order_id)
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    # Set options
    client.options['defaultType'] = order.market.wallet

    try:
        response = client.cancel_order(id=order_id, symbol=order.market.symbol)

    except ccxt.OrderNotFound:
        log.error('Unable to cancel order {0}'.format(order.clientid))
        order.status = 'not_found'

    else:
        # Update object with new status
        status = response['info']['status']
        filled_total = response['filled']
        order.filled = filled_total
        order.status = status.lower()

        if status == 'CANCELED':
            log.info('Cancel order {0}'.format(order.clientid))
        else:
            log.warning('Unable to cancel order {0}, order status is now {1}'.format(order.clientid, status))

    finally:
        order.response = response
        order.save()


##################################################################

# Update orders
@app.task(name='Trading_____Update orders', base=BaseTaskWithRetry)
def update_orders():
    # Iterate through accounts and update open orders
    for account in Account.objects.filter(active=True, exchange__exid='binance'):

        if not account.trading:

            # Continue trading if new trade is detected
            new_trade = account.fetch_open_orders()
            if new_trade:
                if not account.trading:
                    log.info('Trade detected, continue account rebalancing', account=account.name)
                    account.trade(cancel=False)

        else:
            log.warning('Account is already trading', account=account.name)

