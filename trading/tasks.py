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
def rebalance(account_id):
    #
    account = Account.objects.get(id=account_id)
    log.info('Rebalance account', account=account.name)

    # account.create_balances()
    account.sell_spot_all()
    account.close_short_all()
    account.buy_spot_all()
    account.open_short_all()


# Place
#######


# Place a new order and return exchange response as signal
@app.task(base=BaseTaskWithRetry, name='Trading_place_order')
def place_order(account_id, action, code, clientid, order_type, price, reduce_only, side, size, symbol, wallet):
    #
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

    log.info(' ')
    log.info(' *** PLACE ORDER ***')
    log.info('Symbol {0}'.format(symbol))
    log.info('order_type {0}'.format(order_type))
    log.info('side {0}'.format(side))
    log.info('size {0}'.format(size))
    log.info('clientid {0}'.format(clientid))
    log.info(' ')

    return client.create_order(**kwargs)


# Close position
@app.task(base=BaseTaskWithRetry, name='Trading_place_order')
def close_position_market(account_id):
    #
    account = Account.objects.get(id=account_id)
    positions = account.balances.position.open
    for code, value in positions.T.items():
        size = value['quantity']
        if not np.isnan(size):

            log.info('Close position {0}'.format(code))

            # Construct symbol
            symbol = code + '/USDT'

            # Determine action
            side = value['side']
            if side == 'buy':
                action = 'close_long'
            elif side == 'sell':
                action = 'close_short'

            alphanumeric = 'abcdefghijklmnopqrstuvwABCDEFGHIJKLMNOPQRSTUVWWXYZ01234689'
            clientid = ''.join((random.choice(alphanumeric)) for x in range(5))
            place_order.delay(account_id, action, code, clientid, 'market', None, False, side, size, symbol, 'future')


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

    try:

        orderid = response['id']
        clientid = response['info']['clientOrderId']
        status = response['info']['status']
        filled_total = response['filled']

        log.info('Update order {0}'.format(clientid))

        # Select order and update its status
        order = Order.objects.get(account=account, clientid=clientid)
        order.status = status.lower()
        order.orderid = orderid

        # Select attributes
        code = order.market.base.code
        wallet = order.market.wallet
        action = order.action

        # log.info(' ')
        # log.info('  ***  UPDATE *** ')
        # log.info('code {0}'.format(code))
        # log.info('wallet {0}'.format(wallet))
        # log.info('status {0}'.format(status))
        # log.info('clientid {0}'.format(clientid))
        # log.info('action {0}'.format(action))
        # log.info('filled {0}'.format(filled))
        # log.info(' ')

        if filled_total:

            # Determine trade quantity since last update an update object
            filled_new = filled_total - order.filled
            order.filled = filled_total

            # Update balances dataframe
            account.update_balances(action, wallet, code, filled_new)

    except Exception as e:
        log.error('Exception {0}'.format(e.__class__.__name__))
        log.error('Exception {0}'.format(e))

    else:
        order.response = response
        order.save()

        # Return account ID to signal new available resources
        if action in ['sell_spot', 'close_short']:
            if filled_new:
                return account_id


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
        for order in orders:
            cancel_order.delay(account_id, order.orderid)


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

