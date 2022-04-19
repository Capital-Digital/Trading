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
from trading.models import Account, Order
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


# Update all accounts
@app.task(bind=True, name='Update_accounts')
def update_accounts(self, strategy_name):
    from trading.models import Account
    accounts = Account.objects.filter(strategy__name=strategy_name, active=True)
    for account in accounts:
        update_account.delay(account.id)


# Update an account
@app.task(bind=True, base=BaseTaskWithRetry, name='Update_account')
def update_account(self, account_id):
    from trading.models import Account
    account = Account.objects.get(id=account_id)

    log.info('Account update', name=account.name, process=current_process().index)

    account.trade()


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

# Bulk actions
##############


# Bulk rebalance holding of all accounts
@app.task(name='Trading_Rebalance_accounts')
def rebalance_all(strategy_id):
    accounts = Account.objects.filter(strategy__id=strategy_id, active=True)
    for account in accounts:
        rebalance.delay(account.id)


# Bulk update open orders of all accounts
@app.task(name='Trading_____Update_accounts_orders')
def update_accounts_orders():
    #
    for account in Account.objects.filter(active=True, exchange__exid='binance', name='Principal'):
        update_account_orders.delay(account.id)


# Bulk cancel orders of all accounts
@app.task(name='Trading_____Cancel_accounts_orders')
def cancel_accounts_orders():
    #
    for account in Account.objects.filter(active=True, exchange__exid='binance', name='Principal'):
        cancel_account_orders.delay(account.id)


# Account specific actions
##########################


# Rebalance fund of an account
@app.task(name='Trading_____Rebalance_account')
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
    bal_spot = account.balances.spot.free.value[account.quote]
    bal_futu = account.balances.future.free.value[account.quote]
    des_spot = account.to_buy_spot_value().sum()
    des_futu = account.to_open_short_value().sum()

    # Determine resource to transfer between account
    need_spot = max(0, des_spot - bal_spot)
    need_futu = max(0, des_futu - bal_futu)

    log.info(' ')
    log.info('Transfer resources between accounts')
    log.info('***********************************')

    # Transfer
    if need_spot:
        free_futu = max(0, bal_futu - des_futu)
        log.info('Resources are needed in spot')
        log.info('Move {0} {1} from future'.format(round(free_futu, 1), account.quote))
        send_transfer.delay(account_id, 'future', 'spot', free_futu)

    if need_futu:
        free_spot = max(0, bal_spot - des_spot)
        log.info('Resources are needed in future')
        log.info('Move {0} {1} from spot'.format(round(free_futu, 1), account.quote))
        send_transfer.delay(account_id, 'spot', 'future', free_spot)

    # Determine account to allocate resource first
    spot = min(bal_spot, des_spot)
    futu = min(bal_futu, des_futu)

    log.info('')
    log.info('Allocate resources')
    log.info('******************')

    if spot > futu:
        account.buy_spot_all()
        account.open_short_all()
    else:
        account.open_short_all()
        account.buy_spot_all()


# Update open orders of an account
@app.task(name='Trading_____Update_account_orders')
def update_account_orders(account_id):
    #
    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account,
                                  status__in=['new', 'partially_filled']
                                  ).exclude(orderid__isnull=True)

    if orders.exists():

        log.info(' ')
        log.info('Update {0} order(s)'.format(orders.count()))
        log.info('********************')

        for order in orders:
            send_fetch_orderid.delay(account_id, order.orderid)
    else:
        log.info('No order to update')


# Cancel open orders of an account
@app.task(name='Trading_____Cancel_account_orders')
def cancel_account_orders(account_id):
    #
    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account, status__in=['new', 'partially_filled']
                                  ).exclude(orderid__isnull=True)
    if orders.exists():
        log.info('Cancel {0} orders(s)'.format(orders.count()))
        for order in orders:
            send_cancel_order.delay(account_id, order.orderid)
    else:
        log.info('Open order not found')


# Send
######


# Send create order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_create_order')
def send_create_order(account_id, action, code, clientid, order_type, price, reduce_only, side, size, symbol, wallet,
                      then_rebalance=True):
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

    try:
        response = client.create_order(**kwargs)

    except Exception as e:

        log.info('')
        log.error('Order placement failed')
        log.info('code {0}'.format(code))
        log.info('action {0}'.format(action))
        log.info('clientid {0}'.format(clientid))
        log.info('size {0}'.format(size))
        log.info('wallet {0}'.format(wallet))

    else:

        log.info('')
        log.info('Order placement success')
        log.info('code {0}'.format(code))
        log.info('action {0}'.format(action))
        log.info('clientid {0}'.format(clientid))
        log.info('size {0}'.format(size))
        log.info('wallet {0}'.format(wallet))

        # Update object and dataframe
        qty_filled = account.update_order_object.delay(wallet, response)
        account.update_balances(action, wallet, code, qty_filled)

        if then_rebalance:
            return account_id, qty_filled


# Send fetch orderid
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_fetch_orderid')
def send_fetch_orderid(account_id, order_id):
    #
    order = Order.objects.get(orderid=order_id)
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    log.info('Update order {0}'.format(order.clientid))

    # Set options
    client.options['defaultType'] = order.market.wallet
    response = client.fetchOrder(id=order.orderid, symbol=order.market.symbol)

    # Update object and dataframe
    qty_filled = account.update_order_object.delay(order.wallet, response)
    account.update_balances(order.action,
                            order.wallet,
                            order.market.base.code,
                            qty_filled
                            )

    return account_id, qty_filled


# Send fetch all open orders
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_fetch_all_open_orders')
def send_fetch_all_open_orders(account_id):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    # Iterate through wallets
    for wallet in account.exchange.get_wallets():

        client.options['defaultType'] = wallet
        client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

        response = client.fetchOpenOrders()

        qty = []
        for dic in response:

            # Update corresponding order object
            qty_filled = account.update_order_object.delay(wallet, dic)
            qty.append(qty_filled)

        return account_id, qty


# Send transfer order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_transfer_funds')
def send_transfer(account_id, source, dest, quantity):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    log.info('')
    log.info('Transfer')
    log.info('********')
    log.info('From {0} to {1}'.format(source, dest))
    log.info('-> {0} {1}'.format(round(quantity, 1), account.quote))

    try:
        client.transfer(account.quote, quantity, source, dest)

    except Exception as e:
        pass

    else:

        # Update dataframe
        account.update_balances_after_transfer(source, dest, quantity)

        return account_id, quantity


# Send cancellation order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_cancel_order')
def send_cancel_order(account_id, order_id):
    #
    order = Order.objects.get(orderid=order_id)
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    client.options['defaultType'] = order.market.wallet

    try:
        response = client.cancel_order(id=order_id, symbol=order.market.symbol)

    except Exception as e:
        log.error('Order canceled failure')

    else:
        log.info('Order canceled successful')

        # Update object and dataframe
        qty_filled = account.update_order_object.delay(order.wallet, response)
        account.update_balances(order.action,
                                order.wallet,
                                order.market.base.code,
                                qty_filled
                                )