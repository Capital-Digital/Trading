from __future__ import absolute_import, unicode_literals
import traceback
import sys
import asyncio
import time
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
from celery import chain, group, chord, shared_task, Task
from capital.celery import app
from django.core.exceptions import ObjectDoesNotExist
from timeit import default_timer as timer
from billiard.process import current_process
from capital.celery import app
from capital.error import *
from capital.methods import *
from marketsdata.models import Market, Currency, Exchange
from trading.methods import *
from trading.models import Account, Order
import threading
import random

log = structlog.get_logger(__name__)

# warnings.simplefilter(action='ignore', category=FutureWarning)


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

# Bulk actions
##############


# Cancel orders and query assets quantity and open positions
@app.task(name='Trading_____Bulk_prepare_accounts')
def bulk_prepare_accounts():
    for exchange in Exchange.objects.all():
        for account in Account.objects.filter(exchange=exchange, active=True):
            prepare_accounts.delay(account.id)


# Bulk rebalance assets of all accounts
@app.task(name='Trading_Bulk_rebalance_accounts')
def bulk_rebalance(strategy_id):
    accounts = Account.objects.filter(strategy__id=strategy_id, active=True)
    for account in accounts:
        rebalance.delay(account.id)


# Bulk update open orders of all accounts
@app.task(name='Trading_____Bulk_update_orders')
def bulk_update_orders():
    #
    for account in Account.objects.filter(active=True, exchange__exid='binance', name='Principal'):
        update_orders.delay(account.id)


# Check credentials of all accounts
@app.task(name='Trading_____Bulk_check_credentials')
def bulk_check_credentials():
    for exchange in Exchange.objects.all():
        for account in Account.objects.filter(exchange=exchange):
            check_credentials.delay(account.id)


# Account specific actions
##########################

# Cancel orders and query assets quantity/positions
@app.task(name='Trading_____Prepare_accounts')
def prepare_accounts(account_id):
    #
    log.bind(worker=current_process().index)
    log.info('Prepare accounts')

    account = Account.objects.get(id=account_id)
    chord(cancel_orders.s(account.id))(create_balances.s())

    log.unbind('worker')


# Cancel open orders of an account
@app.task(name='Trading_____Cancel_orders')
def cancel_orders(account_id):
    #
    log.bin(worker=current_process().index)
    log.info('Cancel orders')

    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account,
                                  status__in=['new', 'partially_filled']
                                  ).exclude(orderid__isnull=True)
    if orders.exists():
        for order in orders:
            send_cancel_order.delay(account_id, order.orderid)

    log.info('Cancel orders complete')
    log.unbind('worker')

    return account_id


# Create balances dataframe
@app.task(name='Trading_____Create_balances')
def create_balances(account_id):
    #
    log.bin(worker=current_process().index)
    log.info('Create balances')

    if isinstance(account_id, list):
        account_id = account_id[0]

    account = Account.objects.get(id=account_id)

    # Fetch account
    account.get_assets_balances()
    account.get_open_positions()

    # Fetch prices
    account.get_spot_prices()
    account.get_futu_prices()

    # Calculate assets value
    account.calculate_balances_value()

    log.info('Create balances complete')
    log.unbind('worker')


# Rebalance fund of an account
@app.task(name='Trading_____Rebalance_account')
def rebalance(account_id, get_balances=False, release=True):
    #
    account = Account.objects.get(id=account_id)
    log.info('Rebalance ({0})'.format(account.name), worker=current_process().index)

    if get_balances:
        create_balances(account_id)

    else:
        # Wait balances is updated
        while not account.is_fresh_balances():
            pass

    log.info('')
    log.info('Rebalance account', account=account.name)
    log.info('*****************')

    # Update prices
    account.get_spot_prices()
    account.get_futu_prices()

    # Re-calculate assets value
    account.calculate_balances_value()

    # Calculate new delta
    account.get_target()
    account.get_delta()

    # Display account percent
    current = account.balances.account.current.percent
    for coin, val in current[current != 0].sort_values(ascending=False).items():
        log.info('Percentage for {0}: {1}%'.format(coin, round(val * 100, 1)))

    log.info(' ')

    # Display target percent
    target = account.balances.account.target.percent
    for coin, val in target[target != 0].sort_values(ascending=False).items():
        log.info('Target for {0}: {1}%'.format(coin, round(val * 100, 1)))

    if release:

        # Release resources
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
@app.task(name='Trading_____Update_orders')
def update_orders(account_id):
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


# Check an account credential
@app.task(base=BaseTaskWithRetry, name='Trading_____Check_credentials')
def check_credentials(account_id):
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


# REST API
##########


# Send create order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_create_order')
def send_create_order(account_id, action, code, clientid, order_type, price, reduce_only, side, size, symbol, wallet,
                      then_rebalance=True):
    #
    log.info(' ')
    log.info('Place order', worker=current_process().index)
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

    if order_type == 'market':
        del kwargs['price']

    # Set parameters
    if reduce_only:
        kwargs['params']['reduceOnly'] = True

    pprint(kwargs)

    try:
        response = client.create_order(**kwargs)

    except ccxt.InsufficientFunds as e:

        order = Order.objects.get(clientid=clientid)
        order.status = 'canceled'
        order.response = dict(exception=str(e))
        order.save()

        log.info('')
        log.info(e)
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
        qty_filled = account.update_order_object(wallet, response)

        print(qty_filled)
        log.info('filled {0}'.format(qty_filled))

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
    qty_filled = account.update_order_object(order.wallet, response)
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

            pprint(dic)
            # Update corresponding order object
            qty_filled = account.update_order_object(wallet, dic)
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
        qty_filled = account.update_order_object(order.wallet, response)
        account.update_balances(order.action,
                                order.wallet,
                                order.market.base.code,
                                qty_filled
                                )