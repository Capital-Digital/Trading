from __future__ import absolute_import, unicode_literals
import traceback
import sys
import asyncio
import json
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
from trading.models import Account, Order, Fund, Position
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
    log.bind(worker=current_process().index)
    log.info('Cancel orders')

    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account,
                                  status__in=['new', 'partially_filled', 'open']
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
    log.bind(worker=current_process().index)

    log.info('')
    log.info('Create balances...')
    log.info('')

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
    account.calculate_assets_value()

    log.unbind('worker')


# Update funds object
@app.task(name='Trading_____Update_funds_object')
def update_funds_object(account_id):

    account = Account.objects.get(id=account_id)

    try:
        fund = Fund.objects.get(account=account, exchange=account.exchange)

    except ObjectDoesNotExist:
        fund = Fund.objects.create(account=account, exchange=account.exchange)

    finally:

        dt = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        now = dt.strftime(datetime_directive_ISO_8601)

        d = dict()

        if now not in d.keys():
            d[now] = dict()

        for w in ['spot', 'future', 'position']:
            for tp in ['total', 'free', 'used', 'open']:
                for i in ['quantity', 'value']:

                    # Code exists ?
                    for c in account.balances[w][tp][i].dropna().index:

                        # Column exists ?
                        if (w, tp, i) in account.balances.columns:

                            v = account.balances[w][tp][i][c]

                            if np.isnan(v):
                                v = 'NaN'

                            if tp not in d[now].keys():
                                d[now][tp] = dict()
                            if i not in d[now][tp].keys():
                                d[now][tp][i] = dict()
                            if c not in d[now][tp][i].keys():
                                d[now][tp][i][c] = dict()

                            d[now][tp][i][c] = v

            setattr(fund, w, d)

        fund.save()


# Rebalance fund of an account
@app.task(name='Trading_____Rebalance_account')
def rebalance(account_id, get_balances=False, release=True):
    #
    account = Account.objects.get(id=account_id)

    log.bind(worker=current_process().index, account=account.name)

    log.info('')
    log.info('Rebalance...')
    log.info('')

    if get_balances:
        create_balances(account_id)

    # Update prices
    account.get_spot_prices(update=True)
    account.get_futu_prices(update=True)

    # Re-calculate assets value
    account.calculate_assets_value()

    # Calculate new delta
    account.get_target()
    account.calculate_delta()

    log.info(' ')
    log.info('Weights')
    log.info('*******')
    log.info(' ')

    # Display account percent
    current = account.balances.account.current.percent
    for coin, val in current[current != 0].sort_values(ascending=False).items():
        log.info('Percentage for {0}: {1}%'.format(coin, round(val * 100, 1)))

    # Display target percent
    target = account.balances.account.target.percent
    for coin, val in target[target != 0].sort_values(ascending=False).items():
        log.info('Target for {0}: {1}%'.format(coin, round(val * 100, 1)))

    if release:

        log.info(' ')
        log.info('Release resources')
        log.info('*****************')

        # Release resources
        account.sell_spot_all()
        account.close_short_all()

    # Determine available resources
    bal_spot = account.balances.spot.free.value[account.quote]
    bal_futu = account.balances.future.total.value[account.quote]

    if ('position', 'open', 'value') in account.balances.columns:
        pos_val = abs(account.balances.position.open.value.dropna()).sum()
        bal_futu -= pos_val

    if np.isnan(bal_spot):
        bal_spot = 0
    if np.isnan(bal_futu):
        bal_futu = 0

    # Determine desired resources
    des_spot = account.to_buy_spot_value().sum()
    des_futu = account.to_open_short_value().sum()

    log.info(' ')
    log.info('Balance resource spot {0}'.format(round(bal_spot, 1)))
    log.info('Balance resource futu {0}'.format(round(bal_futu, 1)))
    log.info('Desired resource spot {0}'.format(round(des_spot, 1)))
    log.info('Desired resource futu {0}'.format(round(des_futu, 1)))

    # Determine resource to transfer between account
    need_spot = max(0, des_spot - bal_spot)
    need_futu = max(0, des_futu - bal_futu)

    log.info('Need resource spot {0}'.format(round(need_spot, 1)))
    log.info('Need resource futu {0}'.format(round(need_futu, 1)))

    log.info(' ')
    log.info('Transfer resources')
    log.info('******************')
    log.info(' ')

    # Transfer
    if need_spot:
        free_futu = max(0, bal_futu - des_futu)
        log.info('Free resource futu {0}'.format(round(free_futu, 1)))
        log.info('Resources are needed in spot')
        log.info('{0} {1} are missing'.format(round(need_spot, 3), account.quote))
        amount = min(need_spot, free_futu)
        send_transfer.delay(account_id, 'future', 'spot', amount)

    elif need_futu:
        free_spot = max(0, bal_spot - des_spot)
        log.info('Free resource spot {0}'.format(round(free_spot, 1)))
        log.info('Resources are needed in future')
        log.info('{0} {1} are missing'.format(round(need_futu, 3), account.quote))
        amount = min(need_futu, free_spot)
        send_transfer.delay(account_id, 'spot', 'future', amount)

    else:
        log.info('Sufficient resources')
        log.info('No transfer required')

    # Determine resources that could be allocated
    spot = min(bal_spot, des_spot)
    futu = min(bal_futu, des_futu)

    log.info('')
    log.info('Allocate resources')
    log.info('******************')

    # Determine account to allocate resource first
    if spot > futu:
        account.buy_spot_all()
        account.open_short_all()
    else:
        account.open_short_all()
        account.buy_spot_all()

    log.unbind('worker', 'account')


# Update open orders of an account
@app.task(name='Trading_____Update_orders')
def update_orders(account_id):
    #
    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account,
                                  status__in=['new', 'partially_filled', 'open']
                                  ).exclude(orderid__isnull=True)

    if orders.exists():
        for order in orders:
            send_fetch_orderid.delay(account_id, order.orderid)
    else:
        pass


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
        # log.info('Account credentials are valid')

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
    log.bind(worker=current_process().index)
    log.info('Place order...')
    log.info(' ')

    log.info('{0} {1} {2}'.format(side.title(), size, code))
    log.info('Order symbol {0} ({1})'.format(symbol, wallet))
    log.info('Order clientid {0}'.format(clientid))

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

        log.error('Order placement failed', cause=str(e))
        log.error('{0} {1} {2}'.format(side.title(), size, code))
        log.error('Order symbol {0} ({1})'.format(symbol, wallet))
        log.error('Order price {0}'.format(price))
        log.error('Order clientid {0}'.format(clientid))
        log.unbind('worker')

    else:

        log.info('Order placement success')

        # Update object and dataframe
        qty_filled = account.update_order_object(wallet, response)
        log.info('Filled {0} {1} at price {2}'.format(qty_filled, code, price))

        account.update_balances(clientid, action, wallet, code, qty_filled)

        log.unbind('worker')

        if then_rebalance:
            return account_id, qty_filled


# Send fetch orderid
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_fetch_orderid')
def send_fetch_orderid(account_id, order_id):
    #
    order = Order.objects.get(orderid=order_id)
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    # Set options
    client.options['defaultType'] = order.market.wallet
    response = client.fetchOrder(id=order.orderid, symbol=order.market.symbol)

    # Update object and dataframe
    qty_filled = account.update_order_object(order.market.wallet, response)
    account.update_balances(order.clientid,
                            order.action,
                            order.market.wallet,
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
    if quantity > 1:

        account = Account.objects.get(id=account_id)
        client = account.exchange.get_ccxt_client(account)

        log.info('Transfer from {0} to {1}'.format(source, dest))
        log.info('Transfer {0} {1}'.format(round(quantity, 1), account.quote))

        try:
            client.transfer(account.quote, quantity, source, dest)

        except ccxt.BadRequest as e:
            log.error('Transfer failed, bad request', src=source, dst=dest, qty=quantity)

        except Exception as e:
            raise Exception('Transfer failed {0}'.format(str(e)))

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
        # Update object and dataframe
        qty_filled = account.update_order_object(order.market.wallet, response)
        account.update_balances(order.clientid,
                                order.action,
                                order.market.wallet,
                                order.market.base.code,
                                qty_filled
                                )