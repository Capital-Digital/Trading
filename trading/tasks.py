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
            update_historical_balance.delay(account.id)


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
@app.task(name='Trading_____Update_historical_balance')
def update_historical_balance(account_id):
    account = Account.objects.get(id=account_id)

    try:
        fund = Fund.objects.get(account=account, exchange=account.exchange)

    except ObjectDoesNotExist:
        fund = Fund.objects.create(account=account, exchange=account.exchange)

    finally:

        dt = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        now = dt.strftime(datetime_directive_ISO_8601)

        # Calculate current account balance
        current = account.account_value()
        hist = fund.historical_balance

        if not hist:
            hist = dict()

        hist[now] = dict(balance=round(current, 1),
                         strategy_id=account.strategy.id,
                         strategy=account.strategy.name
                         )
        fund.historical_balance = hist
        log.info('Save account current balance', current=current)
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

        # Release resources
        ###################

        log.info(' ')
        log.info('Release resources')
        log.info('*****************')

        # Sell spot
        for code in account.codes_to_sell():
            if account.has_spot_asset('free', code):
                free = account.balances.spot.free.quantity[code]
                delta = account.balances.account.target.delta[code]

                # Determine order size and value
                price = account.balances.price['spot']['bid'][code]
                qty = min(free, delta)
                val = qty * price

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('spot', code, qty, val)
                if valid:
                    # Determine final order value
                    val = qty * price

                    # Create object, place order and apply offset
                    clientid = account.create_object('spot', code, 'sell', 'sell_spot', qty)
                    filled = send_create_order(account.id, clientid, 'sell', 'spot', code, qty, reduce_only)
                    account.offset_order(code, 'sell_spot', qty, val, filled)

        # Close short
        for code in account.codes_to_buy():
            if account.has_opened_short(code):
                opened = account.balances.position.open.quantity[code]
                delta = account.balances.account.target.delta[code]

                # Determine order size and value
                price = account.balances.price['future']['last'][code]
                qty = abs(max(opened, delta))
                val = qty * price

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('future', code, qty, val)
                if valid:
                    # Determine final order value
                    val = qty * price

                    # Create object, place order and apply offset
                    clientid = account.create_object('future', code, 'buy', 'close_short', qty)
                    filled = send_create_order(account.id, clientid, 'buy', 'future', code, qty, reduce_only)
                    account.offset_order(code, 'close_short', qty, val, filled)

    # Allocate free resources
    #########################

    log.info(' ')
    log.info('Allocate resources')
    log.info('******************')

    # Open short
    for code in account.codes_to_sell():
        if not account.has_spot_asset('free', code):

            if account.has_spot_asset('used', code):
                pending = account.balances.spot.used.quantity[code]
                log.info('Pending order detected {0} {1}'.format(round(pending, 3), code))
            else:
                pending = 0

            # Determine delta quantity
            price = account.balances.price['spot']['bid'][code]
            delta = account.balances.account.target.delta[code] - pending  # Offset sell_spot
            if delta > 0:

                # Determine value
                desired_val = delta * price
                val = min(account.free_margin(), desired_val)

                # Transfer is needed ?
                if val < desired_val:
                    amount = min(desired_val - val, account.has_spot_asset('free', account.quote))
                    send_transfer(account.id, 'spot', 'future', amount)
                    account.offset_transfer('spot', 'future', amount)
                    val += amount

                # Determine quantity from available resources
                qty = val / price

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('future', code, qty, val)
                if valid:
                    # Determine final order value
                    val = qty * price

                    # Create object, place order and apply offset
                    clientid = account.create_object('spot', code, 'buy', 'open_short', qty)
                    filled = send_create_order(account.id, clientid, 'sell', 'future', code, qty)
                    account.offset_order(code, 'open_short', qty, val, filled)

    # Buy spot
    for code in account.codes_to_buy():

        # No opened short ?
        if not account.has_opened_short(code):
            remaining = 0
        else:
            # Else determine remaining quantity if an opened short is pending
            remaining = Order.objects.get(account=account,
                                          market__base__code=code,
                                          market__quote__code=account.quote,
                                          market__wallet='future'
                                          ).remaining

        # Get available resource
        free = account.balances.spot.free.quantity[account.quote]

        # Determine order size and value
        price = account.balances.price['spot']['bid'][code]
        delta = abs(account.balances.account.target.delta[code]) - remaining  # Offset pending close_short
        desired_val = delta * price
        val = min(free, desired_val)

        # Transfer is needed ?
        if val < desired_val:
            amount = min(desired_val - val, account.free_margin())
            send_transfer(account.id, 'future', 'spot', amount)
            account.offset_transfer('future', 'spot', amount)
            val += amount

        # Determine quantity from available resources
        qty = val / price

        # Format decimal and validate order
        valid, qty, reduce_only = account.validate_order('spot', code, qty, val)
        if valid:
            # Determine final order value
            val = qty * price

            # Create object, place order and apply offset
            clientid = account.create_object('spot', code, 'buy', 'buy_spot', qty)
            filled = send_create_order(account.id, clientid, 'buy', 'spot', code, qty, reduce_only)
            account.offset_order(code, 'buy_spot', qty, val, filled)

    log.unbind('account')


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
def send_create_order(account_id, clientid, side, wallet, code, desired_qty, reduce_only=False, market_order=False):
    #
    log.info(' ')
    # log.bind(worker=current_process().index)
    log.info('Place order...')
    log.info(' ')

    # Initialize client
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    client.options['defaultType'] = wallet

    # Determine market
    market = Market.objects.filter(exchange=account.exchange,
                                   quote__code=account.quote,
                                   base__code=code,
                                   wallet=wallet)
    if wallet == 'future':
        market = market.get(type='derivative',
                            contract_type='perpetual')

    # Determine price
    if wallet == 'future':
        key = 'last'
    elif side == 'buy':
        key = 'ask'
    elif side == 'sell':
        key = 'bid'

    price = market.get_latest_price(key)

    log.info('{0} {1} {2}'.format(side.title(), desired_qty, code))
    log.info('Order symbol {0} ({1})'.format(market.symbol, wallet))
    log.info('Order clientid {0}'.format(clientid))

    kwargs = dict(
        symbol=market.symbol,
        type=account.order_type,
        side=side,
        amount=desired_qty,
        price=price,
        params=dict(newClientOrderId=clientid)
    )

    if market_order:
        del kwargs['price']

    # Set parameters
    if reduce_only:
        kwargs['params']['reduceOnly'] = True

    try:
        response = client.create_order(**kwargs)

    except ccxt.InsufficientFunds as e:

        order = Order.objects.get(clientid=clientid)
        order.status = 'canceled'
        order.response = dict(exception=str(e))
        order.save()

        log.error('Order placement failed', cause=str(e))
        log.error('{0} {1} {2}'.format(side.title(), desired_qty, code))
        log.error('Order symbol {0} ({1})'.format(market.symbol, wallet))
        log.error('Order price {0}'.format(price))
        log.error('Order clientid {0}'.format(clientid))
        # log.unbind('worker')

    else:

        log.info('Order placement success')

        # Update object and dataframe
        filled = account.update_order_object(wallet, response)

        log.info('Filled {0} {1} at price {2}'.format(filled, code, price))
        # log.unbind('worker')

        return filled


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

        # Generate transfer_id
        alphanumeric = 'abcdefghijklmnopqrstuvwABCDEFGHIJKLMNOPQRSTUVWWXYZ01234689'
        transfer_id = ''.join((random.choice(alphanumeric)) for x in range(3))

        log.bind(worker=current_process().index, account=account.name, id=transfer_id)

        log.info('Transfer ID {0}'.format(transfer_id))
        log.info('Transfer from {0} to {1}'.format(source, dest))
        log.info('Transfer {0} {1}'.format(round(quantity, 1), account.quote))

        try:
            client.transfer(account.quote, quantity, source, dest)

        except ccxt.BadRequest as e:
            log.error('Transfer failed, bad request', src=source, dst=dest, qty=quantity)

        except Exception as e:
            raise Exception('Transfer failed {0}'.format(str(e)))

        else:

            log.info('Transfer success')

            # Update dataframe
            account.update_balances_after_transfer(transfer_id, source, dest, quantity)

            return account_id, quantity

        finally:
            log.unbind('worker', 'account', 'id')


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
