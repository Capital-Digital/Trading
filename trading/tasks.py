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
def bulk_rebalance(strategy_id, reload=False):
    accounts = Account.objects.filter(strategy__id=strategy_id, active=True)
    for account in accounts:
        rebalance.delay(account.id, reload)


# Bulk update open orders of all accounts
@app.task(name='Trading_____Bulk_update_orders')
def bulk_update_orders():
    #
    for account in Account.objects.filter(active=True):
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
def cancel_orders(account_id, user_orders=False):
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

    if user_orders:
        orders = send_fetch_all_open_orders.delay(account_id)
        for order in orders:
            order_id = order['id']
            log.info('Cancel user order {0}'.format(order_id))
            send_cancel_order.delay(account_id, order_id)

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
    log.bind(account=account.name)

    # Fetch account
    account.get_assets_balances()
    account.get_open_positions()

    # Add missing codes
    account.add_missing_coin()

    # Fetch prices
    account.get_spot_prices()
    account.get_futu_prices()

    # Calculate assets value
    account.calculate_assets_value()

    account.drop_dust_coins()
    account.check_columns()

    log.unbind('worker', 'account')


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
        current = account.assets_value()
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
def rebalance(account_id, reload=False, release=True):
    #
    account = Account.objects.get(id=account_id)

    log.bind(worker=current_process().index, account=account.name, release=release)

    log.info('')
    log.info('Rebalance...')
    log.info('')

    if reload:
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

        # Sell spot
        for code in account.codes_to_sell():
            if account.has_spot_asset('free', code):

                log.info(' ')
                log.info('Release resources', action='sell_spot')
                log.info('*****************')

                free = account.balances.spot.free.quantity[code]
                delta = account.balances.account.target.delta[code]

                # Determine order size and value
                price = account.balances.price['spot']['bid'][code]
                qty = min(free, delta)

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('spot', code, qty, price)
                if valid:
                    # Create object, place order and apply offset
                    clientid = account.create_object('spot', code, 'sell', 'sell_spot', qty)
                    send_create_order(account.id, clientid, 'sell_spot', 'sell', 'spot', code, qty, reduce_only)

        # Close short
        for code in account.codes_to_buy():
            if account.has_opened_short(code):

                log.info(' ')
                log.info('Release resources', action='close_short')
                log.info('*****************')

                opened = account.balances.position.open.quantity[code]
                delta = account.balances.account.target.delta[code]

                # Determine order size and value
                price = account.balances.price['future']['last'][code]
                qty = abs(max(opened, delta))

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('future', code, qty, price)
                if valid:
                    # Create object and place order
                    clientid = account.create_object('future', code, 'buy', 'close_short', qty)
                    send_create_order(account.id, clientid, 'close_short', 'buy', 'future', code, qty, reduce_only)

    # Allocate free resources
    #########################

    # Open short
    for code in account.codes_to_sell():
        if not account.has_spot_asset('free', code):

            log.info(' ')
            log.info('Allocate resources', action='open_short')
            log.info('******************')

            try:
                # Test if a sell spot or an open short order is open
                market, flip = account.exchange.get_spot_market(code, account.quote)
                open = Order.objects.get(account=account,
                                         status='open',
                                         market=market,
                                         action__in=['sell_spot', 'open_short']
                                         )
            except ObjectDoesNotExist:
                open_order_size = 0
            else:
                if flip:
                    open_order_size = open.amount / open.price
                else:
                    open_order_size = open.amount

            # Determine delta quantity
            price = account.balances.price['spot']['bid'][code]
            delta = account.balances.account.target.delta[code] - open_order_size  # Offset sell/close order size

            if delta > 0:

                # Determine value
                desired_val = delta * price
                val = min(account.free_margin(), desired_val)

                # Transfer is needed ?
                if val < desired_val:
                    amount = min(desired_val - val, account.balances.spot.free.quantity[account.quote])
                    transfer_id = send_transfer(account.id, 'spot', 'future', amount)
                    if transfer_id:
                        account.offset_transfer('spot', 'future', amount, transfer_id)
                        val += amount

                # Determine quantity from available resources
                qty = val / price

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('future', code, qty, price)
                if valid:
                    # Create object and place order
                    clientid = account.create_object('future', code, 'sell', 'open_short', qty)
                    send_create_order(account.id, clientid, 'open_short', 'sell', 'future', code, qty)

    # Buy spot
    for code in account.codes_to_buy():

        log.info(' ')
        log.info('Allocate resources', action='buy_spot')
        log.info('******************')

        try:
            # Test if a close_short or a buy_spot order is open
            market, flip = account.exchange.get_perp_market(code, account.quote)
            open = Order.objects.get(account=account,
                                     status='open',
                                     market=market,
                                     action__in=['buy_spot', 'close_short']
                                     )
        except ObjectDoesNotExist:

            # If no order is found and if a short is opened then continue
            if account.has_opened_short(code):
                continue
            else:
                open_order_size = 0

        else:
            if flip:
                open_order_size = open.amount / open.price
            else:
                open_order_size = open.amount

        # Get available resource
        free = account.balances.spot.free.quantity[account.quote]
        if np.isnan(free):
            free = 0

        # Determine order size and value
        price = account.balances.price['spot']['bid'][code]
        delta = abs(account.balances.account.target.delta[code]) - open_order_size  # Offset buy/close order size
        desired_val = delta * price
        val = min(free, desired_val)

        # Transfer is needed ?
        if val < desired_val:
            amount = min(desired_val - val, account.free_margin())
            transfer_id = send_transfer(account.id, 'future', 'spot', amount)
            if transfer_id:
                account.offset_transfer('future', 'spot', amount, transfer_id)
                val += amount

        # Determine quantity from available resources
        qty = val / price

        # Format decimal and validate order
        valid, qty, reduce_only = account.validate_order('spot', code, qty, price)
        if valid:
            # Create object and place order
            clientid = account.create_object('spot', code, 'buy', 'buy_spot', qty)
            send_create_order(account.id, clientid, 'buy_spot', 'buy', 'spot', code, qty, reduce_only)


# Update open orders of an account
@app.task(name='Trading_____Update_orders')
def update_orders(account_id):
    #
    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account,
                                  status__in=['open', 'unknown']
                                  )

    if orders.exists():
        for order in orders:
            # log.info('Update order', id=order.orderid)
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


# Market close
@app.task(name='Trading_____Market_close')
def market_close(account_id):
    #
    account = Account.objects.get(id=account_id)
    if 'position' in account.balances.columns.get_level_values(0).tolist():

        opened = account.balances.position.open.quantity.dropna()
        if len(opened) > 0:
            for code in opened.index.tolist():

                log.info('Close position {0}'.format(code))

                amount = account.balances.position.open.quantity[code]
                qty = abs(amount)
                price = account.balances.price.spot.bid[code]

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('spot', code, qty, price)
                if valid:

                    # Create object, place order and apply offset
                    clientid = account.create_object('future', code, 'buy', 'close_short', qty)
                    send_create_order(account.id, clientid, 'close_short', 'buy', 'future', code, qty,
                                      reduce_only=True,
                                      market_order=True
                                      )
        else:
            log.info('No position found')
    else:
        log.info('No position found')


# Market sell
@app.task(name='Trading_____Market_sell')
def market_sell(account_id):
    #
    account = Account.objects.get(id=account_id)
    if account.has_spot_asset('free'):
        for code, qty in account.balances.spot.free.quantity.dropna().items():

            if code != account.quote:

                # Determine order size and value
                price = account.balances.price['spot']['bid'][code]

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('spot', code, qty, price)
                if valid:

                    # Create object, place order and apply offset
                    clientid = account.create_object('spot', code, 'sell', 'sell_spot', qty)
                    send_create_order(account.id, clientid, 'sell_spot', 'sell', 'spot', code, qty, reduce_only,
                                      market_order=True
                                      )
    else:
        log.info('No free asset found', account=account.name)


# REST API
##########


# Send create order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_create_order')
def send_create_order(account_id, clientid, action, side, wallet, code, qty, reduce_only=False, market_order=False):
    #
    log.info(' ')
    log.info('Place {0} order in {1}'.format(side, wallet))

    # Initialize client
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    client.options['defaultType'] = wallet

    # Select market
    if wallet == 'spot':
        market, flip = account.exchange.get_spot_market(code, account.quote)
    else:
        market, flip = account.exchange.get_perp_market(code, account.quote)

    # Determine price
    if wallet == 'future':
        key = 'last'
    elif not flip and side == 'buy':
        key = 'ask'
    elif not flip and side == 'sell':
        key = 'bid'
    elif flip and side == 'buy':
        key = 'bid'
    elif flip and side == 'sell':
        key = 'ask'

    # Change side
    if flip and side == 'buy':
        side = 'sell'
    elif flip and side == 'sell':
        side = 'buy'

    price = market.get_latest_price(key)

    # Change qty
    if flip:
        qty = qty / price

    log.info('{0} {1} {2}'.format(side.title(), qty, code))
    log.info('Order symbol {0} ({1})'.format(market.symbol, wallet))
    log.info('Order clientid {0}'.format(clientid))

    kwargs = dict(
        symbol=market.symbol,
        type=account.order_type,
        side=side,
        amount=qty,
        price=price,
        params=dict(newClientOrderId=clientid)
    )

    if market_order:
        kwargs['type'] = 'market'
        del kwargs['price']

    # Set parameters
    if reduce_only:
        kwargs['params']['reduceOnly'] = True

    try:
        response = client.create_order(**kwargs)

    except ccxt.BadRequest as e:
        order_error(clientid, e, kwargs)
        log.error('BadRequest error')
        return None, None

    except ccxt.InsufficientFunds as e:
        order_error(clientid, e, kwargs)
        log.error('Insufficient funds error')
        return None, None

    except ccxt.ExchangeError as e:
        order_error(clientid, e, kwargs)
        log.error('Exchange error')
        return None, None

    else:

        log.info('Order placement success')

        # Offset resources used and free
        val = qty * price
        account.offset_order_new(code, action, qty, val)

        # Update object status
        filled, average = account.update_order_object(wallet, response, new=True)
        if filled:
            # Offset trade
            account.offset_order_filled(code, action, filled, average)


# Send fetch orderid
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_fetch_orderid')
def send_fetch_orderid(account_id, order_id):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    order = Order.objects.get(orderid=order_id)

    # Set options
    client.options['defaultType'] = order.market.wallet

    try:
        response = client.fetchOrder(id=order_id, symbol=order.market.symbol)

    except ccxt.OrderNotFound:
        log.error('Unknown order {}'.format(order.clientid), id=order_id)

    except Exception as e:
        log.error('Unknown exception when fetching order {}'.format(order.clientid), id=order_id, e=str(e))

    else:

        # Update object
        filled, average = account.update_order_object(order.market.wallet, response)
        if filled:

            code = order.market.base.code

            # Offset trade
            account.offset_order_filled(code, order.action, filled, average)

            t = 0
            while account.busy:
                log.info(' ')
                log.info('Wait account is ready before allocating free resources', account=account.name)
                time.sleep(1)
                t += 1
                if t == 10:
                    raise Exception('Account is still busy after 10s')

            # Rebalance
            rebalance.delay(account_id)


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
        return response


# Send transfer order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_transfer_funds')
def send_transfer(account_id, source, dest, quantity):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    # Generate transfer_id
    alphanumeric = 'abcdefghijklmnopqrstuvwABCDEFGHIJKLMNOPQRSTUVWWXYZ01234689'
    transfer_id = ''.join((random.choice(alphanumeric)) for x in range(5))

    log.bind(worker=current_process().index, account=account.name, id=transfer_id)

    if quantity > 1:

        log.info('Transfer {0}'.format(transfer_id))
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
            log.unbind('worker', 'account', 'id')
            return transfer_id

    else:
        log.unbind('worker', 'account', 'id')


# Send cancellation order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_cancel_order')
def send_cancel_order(account_id, order_id):
    #
    try:
        order = Order.objects.get(orderid=order_id)

    except MultipleObjectsReturned:
        log.error('Multiple orders were found')

    else:
        account = Account.objects.get(id=account_id)
        client = account.exchange.get_ccxt_client(account)
        client.options['defaultType'] = order.market.wallet

        try:
            response = client.cancel_order(id=order_id, symbol=order.market.symbol)

        except Exception as e:
            log.error('Order canceled failure', e=str(e))

        else:
            # Update object and dataframe
            account.update_order_object(order.market.wallet, response)


@app.task(bind=True, name='Test')
def test(self, loop):

    t = 0
    task_id = self.request.id[:3]
    process_id = current_process().index
    a = Account.objects.get(name='Principal')

    pos = Position.objects.get(account=a, pk=process_id)
    log.info('Task {0} start with process {1}'.format(task_id, process_id))
    pos.size = 1

    while t<=loop:
        pos.size += 1
        pos.save()
        t += 1

    log.info('Task {0} complete'.format(task_id))


