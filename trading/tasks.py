from __future__ import absolute_import, unicode_literals
import traceback
import sys
import asyncio
import json
import time
from itertools import accumulate
from pprint import pprint
from django.db.models.query import QuerySet
from django.db.models import Q, Sum, Avg
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
from trading.models import Account, Order, Fund, Position, Asset, Stat
import threading
import random
import math

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

# Fetch assets and update objects
@app.task(name='Trading_____Bulk_fetch_assets')
def bulk_fetch_assets():
    for exchange in Exchange.objects.all():
        for account in Account.objects.filter(exchange=exchange, active=True):
            for wallet in exchange.get_wallets():
                fetch_assets.delay(account.id, wallet)


# Fetch positions and update objects
@app.task(name='Trading_____Bulk_fetch_positions')
def bulk_fetch_positions():
    for exchange in Exchange.objects.all():
        for account in Account.objects.filter(exchange=exchange, active=True):
            fetch_positions.delay(account.id)


# Cancel orders and query assets quantity and open positions
@app.task(name='Trading_____Bulk_prepare_accounts')
def bulk_prepare_accounts():
    for exchange in Exchange.objects.all():
        for account in Account.objects.filter(exchange=exchange, active=True):
            prepare_accounts.delay(account.id)


# Bulk rebalance assets of all accounts
@app.task(name='Trading_Bulk_rebalance_accounts')
def bulk_rebalance(strategy_id, reload=False):
    accounts = Account.objects.filter(strategy__id=strategy_id, active=True)
    for account in accounts:

        while not account.is_tradable():
            log.warning('Wait {0} markets update before sync. the account'.format(account.quote))
            time.sleep(0.1)

        rebalance.delay(account.id, reload)


# Bulk update stats
@app.task(name='Trading_Bulk_update_stats')
def bulk_update_stats():
    accounts = Account.objects.filter(active=True)
    for account in accounts:
        update_stats.delay(account.id)


# Bulk update open orders of all accounts
@app.task(name='Trading_____Bulk_update_orders')
def bulk_update_orders():
    #
    for account in Account.objects.filter(active=True, busy=False):
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
    log.info('Prepare accounts')

    account = Account.objects.get(id=account_id)
    chord(cancel_orders.s(account.id))(create_balances.s())


# Synchronize open orders
@app.task(bind=True, name='Trading_____Sync_orders')
def synchronize_orders(self, account_id, user_orders=False):
    #
    account = Account.objects.get(id=account_id)
    log.bind(account=account.name)
    if self.request.id:
        log.bind(worker=current_process().index)

    if user_orders:
        wallet, dic = send_fetch_all_open_orders.delay(account_id)
        for order in dic:
            orderid = order['id']
            try:
                Order.objects.get(account=account, orderid=orderid)
            except ObjectDoesNotExist:
                code = order['symbol']
                qty = order['amount']
                price = order['price']
                side = order['side']

                log.info('Synchronize order {0}'.format(orderid))
                account.create_object(wallet, code, side, None, qty, price)


# Cancel open orders of an account
@app.task(bind=True, name='Trading_____Cancel_orders')
def cancel_orders(self, account_id, user_orders=False):
    #
    account = Account.objects.get(id=account_id)

    log.bind(account=account.name)
    if self.request.id:
        log.bind(worker=current_process().index)

    orders = Order.objects.filter(account=account, status='open')  # .exclude(orderid__isnull=True)

    if orders.exists():
        for order in orders:
            log.info('Cancel order {0}'.format(order.clientid))
            send_cancel_order(account_id, order.orderid)

    if user_orders:
        wallet, response = send_fetch_all_open_orders(account_id)
        for dic in response:

            orderid = dic['id']
            symbol = dic['symbol']

            log.info('Cancel order {0}'.format(orderid))
            send_cancel_order(account_id, orderid, wallet, symbol)

    log.info('Cancel orders complete')

    log.unbind('account')
    if self.request.id:
        log.unbind('worker')

    return account_id


# Create balances dataframe
@app.task(bind=True, name='Trading_____Create_balances')
def create_balances(self, account_id):
    #

    if isinstance(account_id, list):
        account_id = account_id[0]

    account = Account.objects.get(id=account_id)

    log.bind(account=account.name)
    if self.request.id:
        log.bind(worker=current_process().index)

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


# Create or update stats object
@app.task(name='Trading_____Update_stats')
def update_stats(account_id):
    account = Account.objects.get(id=account_id)

    try:
        stat = Stat.objects.get(account=account, exchange=account.exchange, strategy=account.strategy)

    except ObjectDoesNotExist:
        stat = Stat.objects.create(account=account, exchange=account.exchange, strategy=account.strategy)

    finally:

        dt = dt_aware_now(0)
        idx = pd.DatetimeIndex([dt])

        if not isinstance(stat.account_value, pd.DataFrame):
            stat.account_value = pd.DataFrame()

        if idx[0] not in stat.account_value.index:

            assets = round(account.assets_value(), 1)
            positions = round(account.positions_pnl(), 1)
            val = assets + positions
            sid = account.strategy.id

            stat.account_value.loc[idx[0], ('balance', 'strategy_id')] = (val, sid)
            stat.save()

            log.info('Update account value complete')

        else:
            log.info('Account value is already present')

            # btc = Currency.objects.get(code='BTC').get_latest_price(account.exchange, 'BUSD', 'last')
            # eth = Currency.objects.get(code='ETH').get_latest_price(account.exchange, 'BUSD', 'last')
            #


# Compare return with Bitcoin and Ethereum
@app.task(name='Trading_____Calculate_benchmark')
def stats_benchmark(account_id):
    account = Account.objects.get(id=account_id)

    try:
        stat = Stat.objects.get(account=account, exchange=account.exchange, strategy=account.strategy)

    except ObjectDoesNotExist:
        stat = Stat.objects.create(account=account, exchange=account.exchange, strategy=account.strategy)

    finally:

        dt = datetime.now().replace(minute=0, second=0, microsecond=0)
        idx = pd.DatetimeIndex([dt])

        if not isinstance(stat.benchmark, pd.DataFrame):
            stat.benchmark = pd.DataFrame()

        if idx not in stat.benchmark.index:

            val = round(account.assets_value(), 1)
            sid = account.strategy.id

            stat.assets_value_history.loc[idx, ('balance', 'strategy_id')] = (val, sid)
            stat.save()

            log.info('Update account value complete')

        else:
            log.info('Account value is already present')

            # btc = Currency.objects.get(code='BTC').get_latest_price(account.exchange, 'BUSD', 'last')
            # eth = Currency.objects.get(code='ETH').get_latest_price(account.exchange, 'BUSD', 'last')
            #


# Rebalance fund of an account
@app.task(bind=True, name='Trading_____Rebalance_account')
def rebalance(self, account_id, reload=False, release=True):
    #
    account = Account.objects.get(id=account_id)

    log.info('Task id is {0}'.format(self.request.id))

    # Mark the account as busy to avoid concurrent
    # rebalancing after a new trade is detected
    account.busy = True
    account.save()

    log.bind(account=account.name)
    if self.request.id:
        log.bind(worker=current_process().index)

    log.info('')

    if reload:

        log.info('Synchronize (fetch data)')
        create_balances(account_id)

    else:
        log.info('Synchronize (select data from df)')

    # Add missing codes
    account.add_missing_coin()

    # Update prices
    account.get_spot_prices(update=True)
    account.get_futu_prices(update=True)

    # Re-calculate assets value
    account.calculate_assets_value()

    account.drop_dust_coins()
    account.check_columns()

    # Calculate new delta
    account.get_target()
    account.calculate_delta()

    log.info(' ')
    log.info('Account')
    log.info('*******')
    log.info(' ')

    # Display account percent
    current = account.balances.account.current.percent
    for coin, val in current[current != 0].sort_values(ascending=False).items():
        log.info('Percentage for {0}: {1}%'.format(coin, round(val * 100, 1)))

    for coin, val in current[current != 0].sort_values(ascending=False).items():
        log.info('Exposure -> {0} {1}'.format(round(account.balances.account.current.exposure[coin], 3), coin))

    log.info(' ')
    log.info('Targets')
    log.info('*******')
    log.info(' ')

    # Display target percent
    target = account.balances.account.target.percent

    if account.codes_long():

        log.info('Long coins')
        log.info('-------------------------')
        for coin, val in target[target > 0].sort_values(ascending=False).items():
            if coin != account.quote:
                log.info('Target pct for {0}: {1}%'.format(coin, round(val * 100, 1)))
        log.info('-------------------------')
        for coin, val in target[target > 0].sort_values(ascending=False).items():
            if coin != account.quote:
                log.info('Target qty for {0}: {1}'.format(coin,
                                                          round(account.balances.account.target.quantity[coin], 4)))
        log.info('-------------------------')

    if account.codes_short():

        log.info('')
        log.info('Short coins')
        log.info('-------------------------')
        for coin, val in target[target < 0].sort_values(ascending=False).items():
            log.info('Target pct for {0}: {1}%'.format(coin, round(val * 100, 1)))
        log.info('-------------------------')
        for coin, val in target[target < 0].sort_values(ascending=False).items():
            log.info('Target qty for {0}: {1}'.format(coin, round(account.balances.account.target.quantity[coin], 4)))
        log.info('-------------------------')

    log.info(' ')
    log.info('Delta')
    log.info('*******')
    log.info(' ')

    # Display target percent
    delta = account.balances.account.target.delta
    for coin, val in delta[delta != 0].sort_values(ascending=False).items():
        log.info('Delta qty for {0}: {1}'.format(coin, round(val, 4)))
    log.info('---------------------------')

    if release:

        # Release resources
        ###################

        # Sell spot
        for code in account.codes_to_sell_spot():

            log.info(' ')
            log.bind(action='sell_spot')
            log.info('Sell spot {0}'.format(code))
            log.info('*************')

            # Return amount of open orders
            open_spot = account.get_open_orders_spot(code, side='sell', action='sell_spot')
            open_futu = account.get_open_orders_futu(code, side='sell', action='open_short')

            free = account.balances.spot.free.quantity[code]
            delta = max(0, account.balances.account.target.delta[code] - (open_spot + open_futu))
            need = min(free, delta)

            if need:

                log.info('Total free assets in spot is {0} {1}'.format(round(free, 4), code))
                log.info('Desired quantity to sell spot {1} is {0}'.format(round(need, 4), code))

                # Determine order size and value
                price = account.balances.price['spot']['bid'][code]
                qty = min(free, need)  # limit sell to what is actually available

                log.info('Quantity sellable is {0} {1}'.format(round(qty, 4), code))

                # Format decimal and validate order
                valid, qty, reduce_only = account.validate_order('spot', 'sell', code, qty, price, 'sell_spot')
                if valid:
                    # Create object, place order and apply offset
                    clientid = account.create_object('spot', code, 'sell', 'sell_spot', qty, price)
                    send_create_order(account.id, clientid, 'sell_spot', 'sell', 'spot', code, qty, reduce_only)

                # Refresh obj
                account.refresh_from_db()
            log.unbind('action')

        # Close short
        for code in account.codes_to_buy():
            if account.has_opened_short(code):

                log.info(' ')
                log.bind(action='close_short')
                log.info('Close short {0}'.format(code))
                log.info('***************')

                # Return amount of open orders
                open_spot = account.get_open_orders_spot(code, side='buy', action='buy_spot')
                open_futu = account.get_open_orders_futu(code, side='buy', action='close_short')

                opened = abs(account.balances.position.open.quantity[code])
                delta = max(0, abs(account.balances.account.target.delta[code]) - (open_spot + open_futu))
                need = min(opened, delta)  # limit close to what is actually opened

                if need:

                    log.info('Opened short position is {0} {1}'.format(round(-opened, 4), code))
                    log.info('Desired quantity to close short {1} is {0}'.format(round(-delta, 4), code))

                    # Determine order size and value
                    price = account.balances.price['future']['last'][code]
                    qty = min(opened, need)

                    log.info('Quantity closable is {0} {1}'.format(round(qty, 4), code))

                    # Format decimal and validate order
                    valid, qty, reduce_only = account.validate_order('future', 'buy', code, qty, price, 'close_short')
                    if valid:
                        # Create object and place order
                        clientid = account.create_object('future', code, 'buy', 'close_short', qty, price)
                        send_create_order(account.id, clientid, 'close_short', 'buy', 'future', code, qty, reduce_only)

                    account.refresh_from_db()
                log.unbind('action')

    # Allocate free resources
    #########################

    # Open short
    for code in account.codes_to_open_short():

        log.info(' ')
        log.bind(action='open_short')
        log.info('Open short {0}'.format(code))
        log.info('**************')

        # Return amount of open orders
        open_spot = account.get_open_orders_spot(code, side='sell', action='sell_spot')
        open_futu = account.get_open_orders_futu(code, side='sell', action='open_short')

        # Determine delta quantity
        price = account.balances.price['spot']['bid'][code]
        delta = account.balances.account.target.delta[code] - (open_spot + open_futu)  # Offset sell/close order

        if delta > 0:

            log.info('Delta quantity for {1} is {0}'.format(round(delta, 4), code))

            # Determine value
            desired_val = delta * price
            free_margin = account.free_margin()
            val = min(free_margin, desired_val)

            log.info('Desired order value is {0} {1}'.format(round(desired_val, 1), account.quote))
            log.info('Free margin is {0} {1}'.format(round(free_margin, 1), account.quote))
            log.info('Maximum order value is {0} {1}'.format(round(val, 1), account.quote))

            # Transfer is needed ?
            if val < desired_val:

                amount = min(desired_val - val, account.balances.spot.free.quantity[account.quote])
                transfer_id = send_transfer(account.id, 'spot', 'future', amount)
                if transfer_id:
                    account.offset_transfer('spot', 'future', amount, transfer_id)
                    val += amount

                    log.info('Order value after transfer is {0} {1}'.format(round(val, 1), account.quote))

            # Determine quantity from available resources
            qty = math.floor(val) / price
            log.info('Maximum order quantity is {0} {1}'.format(round(qty, 4), code))

            # Format decimal and validate order
            valid, qty, reduce_only = account.validate_order('future', 'sell', code, qty, price, 'open_short')
            if valid:
                # Create object and place order
                clientid = account.create_object('future', code, 'sell', 'open_short', qty, price)
                send_create_order(account.id, clientid, 'open_short', 'sell', 'future', code, qty)

            account.refresh_from_db()
        log.unbind('action')

    # Buy spot
    for code in account.codes_to_buy():
        # if not account.has_opened_short(code):

        log.info(' ')
        log.bind(action='buy_spot')
        log.info('Buy spot {0}'.format(code))
        log.info('************')

        account.refresh_from_db()

        # Return amount of open orders
        open_spot = account.get_open_orders_spot(code, side='buy', action='buy_spot')
        open_futu = account.get_open_orders_futu(code, side='buy', action='close_short')

        # Determine desired order size and value
        price = account.balances.price['spot']['bid'][code]
        delta = max(0, abs(account.balances.account.target.delta[code]) - (open_spot + open_futu))

        if delta:

            desired_val = delta * price

            # Get available resource
            free = account.balances.spot.free.quantity[account.quote]
            if np.isnan(free):
                free = 0

            val = min(free, desired_val)

            log.info('Delta quantity for {1} is {0}'.format(round(delta, 4), code))
            log.info('Desired order value is {0} {1}'.format(round(desired_val, 1), account.quote))
            log.info('Available resources is {1} {0}'.format(account.quote, round(free, 1)))
            log.info('Maximum order value is {0} {1}'.format(round(val, 1), account.quote))

            # Transfer is needed ?
            if val < desired_val:
                amount = min(desired_val - val, account.free_margin())
                transfer_id = send_transfer(account.id, 'future', 'spot', amount)
                if transfer_id:
                    account.offset_transfer('future', 'spot', amount, transfer_id)
                    val += amount
                    log.info('Order value after transfer is {0} {1}'.format(round(val, 1), account.quote))

            # Determine quantity from available resources
            qty = math.floor(val) / price
            log.info('Maximum order quantity is {0} {1}'.format(round(qty, 4), code))

            # Format decimal and validate order
            valid, qty, reduce_only = account.validate_order('spot', 'buy', code, qty, price, 'buy_spot')
            if valid:
                # Create object and place order
                clientid = account.create_object('spot', code, 'buy', 'buy_spot', qty, price)
                send_create_order(account.id, clientid, 'buy_spot', 'buy', 'spot', code, qty, reduce_only)

            account.refresh_from_db()
        log.unbind('action')

    account.busy = False
    account.save()

    log.info(' ')
    log.info('Synchronization complete for {0}'.format(account.name))
    log.unbind('account')


# Update open orders of an account
@app.task(name='Trading_____Update_orders')
def update_orders(account_id):
    #
    account = Account.objects.get(id=account_id)
    orders = Order.objects.filter(account=account, status__in=['open', 'unknown'])

    if orders.exists():
        for order in orders:

            log.bind(clientid=order.clientid)
            # log.info('Fetch order {0}'.format(order.clientid))

            try:
                response = send_fetch_orderid(account_id, order.orderid)
            except ccxt.OrderNotFound:
                pass

            else:
                # log.info('Update order object {0}'.format(order.clientid))

                account.refresh_from_db()
                filled, average = account.update_order_object(order.market.wallet, response)

                if filled:

                    code = order.market.base.code
                    account.offset_order_filled(order.clientid, code, order.action, filled, average)

                    t = 0
                    while account.busy:
                        log.info('Account is busy, wait before new sync.')
                        time.sleep(1)
                        t += 1
                        if t > 10:
                            raise Exception('Account is busy after more than 10s')

                    log.info('Sync. account after a trade')
                    rebalance.delay(account_id, reload=False)

        # log.info('Open order update complete in account {0}'.format(account.name))

    else:
        pass
        # log.info('Open order not found in account {0}'.format(account.name))


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
    client = account.exchange.get_ccxt_client(account)

    for pos in Position.objects.filter(account=account):

        log.info('Close position {0}'.format(pos.market.symbol))

        amount = float(pos.size)
        if amount > 0:
            side = 'sell'
        else:
            side = 'buy'

        qty = abs(amount)

        kwargs = dict(
            symbol=pos.market.symbol,
            type='market',
            side=side,
            amount=qty,
            # params=dict(reduceOnly=True)
        )
        pprint(kwargs)
        try:
            client.create_order(**kwargs)
        except ccxt.InsufficientFunds:
            log.error('Insufficient funds')
        else:
            continue

# Fetch assets
@app.task(base=BaseTaskWithRetry, name='Trading_____Fetch_assets')
def fetch_assets(account_id, wallet=None):
    #
    account = Account.objects.get(id=account_id)
    log.bind(account=account.name, wallet=wallet)
    log.info('Fetch assets')
    client = account.exchange.get_ccxt_client(account)
    if wallet:
        client.options['defaultType'] = wallet
    response = client.fetchBalance()

    # Exclude LBTC from dictionaries (staking or earning account)
    total = {k: v for k, v in response['total'].items() if v > 0 and k != 'LDBTC'}
    free = {k: v for k, v in response['free'].items() if v > 0 and k != 'LDBTC'}
    used = {k: v for k, v in response['used'].items() if v > 0 and k != 'LDBTC'}

    # Delete obsolete objects
    Asset.objects.filter(exchange=account.exchange, account=account, wallet=wallet
                         ).exclude(currency__code__in=[list(total.keys())]).delete()

    # Update objects
    for k, v in total.items():
        currency = Currency.objects.get(code=k)
        try:
            obj = Asset.objects.get(currency=currency,
                                    exchange=account.exchange,
                                    account=account,
                                    wallet=wallet
                                    )
        except ObjectDoesNotExist:
            obj = Asset.objects.create(currency=currency,
                                       exchange=account.exchange,
                                       account=account,
                                       wallet=wallet
                                       )
        finally:
            obj.total = v

            if k in free.keys():
                obj.free = free[k]
            if k in used.keys():
                obj.used = used[k]

            obj.dt_returned = response['datetime']
            obj.save()

    log.unbind('account')


# Fetch positions
@app.task(base=BaseTaskWithRetry, name='Trading_____Fetch_positions')
def fetch_positions(account_id, wallet='future'):
    account = Account.objects.get(id=account_id)
    log.bind(account=account.name)
    log.info('Fetch positions')
    client = account.exchange.get_ccxt_client(account)
    client.options['defaultType'] = wallet

    #  and query all futures endpoint
    response = client.fapiPrivateGetPositionRisk()
    opened = [i for i in response if float(i['positionAmt']) != 0]

    # Delete closed positions
    Position.objects.filter(exchange=account.exchange, account=account
                            ).exclude(market__response__id__in=[p['symbol'] for p in opened]).delete()

    if opened:
        for position in opened:
            market = Market.objects.get(exchange=account.exchange, response__id=position['symbol'], type='derivative')
            try:
                obj = Position.objects.get(market=market,
                                           exchange=account.exchange,
                                           account=account
                                           )
            except ObjectDoesNotExist:
                obj = Position.objects.create(market=market,
                                              exchange=account.exchange,
                                              account=account
                                              )
            finally:

                amount = float(position['positionAmt'])

                obj.size = amount
                obj.side = 'buy' if amount > 0 else 'sell'
                obj.notional_value = float(position['notional'])
                obj.last = float(position['markPrice'])
                obj.entry_price = float(position['entryPrice'])
                obj.margin_mode = position['marginType']
                obj.leverage = float(position['leverage'])
                obj.unrealized_pnl = float(position['unRealizedProfit'])
                obj.liquidation_price = float(position['liquidationPrice'])
                obj.instrument_id = position['symbol']
                obj.settlement = market.margined
                obj.response = opened
                obj.save()

    else:
        log.info('No position opened')

    log.unbind('account')


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
                valid, qty, reduce_only = account.validate_order('spot', 'sell', code, qty, price)
                if valid:
                    # Create object, place order and apply offset
                    clientid = account.create_object('spot', code, 'sell', 'sell_spot', qty, price)
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
    # if reduce_only:
    #     kwargs['params']['reduceOnly'] = True
    #     log.info('Set reduceOnly to True')

    # if action == 'close_short':
    #     kwargs['params']['closePosition'] = True
    #     log.info('Set closePosition to True')

    try:
        log.info(' ')
        log.info('Place order {0} to {1} {2} {3}'.format(clientid, action.replace('_', ' '), qty, code))
        log.info('{0}'.format(market.symbol))

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

    except Exception as e:
        order_error(clientid, e, kwargs)
        log.error('Unknown error')
        return None, None

    else:

        log.info('Order placement success')

        # Offset resources released and used
        val = qty * price
        account.offset_order_new(code, action, qty, val)

        # Update object status
        filled, average = account.update_order_object(wallet, response, new=True)
        if filled:
            # Offset trade
            account.offset_order_filled(clientid, code, action, filled, average)


# Fetch an order by its orderid
@app.task(bind=True, base=BaseTaskWithRetry, name='Trading_____Send_fetch_orderid')
def send_fetch_orderid(self, account_id, order_id):
    #
    account = Account.objects.get(id=account_id)
    log.bind(account=account.name)
    if self.request.id:
        log.bind(worker=current_process().index)

    order = Order.objects.get(orderid=order_id)
    client = account.exchange.get_ccxt_client(account)
    client.options['defaultType'] = order.market.wallet

    try:
        response = client.fetchOrder(id=order_id, symbol=order.market.symbol)
    except ccxt.OrderNotFound:
        log.error('Unknown order {}'.format(order.clientid), id=order_id)

    except Exception as e:
        log.error('Unknown exception when fetching order {}'.format(order.clientid), id=order_id, e=str(e))

    else:
        return response


# Fetch all open orders of an account
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_fetch_all_open_orders')
def send_fetch_all_open_orders(account_id):
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    for wallet in account.exchange.get_wallets():

        log.info('Fetch all open orders (user n app')

        client.options['defaultType'] = wallet
        client.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

        response = client.fetchOpenOrders()
        return wallet, response


# Send transfer order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_transfer_funds')
def send_transfer(account_id, source, dest, quantity):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    # Generate transfer_id
    alphanumeric = 'abcdefghijklmnopqrstuvwABCDEFGHIJKLMNOPQRSTUVWWXYZ01234689'
    transfer_id = ''.join((random.choice(alphanumeric)) for x in range(5))

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
            return transfer_id


# Send cancellation order
@app.task(base=BaseTaskWithRetry, name='Trading_____Send_cancel_order')
def send_cancel_order(account_id, orderid, wallet=None, symbol=None):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)
    try:
        order = Order.objects.get(orderid=orderid)
        wallet = order.market.wallet
        symbol = order.market.symbol

    except MultipleObjectsReturned:
        log.error('Multiple orders were found')
        return

    except ObjectDoesNotExist:
        update_object = False
    else:
        update_object = True

    finally:
        client.options['defaultType'] = wallet

        try:
            response = client.cancel_order(id=orderid, symbol=symbol)

        except Exception as e:
            log.error('Order canceled failure {0}'.format(orderid), e=str(e))

        else:
            log.info('Order {0} canceled'.format(orderid))
            if update_object:
                # Update object and dataframe
                log.info('Update object of order {0}'.format(orderid))
                account.update_order_object(order.market.wallet, response)
            else:
                log.info('Order {0} has no object'.format(orderid))


@app.task(bind=True, name='Test')
def test(self, loop):
    t = 0
    task_id = self.request.id[:3]
    process_id = current_process().index
    a = Account.objects.get(name='Principal')

    pos = Position.objects.get(account=a, pk=process_id)
    log.info('Task {0} start with process {1}'.format(task_id, process_id))
    pos.size = 1

    while t <= loop:
        pos.size += 1
        pos.save()
        t += 1

    log.info('Task {0} complete'.format(task_id))
