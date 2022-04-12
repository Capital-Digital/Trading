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


@app.task(name='Trading_Create_accounts_balances')
def create_accounts_balances(strategy_id):
    accounts = Account.objects.filter(strategy__id=strategy_id, active=True)
    for account in accounts:
        get_balances_qty.delay(account.id)


@app.task(base=BaseTaskWithRetry)
def get_balances_qty(account_id):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    log.bind(user=account.name)
    log.info('Get balances qty')

    # Del attribute
    if hasattr(account, 'balances'):
        del account.balances

    # Iterate through exchange's wallets
    for wallet in account.exchange.get_wallets():

        client.options['defaultType'] = wallet
        response = client.fetchBalance()
        for key in ['total', 'free', 'used']:

            # Exclude LBTC from dictionary (staking or earning account)
            dic = {k: v for k, v in response[key].items() if v > 0 and k != 'LDBTC'}

            if dic:
                tmp = pd.DataFrame(index=dic.keys(),
                                   data=dic.values(),
                                   columns=pd.MultiIndex.from_product([[wallet], [key], ['quantity']])
                                   )
                account.balances = tmp if not hasattr(account, 'balances') else pd.concat([account.balances, tmp])
                account.balances = account.balances.groupby(level=0).last()
            else:
                account.balances = pd.DataFrame() if not hasattr(account, 'balances') else account.balances

    log.info('Get balances qty done')


@app.task(base=BaseTaskWithRetry)
def get_positions(account_id):
    #
    account = Account.objects.get(id=account_id)
    client = account.exchange.get_ccxt_client(account)

    log.bind(user=account.name)
    log.info('Get positions')

    response = client.fapiPrivateGetPositionRisk()
    opened = [i for i in response if float(i['positionAmt']) != 0]
    closed = [i for i in response if float(i['positionAmt']) == 0]

    if opened:

        for position in opened:
            market = Market.objects.get(exchange=account.exchange, response__id=position['symbol'], type='derivative')
            code = market.base.code
            quantity = float(position['positionAmt'])
            account.balances.loc[code, ('position', 'open', 'quantity')] = quantity
            account.balances.loc[code, ('position', 'open', 'side')] = 'buy' if quantity > 0 else 'sell'
            account.balances.loc[code, ('position', 'open', 'value')] = quantity * float(position['markPrice'])
            account.balances.loc[code, ('position', 'open', 'leverage')] = float(position['leverage'])
            account.balances.loc[code, ('position', 'open', 'unrealized_pnl')] = float(position['unRealizedProfit'])
            account.balances.loc[code, ('position', 'open', 'liquidation')] = float(position['liquidationPrice'])
            account.save()

    log.info('Get positions done')


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

