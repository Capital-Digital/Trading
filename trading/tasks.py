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
from django.core.exceptions import ObjectDoesNotExist
from timeit import default_timer as timer

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


@shared_task(name='Trading_____Update orders', base=BaseTaskWithRetry)
def update_orders():

    # Iterate through accounts and update open orders
    for account in Account.objects.filter(trading=True, exchange__exid='binance'):

        # Continue trading if trade occurred
        trade = account.fetch_open_orders()
        if trade:
            account.trade(cancel=False)


@shared_task(name='Trading_____Trade account', base=BaseTaskWithRetry)
def trade():
    for account in Account.objects.filter(trading=True, exchange__exid='binance'):
        if datetime.now().hour in account.strategy.execution_hours():
            account.trade()


@shared_task(name='Trading_____Trade single account', base=BaseTaskWithRetry)
def trade_single(pk):
    Account.objects.get(pk=pk).trade()
