import numpy as np

from capital.methods import *
from trading.error import *
from marketsdata.models import Market, Currency
from django.utils import timezone
import structlog
from datetime import timedelta, datetime
from pprint import pprint
import ccxt
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned

log = structlog.get_logger(__name__)

datetime_directives_std = '%Y-%m-%dT%H:%M:%S.%fZ'

dt = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)


def convert_balance(row, wallet, exchange):
    price = Currency.objects.get(code=row.name).get_latest_price(exchange, 'last')
    return row[wallet] * price


# Format decimal
def format_decimal(counting_mode, precision, n):
    # Rounding mode
    TRUNCATE = 0  # will round the last decimal digits to precision
    ROUND = 1  # will cut off the digits after certain precision

    # Counting mode
    # If exchange.precisionMode is DECIMAL_PLACES then the market['precision'] designates the number of decimal
    # digits after the dot. If exchange.precisionMode is SIGNIFICANT_DIGITS then the market['precision'] designates
    # the number of non-zero digits after the dot. When exchange.precisionMode is TICK_SIZE then the market[
    # 'precision'] designates the smallest possible float fractions. rounding mode

    DECIMAL_PLACES = 2  # 99% of exchanges use this counting mode (Binance)
    SIGNIFICANT_DIGITS = 3  # Bitfinex
    TICK_SIZE = 4  # Bitmex, FTX

    # Padding mode
    NO_PADDING = 5  # default for most cases
    PAD_WITH_ZERO = 6  # appends zero characters up to precision
    return float(ccxt.decimal_to_precision(n,
                                           rounding_mode=0,
                                           precision=precision,
                                           counting_mode=counting_mode,
                                           padding_mode=5
                                           ))


# Return amount limit min or amount limit max if condition is not satisfy
def limit_amount(market, amount):
    # Check amount limits
    if market.limits['amount']['min']:
        if amount < market.limits['amount']['min']:
            return

    if market.limits['amount']['max']:
        if amount > market.limits['amount']['max']:
            log.warning('Amount > limit max', market=market.symbol, type=market.type,
                        amount=amount, limit=market.limits['amount']['max'])
            return market.limits['amount']['max']

    return amount


# Return True if price limit conditions are satisfy otherwise False
def limit_price(market, price):
    # Check price limits
    if market.limits['price']['min']:
        if price < market.limits['price']['min']:
            log.warning('Price < limit min', price=price, limit=market.limits['price']['min'])
            return False

    if market.limits['price']['max']:
        if price > market.limits['price']['max']:
            log.warning('Price > limit max', price=price, limit=market.limits['price']['max'])
            return False

    return True


# Return True if cost limit conditions are satisfy otherwise False
def limit_cost(market, cost):
    # Check cost limits
    if market.limits['cost']['min']:
        if cost < market.limits['cost']['min']:
            return False

    if market.limits['cost']['max']:
        if cost > market.limits['cost']['max']:
            # log.warning('Cost > limit max', cost=amount * price, limit=market.limits['cost']['max'])
            return False

    return True


# Test MIN_NOTIONAL
def test_min_notional(market, action, amount, price):
    cost = amount * price
    min_notional = limit_cost(market, cost)

    if market.exchange.exid == 'binance':

        # If market is spot and if condition is applied to MARKET order
        if market.type == 'spot':
            if market.response['info']['filters'][3]['applyToMarket']:
                if min_notional:
                    return True, None
            else:
                return True, None

        # If market is USDT margined and if verification fails, set reduce_only = True
        elif not min_notional:
            if market.type == 'derivative':
                if market.margined.code == 'USDT':
                    if action == 'close_short':
                        return True, True  # Reduce only = True
        else:
            return True, None
    else:
        if min_notional:
            return True, None

    # In last resort return False
    return False, None
