import numpy as np

from capital.methods import *
from trading.error import *
from marketsdata.models import Market, Currency
from django.utils import timezone
import structlog
from datetime import timedelta, datetime
from pprint import pprint
import ccxt
import string
import itertools
import random

from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned

log = structlog.get_logger(__name__)

datetime_directives_std = '%Y-%m-%dT%H:%M:%S.%fZ'

dt = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)


def convert_balance(row, wallet, quote):
    price = Currency.objects.get(code=row.name).get_latest_price(quote, 'last')
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


# Return True if cost limit conditions are satisfied otherwise False
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


# Set order status to error
def order_error(clientid, exception, kwargs):
    from trading.models import Order
    order = Order.objects.get(clientid=clientid)

    order.status = 'error'
    if not isinstance(order.response, dict):
        order.response = dict()

    order.response['exception'] = str(exception)
    order.response['dataframe'] = order.account.balances.to_json(orient='index')
    order.save()

    log.info(kwargs)


# Generate pseudonym
def pseudo_generator(count):
    initial_consonants = ({'ch', 'b', 'c', 'br', 'cl', 'cr', 'd', 'dr', 'f', 'fl', 'fr', 'g', 'gl', 'gr', 'p', 'pl',
                           'pr', 's', 'sk', 'sl', 'sm', 'sn', 'm', 'n',
                           'sp', 'st', 'str', 't', 'tr'}
    )

    middle_consonants = ({'b', 'c', 'd', 'f', 'g', 'j', 'k', 'l', 'm', 'n', 'p', 'r', 's', 't', 'v'})

    final_consonants = ({'nette', 'rd', 'tard', 'nce', 'se', 'me', 'tion', 'trude', 'la', 'ture', 'ne', 'nt', 'ux',
                         'ne', 'son', 'pin', 'mot', 's', 'lier', 'mat', 'side', 'lle', 'nal', 'gio', 'çois', 'nan',
                         'blier', 'chat'}
    )

    vowels_first = 'aeiouéè'  # we'll keep this simple
    vowels = 'aeiou'  # we'll keep this simple

    # each syllable is consonant-vowel-consonant "pronounceable"
    syllables = map(''.join, itertools.product(initial_consonants,
                                               vowels_first,
                                               middle_consonants,
                                               vowels,
                                               final_consonants))

    # you could trow in number combinations, maybe capitalized versions...

    return random.sample(set(syllables), count)
