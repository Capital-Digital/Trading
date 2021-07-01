from __future__ import absolute_import, unicode_literals

import asyncio
import configparser
import time
from datetime import datetime, timedelta
from pprint import pprint

import ccxt
import pandas as pd
import requests
import structlog
import urllib3
from celery import chain, group, shared_task, Task, Celery, states
from celery.exceptions import Ignore
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.db import models
from django.db.models import Q
from django.utils import timezone

import capital.celery as celery
from capital.methods import *
from marketsdata.error import *
from marketsdata.methods import *
from marketsdata.models import Exchange, Candle, Market, OrderBook
import strategy.tasks as strategies

log = structlog.get_logger(__name__)

global data
data = {}

# Load config file
config = configparser.ConfigParser()
config.read('capital/config.ini')


class BaseTaskWithRetry(Task):
    autoretry_for = (ccxt.DDoSProtection,
                     ccxt.RateLimitExceeded,
                     ccxt.RequestTimeout,
                     ccxt.NetworkError,
                     urllib3.exceptions.ReadTimeoutError,
                     requests.exceptions.ReadTimeout)

    retry_kwargs = {'max_retries': 5, 'default_retry_delay': 3}
    retry_backoff = True
    retry_backoff_max = 30
    retry_jitter = False


@shared_task()
def run(exid):
    for market in Market.objects.filter(exchange__exid=exid).order_by('symbol'):

        if not market.is_populated():
            continue

        # Create a Celery task that handle retransmissions and run it
        insert_candle_history.s(exid=exid, wallet=market.default_type, symbol=market.symbol,
                                recent=True).apply_async(countdown=10)


# Insert OHLCV candles history
@shared_task(bind=True, name='Markets_____Insert candle history', base=BaseTaskWithRetry)
def insert_candle_history(self, exid, wallet, symbol, recent=None):

    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid, symbol=symbol, wallet=wallet)

    if exchange.is_active():

        def insert(market):

            if self.request.retries > 0:
                log.warning("Download attempt {0}/{1} for {2} at {3}".format(self.request.retries,
                                                                          self.max_retries, market.symbol, exid))

            end = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

            # Create ranges of indexes
            idx_range = pd.date_range(start=start, end=end, freq='H')
            idx_db = pd.DatetimeIndex([i['dt'] for i in Candle.objects.filter(market=market).values('dt')])

            # Check for missing indexes
            idx_miss = idx_range.difference(idx_db)

            # Set limit to it's maximum if start datetime is the exchange launch date. Else set limit to the minimum
            if not exchange.limit_ohlcv:
                raise Exception('There is no OHLCV limit for exchange {0}'.format(exchange.exid))
            elif not start:
                limit = exchange.limit_ohlcv
            else:
                limit = min(exchange.limit_ohlcv, len(idx_range))

            log.bind(limit=limit)

            if len(idx_miss):

                # Calculate since variable
                since_dt = idx_miss[-1] - timedelta(hours=limit - 1)  # add one hour to get the latest candle
                since_ts = int(since_dt.timestamp() * 1000)

                client = exchange.get_ccxt_client()

                # Select market type if necessary
                if exchange.default_types:
                    client.options['defaultType'] = market.default_type

                if exchange.has_credit(market.default_type):
                    client.load_markets(True)
                    market.exchange.update_credit('load_markets', market.default_type)

                while True:

                    try:
                        if exchange.has_credit(market.default_type):
                            response = client.fetchOHLCV(market.symbol, '1h', since_ts, limit)
                            exchange.update_credit('fetchOHLCV', market.default_type)

                    except ccxt.BadSymbol as e:

                        log.exception('Bad symbol')

                        if market.type == 'derivative':
                            if market.derivative == 'future':
                                log.info('Deactivate future market')
                                market.active = False
                        else:
                            log.info('Exclude market')
                            market.excluded = True

                        market.save()

                    except ccxt.ExchangeError as e:
                        print(since_ts, limit)
                        log.exception('Exchange error')
                        return

                    except Exception as e:
                        print(getattr(e, 'message', repr(e)))
                        print(getattr(e, 'message', str(e)))
                        log.exception('An unexpected error occurred', exception=e.__class__.__name__)

                    else:
                        if not len(response):
                            break
                        else:

                            # Extract a list of datetime objects from response
                            idx_ohlcv = pd.DatetimeIndex(
                                [timezone.make_aware(datetime.fromtimestamp(ohlcv[0] / 1000)) for ohlcv in response])

                            # Select datetime objects present in the list of missing datetime objects
                            missing = idx_ohlcv.intersection(idx_miss)

                            # There is at least one candle to insert
                            if len(missing):

                                insert = 0
                                for ohlcv in response:

                                    if len(ohlcv) != 6:
                                        log.error('Unknown OHLCV format')
                                    else:
                                        ts, op, hi, lo, cl, vo = ohlcv
                                        dt = timezone.make_aware(datetime.fromtimestamp(ts / 1000))

                                        if cl is None:
                                            log.warning('Invalid price (None)', timestamp=ts)
                                            continue

                                        if vo is None:
                                            log.warning('Invalid volume (None)', timestamp=ts)
                                            continue

                                    # Prevent insert candle of the current hour
                                    if dt > end:
                                        continue

                                    # convert volumes of spot markets
                                    vo = get_volume_quote_from_ohlcv(market, vo, cl)

                                    # Break the loop if no conversion rule found
                                    if vo is False:
                                        log.error('No rule for volume conversion')
                                        break

                                    try:
                                        Candle.objects.get(market=market, dt=dt)

                                    except ObjectDoesNotExist:

                                        insert += 1
                                        # log.info('Insert candle', dt=dt.strftime("%Y-%m-%d %H:%M:%S"))
                                        Candle.objects.create(market=market,
                                                              exchange=exchange,
                                                              close=cl,
                                                              volume=vo,
                                                              dt=dt)
                                    else:
                                        # Candles returned by exchange can be into database
                                        continue

                                log.info(
                                    'Candles inserted : {0}'.format(insert))  # since=since_dt.strftime("%Y-%m-%d %H:%M"))

                                if insert == 0:
                                    break

                                if since_dt == start:
                                    break

                                elif since_dt < idx_miss[0]:
                                    break

                                # Filter unchecked indexes
                                idx_miss_not_checked = idx_miss[idx_miss < since_dt]

                                # Shift since variable
                                since_dt = idx_miss_not_checked[-1] - timedelta(
                                    hours=limit - 1)  # Remove 1 to prevent holes
                                since_dt = start if since_dt < start else since_dt
                                since_ts = int(since_dt.timestamp() * 1000)

                                if exid == 'bitfinex2':
                                    time.sleep(3)
                                else:
                                    time.sleep(1)

                            else:

                                if since_dt == start:
                                    break

                                elif since_dt < idx_miss[0]:
                                    break

                                else:
                                    break

        market = Market.objects.get(exchange=exchange, default_type=wallet, symbol=symbol)

        # Set start date
        if recent:
            start = market.get_candle_datetime_last()
        else:
            start = timezone.make_aware(datetime.combine(exchange.start_date, datetime.min.time()))

        try:
            insert(market)

        except Exception as e:
            log.exception('Unable to fetch OHLCV')

        else:
            if market.has_gap():
                log.warning('Insert complete with gaps, exclude market')
                market.updated = True
                market.excluded = True

            else:
                log.info('Insert complete')
                market.updated = True
                market.save()


# Save exchange properties
@shared_task(bind=True, base=BaseTaskWithRetry)
def update_properties(self, exid):
    log.debug('Save exchange properties', exchange=exid)

    from marketsdata.models import Exchange
    exchange = Exchange.objects.get(exid=exid)
    client = exchange.get_ccxt_client()

    exchange.version = client.version
    exchange.precision_mode = client.precisionMode
    exchange.api = client.api
    exchange.countries = client.countries
    exchange.urls = client.urls
    exchange.has = client.has
    exchange.timeframes = client.timeframes
    exchange.timeout = client.timeout
    exchange.rate_limit = client.rateLimit
    exchange.credentials = client.requiredCredentials
    exchange.save()

    log.debug('Save exchange properties complete', exchange=exid)


@shared_task(bind=True, name='Markets_____Update information')
def update_information(self):
    """"
    Hourly task to update currencies, markets and exchange properties

    """
    log.info('Update information')

    from marketsdata.models import Exchange
    exchanges = [e.exid for e in Exchange.objects.all()]

    # must use si() signatures
    chains = [chain(update_status.si(exid),
                    update_properties.si(exid),
                    update_currencies.si(exid),
                    update_markets.si(exid),
                    update_funding.si(exid)
                    ) for exid in exchanges]

    res = group(*chains)()

    while not res.ready():
        # print(res.completed_count())
        time.sleep(0.5)

    if res.successful():
        log.info('Update information complete {0} chains'.format(res.completed_count()))

    elif res.failed():
        res.forget()
        log.error('Update information failed')


@celery.app.task(bind=True)
@shared_task(bind=True, name='Markets_____Update market prices and execute strategies')
def update(self):
    """"
    Hourly task to update prices and execute strategies

    """

    from marketsdata.models import Exchange
    from strategy.models import Strategy

    # Create lists of exchanges
    exchanges = [e.exid for e in Exchange.objects.all()]
    exchanges_w_strat = list(set(Strategy.objects.filter(production=True).values_list('exchange__exid', flat=True)))
    exchanges_wo_strat = [e for e in exchanges if e not in exchanges_w_strat]

    if exchanges_w_strat:

        # Create a list of chains
        chains = [chain(
            outtodate_markets.s(exid),
            update_prices.si(exid),
            update_top_markets.si(exid),
            strategies.update.si(exid)
        ) for exid in exchanges_w_strat]

        result = group(*chains).delay()

        # start by updating exchanges with a strategy
        # gp1 = group(update_prices.s(exid) for exid in exchanges_w_strat).delay()

        while not result.ready():
            time.sleep(0.5)

        if result.successful():
            log.info('Markets and strategies update complete')

            # Then update the rest of our exchanges
            result = group([update_prices.s(exid) for exid in exchanges_wo_strat]).delay()

            while not result.ready():
                time.sleep(0.5)

            if result.successful():
                log.info('Rest of exchanges update complete')

            else:
                log.error('Rest of exchanges update failed')

        else:
            log.error('Markets and strategies update failed')

    else:
        log.info('There is no strategy in production. Update prices')
        group(update_prices.s(exid) for exid in exchanges)()


# Set updated flag to False
@shared_task(base=BaseTaskWithRetry)
def outtodate_markets(exid):

    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)

    if exchange.is_active():
        if exchange.is_update_time():

            log.info('Set update flag for markets')

            for market in Market.objects.filter(exchange__exid=exid, excluded=False):
                market.updated = False
                market.save()

            log.info('Set update flag for markets OK')


@shared_task(bind=True, base=BaseTaskWithRetry)
def update_status(self, exid):
    """"
    Save exchange status

    """

    from marketsdata.models import Exchange
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)
    log.info('Save exchange status')

    try:
        response = exchange.get_ccxt_client().fetchStatus()

    except ccxt.ExchangeNotAvailable:

        exchange.status = 'nok'
        exchange.status_at = timezone.now()
        exchange.save()

        log.error('Exchange is {0}'.format(exchange.status))

    else:

        if response['status'] is not None:
            exchange.status = response['status']

            if response['status'] != 'ok':
                log.warning('Exchange status is {0}'.format(exchange.status))
            else:
                log.info('Exchange status is OK')

        if response['updated'] is not None:
            exchange.status_at = timezone.make_aware(datetime.fromtimestamp(response['updated'] / 1e3))
        if response['eta'] is not None:
            exchange.eta = datetime.fromtimestamp(response['eta'] / 1e3)
        if response['url'] is not None:
            exchange.url = response['url']

        exchange.save(update_fields=['status', 'eta', 'status_at', 'url'])


@shared_task(bind=True, base=BaseTaskWithRetry)
def update_currencies(self, exid):
    """
    Create/update currency information from load_markets().currencies

    """

    from marketsdata.models import Exchange, Currency, CurrencyType
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)
    log.info('Update currencies')

    if exchange.is_active():

        client = exchange.get_ccxt_client()

        def update(value):
            code = value['code']

            try:
                curr = Currency.objects.get(code=code, exchange=exchange)
            except MultipleObjectsReturned:
                log.warning('Duplicate currency {0}'.format(code))
                pass
            except ObjectDoesNotExist:

                try:
                    curr = Currency.objects.get(code=code)

                except MultipleObjectsReturned:
                    log.error('Duplicate currency {0}'.format(code))
                    pass

                except ObjectDoesNotExist:

                    # create currency
                    curr = Currency.objects.create(code=code)

                    # set base type
                    curr.type.add(CurrencyType.objects.get(type='base'))

                    # add exchange
                    curr.exchange.add(exchange)

                    log.info('New currency created {0}'.format(code))
                    curr.save()
                else:

                    # add exchange
                    curr.exchange.add(exchange)
                    log.info('Exchange attached to currency {0}'.format(code))
                    curr.save()

            else:
                pass

            # Add or remove CurrencyType.type = quote if needed
            if code in config['MARKETSDATA']['supported_quotes']:
                curr.type.add(CurrencyType.objects.get(type='quote'))
                curr.save()
            else:
                quoteType = CurrencyType.objects.get(type='quote')
                if quoteType in curr.type.all():
                    curr.type.remove(quoteType)
                    curr.save()

            # Add or remove stablecoin = True if needed
            if code in config['MARKETSDATA']['supported_stablecoins']:
                curr.stable_coin = True
                curr.save()
            else:
                curr.stable_coin = False
                curr.save()

        # Iterate through all currencies. Skip OKEx because it returns
        # all currencies characteristics in a single call ccxt.okex.currencies

        if exchange.default_types and exid != 'okex':

            for default_type in exchange.get_default_types():
                log.bind(default_type=default_type)
                client.options['defaultType'] = default_type

                if exchange.has_credit(default_type):
                    client.load_markets(True)
                    exchange.update_credit('load_markets', default_type)

                    for currency, value in client.currencies.items():
                        update(value)

                    log.unbind('default_type')

        else:
            if exchange.has_credit():
                client.load_markets(True)
                exchange.update_credit('load_markets')

                for currency, value in client.currencies.items():
                    update(value)


# Create/update markets information from load_markets().markets
@shared_task(bind=True, base=BaseTaskWithRetry)
def update_markets(self, exid):

    def update(values, default_type=None):

        log.bind(symbol=values['symbol'], base=values['base'], quote=values['quote'])

        if default_type == 'future':
            pprint(values)

        # Check is the base currency is in the database (reported by instance.currencies)
        if values['base'] not in [b.code for b in bases]:
            log.debug("Unknown base currency".format(values['base']))
            return

        # Check if the quote currency is supported CurrencyType
        if values['quote'] not in [q.code for q in quotes]:
            return

        # Set market type
        if 'type' in values:
            ccxt_type_response = values['type']
        else:
            if 'swap' in values:
                if values['swap']:
                    ccxt_type_response = 'swap'
            if 'spot' in values:
                if values['spot']:
                    ccxt_type_response = 'spot'
            if 'future' in values:
                if values['future']:
                    ccxt_type_response = 'future'
            if 'futures' in values:
                if values['futures']:
                    ccxt_type_response = 'futures'
            if 'delivery' in values:
                if values['delivery']:
                    ccxt_type_response = 'delivery'
            if 'option' in values:
                if values['option']:
                    return

        # Prevent insertion of all unlisted BitMEX contract
        if exid == 'bitmex' and not values['active']:
            return

        if 'ccxt_type_response' not in locals():
            pprint(values)
            raise Exception('Can not find market type')

        # Set derivative type and margined coin
        if ccxt_type_response in ['swap', 'future', 'futures', 'delivery']:

            type = 'derivative'
            derivative = get_derivative_type(exid, values)  # perpetual or future
            margined = get_derivative_margined(exid, values)
            delivery_date = get_derivative_delivery_date(exid, values)
            listing_date = get_derivative_listing_date(exid, values)
            contract_value_currency = get_derivative_contract_value_currency(exid, default_type, values)
            contract_value = get_derivative_contract_value(exid, values)

            # Abort if one of these field is None
            if not derivative or not margined:
                if exid == 'binance':
                    if default_type == 'future':
                        if 'BUSD' in values['quote']:
                            print(derivative)
                            print(margined)
                            pprint(values)
                return

        elif ccxt_type_response == 'spot':
            type = ccxt_type_response
            derivative = None

        elif ccxt_type_response == 'option':
            return

        # Test market activity
        if 'active' in values:
            active = values['active']

        ######################
        # Exchanges specific #
        # ####################

        # At this point OKEx ccxt_type_options = None, so we need to set the appropriate ccxt_type_options
        # so we can filter markets and update price with fetch_tickers() later on

        if exid == 'okex':
            if type == 'spot':
                default_type = 'spot'
            elif derivative == 'perpetual':
                default_type = 'swap'
            elif derivative == 'future':
                default_type = 'futures'

        if exid == 'bybit':
            # log.warning('Bybit market activity unavailable')
            active = True

        if exid == 'ftx' and 'MOVE' in values['symbol']:
            return

        # set limits
        amount_min = values['limits']['amount']['min'] if values['limits']['amount']['min'] else None
        amount_max = values['limits']['amount']['max'] if values['limits']['amount']['max'] else None
        price_min = values['limits']['price']['min'] if values['limits']['price']['min'] else None
        price_max = values['limits']['price']['max'] if values['limits']['price']['max'] else None
        cost_min = values['limits']['cost']['min'] if values['limits']['cost']['min'] else None
        cost_max = values['limits']['cost']['max'] if values['limits']['cost']['max'] else None

        # create dictionary
        defaults = {
            'quote': quotes.get(code=values['quote']),
            'base': bases.get(code=values['base']),
            'type': type,
            'ccxt_type_response': ccxt_type_response,
            'default_type': default_type,
            'active': active,
            'maker': values['maker'],
            'taker': values['taker'],
            'amount_min': amount_min,
            'amount_max': amount_max,
            'price_min': price_min,
            'price_max': price_max,
            'cost_min': cost_min,
            'cost_max': cost_max,
            'limits': values['limits'],
            'precision': values['precision'],
            'response': values
        }

        if type == 'derivative':
            defaults['derivative'] = derivative
            defaults['margined'] = margined
            defaults['delivery_date'] = delivery_date
            defaults['listing_date'] = listing_date
            defaults['contract_value_currency'] = contract_value_currency
            defaults['contract_value'] = contract_value

        # create or update market object
        obj, created = Market.objects.update_or_create(symbol=values['symbol'],
                                                       exchange=exchange,
                                                       type=type,
                                                       derivative=derivative,
                                                       defaults=defaults
                                                       )
        if created:
            log.info('Creation of new {1} market {0}'.format(values['symbol'], type))

    from marketsdata.models import Exchange, Market, Currency
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)

    if exchange.is_active():

        log.info('Update markets')

        quotes = Currency.objects.filter(exchange=exchange, type__type='quote')
        bases = Currency.objects.filter(exchange=exchange, type__type='base')
        client = exchange.get_ccxt_client()

        if not quotes.exists():
            raise ConfigurationError('No quote currency attached to exchange {0}, update currencies first'.format(exid))
        if not bases.exists():
            raise ConfigurationError('No base currency attached to exchange {0}, update currencies first'.format(exid))

        # Iterate through supported market types. Skip OKEx because it returns
        # all markets characteristics in a single call ccxt.okex.markets

        if exchange.default_types and exid != 'okex':

            for wallet in exchange.get_default_types():

                client.options['defaultType'] = wallet
                if exchange.has_credit(wallet):
                    client.load_markets(True)
                    exchange.update_credit('load_markets', wallet)

                for market, values in client.markets.items():
                    update(values, default_type=wallet)

        else:
            if exchange.has_credit():
                client.load_markets(True)
                exchange.update_credit('load_markets')

                for market, values in client.markets.items():
                    update(values)

        log.unbind('base', 'quote', 'symbol')
        log.info('Update markets complete')


@shared_task(base=BaseTaskWithRetry)
def update_funding(exid):

    from marketsdata.models import Exchange, Market
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)

    if exchange.is_active():
        if exid == 'binance':

            log.info('Update funding')

            def update(response, market):
                premiumindex = [i for i in response if i['symbol'] == market.response['id']][0]
                market.funding_rate = premiumindex
                market.save()

            client = exchange.get_ccxt_client()

            # Fetch funding rates for USDT-margined contracts
            response = client.fapiPublic_get_premiumindex()
            markets_usdt_margined = Market.objects.filter(exchange=exchange,
                                                          derivative='perpetual',
                                                          default_type='future',
                                                          excluded=False,
                                                          updated=True
                                                          )
            for market in markets_usdt_margined:
                update(response, market)

            # Fetch funding rates for COIN-margined contracts
            response = client.dapiPublic_get_premiumindex()
            markets_coin_margined = Market.objects.filter(exchange=exchange,
                                                          derivative='perpetual',
                                                          default_type='delivery',
                                                          excluded=False,
                                                          updated=True
                                                          )
            for market in markets_coin_margined:
                update(response, market)

            log.info('Update funding complete')


@shared_task(bind=True, base=BaseTaskWithRetry)
def update_prices(self, exid):
    """
    Update market prices and volume every hour at 00:00

    """

    from marketsdata.models import Exchange, Market, Candle
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)

    if exchange.is_active():
        if exchange.is_update_time():
            if exchange.has['fetchTickers']:

                # Update ticker price and volume
                def update(response, market):

                    log.bind(wallet=market.default_type, symbol=market.symbol)

                    # Select response
                    if market.symbol in response:
                        response = response[market.symbol]
                    else:
                        log.warning('Symbol not in response')
                        market.excluded = True
                        market.updated = False
                        market.save()
                        return

                    # Select latest price
                    if 'last' in response or response['last']:
                        last = response['last']
                    else:
                        log.warning('Last price not in response')
                        market.excluded = True
                        market.updated = False
                        market.save()
                        return

                    # Extract 24h rolling volume in USD
                    vo = get_volume_quote_from_ticker(market, response)
                    if vo is False:
                        return

                    # Create datetime object
                    delta = timedelta(minutes=exchange.update_frequency)
                    dt = timezone.now().replace(minute=0, second=0, microsecond=0) - delta

                    try:
                        Candle.objects.get(market=market, exchange=exchange, dt=dt)

                    except Candle.DoesNotExist:
                        Candle.objects.create(market=market,
                                              exchange=exchange,
                                              dt=dt,
                                              close=last,
                                              volume_avg=vo / 24
                                              )
                        market.updated = True
                        market.save()

                    else:
                        log.warning('Candle exists')
                    finally:
                        log.unbind('wallet', 'symbol')

                client = exchange.get_ccxt_client()
                log.info('Prices update start')

                # Create a list of active markets
                markets = Market.objects.filter(exchange=exchange,
                                                excluded=False,
                                                updated=False,
                                                active=True).order_by('symbol', 'default_type')

                if exchange.default_types:

                    # Iterate through default_types
                    for default_type in exchange.get_default_types():
                        client.options['defaultType'] = default_type
                        if exchange.has_credit(default_type):

                            # Load markets
                            client.load_markets(True)
                            exchange.update_credit('load_markets', default_type)
                            if exchange.has_credit(default_type):

                                # FetchTickers
                                response = client.fetch_tickers()
                                exchange.update_credit('fetch_tickers', default_type)

                                for market in markets.filter(default_type=default_type):

                                    # Download OHLCV if gap detected
                                    if market.has_gap():
                                        insert_candle_history(exid,
                                                              market.default_type,
                                                              market.symbol,
                                                              recent=True)
                                    else:
                                        # Else select price and volume from response
                                        update(response, market)

                else:
                    if exchange.has_credit():
                        client.load_markets(True)
                        exchange.update_credit('load_markets')

                        if exchange.has_credit():
                            response = client.fetch_tickers()
                            exchange.update_credit('fetch_tickers')

                            for market in markets:

                                # Download OHLCV if gap detected
                                if market.has_gap():
                                    insert_candle_history(exid,
                                                          market.default_type,
                                                          market.symbol,
                                                          recent=True)
                                else:
                                    # Else select price and volume from response
                                    update(response, market)

                # Set exchange last update time
                exchange.last_price_update_dt = timezone.now()
                exchange.save()

                log.info('Prices update complete')

            else:
                raise('Exchange {0} does not support FetchTickers()'.format(exid))
        else:
            raise('It is not the time to update prices for {0}'.format(exid))


@shared_task(bind=True, base=BaseTaskWithRetry)
def update_top_markets(self, exid):

    import operator
    exchange = Exchange.objects.get(exid=exid)

    if exid == 'binance':

        for wallet in exchange.get_default_types():

            log.info('Set top = True')
            markets = Market.objects.filter(exchange__exid=exid,
                                            quote__code=exchange.dollar_currency,
                                            default_type=wallet,
                                            updated=True,
                                            active=True)

            v = [[m.candle.first().volume_avg, m.candle.first().id] for m in markets if m.candle.first().volume_avg]

            top = sorted(v, key=operator.itemgetter(0))[-20:]

            for pk in [pk for pk in [t[1] for t in top]]:
                market = Candle.objects.get(pk=pk).market
                market.top = True
                market.save()

