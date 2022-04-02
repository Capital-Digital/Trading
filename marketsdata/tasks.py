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
from marketsdata.methods import *
from marketsdata.models import Exchange, Market, Currency, CoinPaprika, Candles, Tickers
import strategy.tasks as task
from trading.models import Account

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


@shared_task(name='Markets_____Update information')
def update_information():
    """"
    Hourly task to update currencies, markets and exchange properties

    """
    log.info('Update start')

    from marketsdata.models import Exchange
    exchanges = [e.exid for e in Exchange.objects.filter(enable=True)]

    # must use si() signatures
    chains = [chain(status.si(exid),
                    properties.si(exid),
                    currencies.si(exid),
                    markets.si(exid),
                    funding.si(exid)
                    ) for exid in exchanges]

    log.info('Execute chain for {0} exchanges'.format(len(exchanges)))
    res = group(*chains)()

    while not res.ready():
        print('wait...')
        time.sleep(1)

    if res.successful():
        log.info('Update complete {0} exchanges'.format(res.completed_count()))

    elif res.failed():
        res.forget()
        log.error('Update failed')


@shared_task(base=BaseTaskWithRetry)
def properties(exid):
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


@shared_task(base=BaseTaskWithRetry)
def status(exid):
    log.bind(exchange=exid)
    log.info('Update status')
    from marketsdata.models import Exchange
    from trading.models import Account
    exchange = Exchange.objects.get(exid=exid)
    a = Account.objects.get(id=1)

    try:
        client = exchange.get_ccxt_client(a)
        response = client.fetchStatus()

    except ccxt.ExchangeNotAvailable:

        exchange.status = 'nok'
        exchange.status_at = timezone.now()
        exchange.save()

        log.error('Exchange status for {1} is {0}'.format(exchange.status, exchange.exid))

    else:

        if response['status'] is not None:
            exchange.status = response['status']

            if response['status'] != 'ok':
                log.warning('Update status for {1} is {0}'.format(exchange.status, exchange.exid))
            else:
                pass
                # log.info('Update status complete')

        if response['updated'] is not None:
            exchange.status_at = timezone.make_aware(datetime.fromtimestamp(response['updated'] / 1e3))
        if response['eta'] is not None:
            exchange.eta = datetime.fromtimestamp(response['eta'] / 1e3)
        if response['url'] is not None:
            exchange.url = response['url']

        exchange.save(update_fields=['status', 'eta', 'status_at', 'url'])


@shared_task(base=BaseTaskWithRetry)
def currencies(exid):
    """
    Create/update currency information from load_markets().currencies

    """

    from marketsdata.models import Exchange, Currency
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)
    log.info('Update currencies')

    if exchange.is_trading():

        client = exchange.get_ccxt_client()

        def update(currency):

            code = currency['code']
            log.bind(code=code)

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

            # Add or remove stablecoin = True if needed
            if code in config['MARKETSDATA']['supported_stablecoins']:
                if not Currency.objects.get(code=code).stable_coin:
                    log.info('Tag currency as stablecoin')
                    curr.stable_coin = True
                    curr.save()
            elif Currency.objects.get(code=code).stable_coin:
                log.info('Untag currency as stablecoin')
                curr.stable_coin = False
                curr.save()

        # Iterate through all currencies. Skip OKEx because it returns
        # all currencies characteristics in a single call ccxt.okex.currencies

        if exchange.wallets and exid != 'okex':

            for wallet in exchange.get_wallets():

                log.bind(wallet=wallet)
                client.options['defaultType'] = wallet

                if exchange.has_credit(wallet):
                    client.load_markets(True)
                    exchange.update_credit('load_markets', wallet)

                    for currency, dic in client.currencies.items():
                        update(dic)

                log.unbind('wallet')

        else:
            if exchange.has_credit():
                client.load_markets(True)
                exchange.update_credit('load_markets')

                for currency, dic in client.currencies.items():
                    update(dic)


@shared_task(base=BaseTaskWithRetry)
def markets(exid):
    def update():

        def is_known_currency(code):
            try:
                Currency.objects.get(exchange=exchange, code=code)
            except ObjectDoesNotExist:
                log.warning('Unknown currency {0}'.format(code))
                return False
            else:
                return True

        def get_market_type():
            try:
                if exid == 'binance':
                    if 'contractType' in response['info']:
                        type = 'derivative'
                    else:
                        type = 'spot'
            except KeyError:
                pprint(response)
                log.exception('Cannot find market type')
            else:
                return type

        def get_status():
            if exid == 'binance':
                if 'status' in response['info']:
                    market_status = response['info']['status'].lower()
                elif 'contractStatus' in response['info']:
                    market_status = response['info']['contractStatus'].lower()
            elif exid == 'bybit':
                market_status = response['info']['status'].lower()
            elif exid == 'ftx':
                market_status = 'enabled' if response['info']['enabled'] else 'disabled'
            if 'market_status' in locals():
                return market_status
            else:
                pprint(response)
                log.error('Cannot find status')

        def is_trading():
            if exid == 'binance':
                if status == 'trading':
                    trading = True
                else:
                    trading = False
            return trading

        def get_contract_type():
            try:
                if exid == 'binance':
                    contract_type = response['info']['contractType'].lower()

            except KeyError:
                pprint(response)
                log.exception('Cannot find contract type')
            else:
                return contract_type

        def get_margined():
            try:
                if exid == 'binance':
                    margined = response['info']['marginAsset']
            except KeyError:
                pprint(response)
                log.exception('Cannot find margin asset')
            else:
                return Currency.objects.get(code=margined)

        def get_delivery_date():
            try:
                if exid == 'binance':
                    delivery_date = response['info']['deliveryDate']
                    delivery_date = timezone.make_aware(datetime.fromtimestamp(float(delivery_date) / 1000))
            except KeyError:
                pprint(response)
                log.exception('Cannot find delivery date')
            else:
                return delivery_date

        def get_listing_date():
            try:
                if exid == 'binance':
                    listing_date = response['info']['onboardDate']
                    listing_date = timezone.make_aware(datetime.fromtimestamp(float(listing_date) / 1000))
            except KeyError:
                pprint(response)
                log.exception('Cannot find listing date')
            else:
                return listing_date

        def get_contract_size():
            try:
                if exid == 'binance':
                    if 'contractSize' in response['info']:
                        contract_size = response['info']['contractSize']
                    else:
                        contract_size = None
            except KeyError:
                pprint(response)
                log.exception('Cannot find contract size')
            else:
                return contract_size

        def get_contract_currency():
            try:
                if exid == 'binance':
                    if 'contract_val_currency' in response['info']:
                        contract_currency = response['info']['contract_val_currency']
                    else:
                        contract_currency = None
            except KeyError:
                pprint(response)
                log.exception('Cannot find contract currency')
            else:
                return contract_currency

        def is_halted():
            if exid == 'binance':
                if status in ['close', 'break']:
                    halt = True
                else:
                    halt = False
            if exid == 'ftx':
                if status == 'disabled':
                    halt = True
                else:
                    halt = False
            return halt

        base = response['base']
        quote = response['quote']
        symbol = response['symbol']

        log.bind(symbol=symbol)

        if is_known_currency(base):
            if is_known_currency(quote):
                if quote in exchange.get_supported_quotes():

                    market_type = get_market_type()
                    status = get_status()

                    if is_halted():
                        try:
                            market = Market.objects.get(exchange=exchange,
                                                        wallet=wallet,
                                                        symbol=symbol
                                                        )
                        except ObjectDoesNotExist:
                            return
                        else:
                            log.info('Delete halted market')
                            market.delete()
                            return

                    # Set derivative specs
                    if market_type == 'derivative':
                        contract_type = get_contract_type()
                        margined = get_margined()
                        delivery_date = get_delivery_date()
                        listing_date = get_listing_date()
                        contract_currency = get_contract_currency()
                        contract_size = get_contract_size()

                    # set limits
                    amount_min = response['limits']['amount']['min'] if response['limits']['amount']['min'] else None
                    amount_max = response['limits']['amount']['max'] if response['limits']['amount']['max'] else None
                    price_min = response['limits']['price']['min'] if response['limits']['price']['min'] else None
                    price_max = response['limits']['price']['max'] if response['limits']['price']['max'] else None
                    cost_min = response['limits']['cost']['min'] if response['limits']['cost']['min'] else None
                    cost_max = response['limits']['cost']['max'] if response['limits']['cost']['max'] else None

                    # create dictionary
                    defaults = {
                        'wallet': wallet,
                        'quote': Currency.objects.get(exchange=exchange, code=quote),
                        'base': Currency.objects.get(exchange=exchange, code=base),
                        'type': market_type,
                        'status': status,
                        'trading': is_trading(),
                        'amount_min': amount_min,
                        'amount_max': amount_max,
                        'price_min': price_min,
                        'price_max': price_max,
                        'cost_min': cost_min,
                        'cost_max': cost_max,
                        'limits': response['limits'],
                        'precision': response['precision'],
                        'response': response
                    }

                    if market_type == 'derivative':
                        defaults['contract_type'] = contract_type
                        defaults['margined'] = margined
                        defaults['delivery_date'] = delivery_date
                        defaults['listing_date'] = listing_date
                        defaults['contract_currency'] = contract_currency
                        defaults['contract_value'] = contract_size

                    # create or update market object
                    obj, created = Market.objects.update_or_create(exchange=exchange,
                                                                   wallet=wallet,
                                                                   symbol=symbol,
                                                                   defaults=defaults
                                                                   )
                    if created:
                        log.info('Create new market')

            else:
                return
        else:
            return

        log.unbind('symbol')

    from marketsdata.models import Exchange, Market, Currency
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)

    log.info('Update market')

    if exchange.is_trading():

        client = exchange.get_ccxt_client()

        # Iterate through supported wallets
        if exchange.wallets:

            for wallet in exchange.get_wallets():

                client.options['defaultType'] = wallet
                if exchange.has_credit(wallet):
                    client.load_markets(True)
                    exchange.update_credit('load_markets', wallet)

                log.bind(wallet=wallet)
                log.info('Update markets {0}'.format(wallet))

                if exid in ['binance']:
                    for market, response in client.markets.items():
                        update()
                else:
                    log.info('Skip update')

                # Delete markets not reported by the exchange
                unlisted = Market.objects.filter(exchange=exchange,
                                                 wallet=wallet
                                                 ).exclude(symbol__in=list(client.markets.keys()))
                if unlisted:
                    log.info('Delete {0} unlisted market(s)'.format(unlisted.count()))
                    unlisted.delete()

                log.unbind('wallet')

        else:
            wallet = None
            if exchange.has_credit():
                client.load_markets(True)
                exchange.update_credit('load_markets')

                if exid == 'binance':
                    for market, response in client.markets.items():
                        update()
                else:
                    log.info('Skip update')

                # Delete markets not reported by the exchange
                unlisted = Market.objects.filter(exchange=exchange).exclude(symbol__in=list(client.markets.keys()))
                if unlisted:
                    log.info('Delete {0} unlisted market(s)'.format(unlisted.count()))
                    unlisted.delete()

        # log.info('Update markets complete')
        log.unbind('exchange')


@shared_task(base=BaseTaskWithRetry)
def funding(exid):
    from marketsdata.models import Exchange, Market
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)

    if exchange.is_trading():
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
                                                          contract_type='perpetual',
                                                          wallet='future',
                                                          updated=True
                                                          )
            for market in markets_usdt_margined:
                update(response, market)

            # Fetch funding rates for COIN-margined contracts
            response = client.dapiPublic_get_premiumindex()
            markets_coin_margined = Market.objects.filter(exchange=exchange,
                                                          contract_type='perpetual',
                                                          wallet='delivery',
                                                          updated=True
                                                          )
            for market in markets_coin_margined:
                update(response, market)

            # log.info('Update funding complete')


# Insert candles history
@shared_task(base=BaseTaskWithRetry, name='Markets_____Fetch candle history')
def fetch_candle_history(exid):
    exchange = Exchange.objects.get(exid=exid)
    markets = Market.objects.filter(exchange=exchange,
                                    trading=True,
                                    quote__code__in=['USDT', 'BUSD']
                                    ).order_by('base')

    if exchange.is_trading():

        for market in markets:

            log.info('Fetch candles for {0} {1}'.format(market.symbol, market.type))

            now = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
            directive = '%Y-%m-%dT%H:%M:%SZ'
            limit = exchange.limit_ohlcv

            # Prepare client
            client = exchange.get_ccxt_client()
            client.options['defaultType'] = market.wallet if exchange.wallets else None

            # Candle already exist ?
            queryset = Candles.objects.filter(market=market)
            if queryset:

                # Get latest timestamp
                end = queryset.order_by('-year', '-semester')[0].data[-1][0]
                dt = datetime.strptime(end, directive).replace(tzinfo=pytz.UTC)

                if dt == now:
                    log.info('Market is updated')
                    continue
                else:
                    log.info('Market need an update')

            else:
                # Set datetime
                dt = exchange.start_date
                log.info('Market is new')

            # timestamp not updated ?
            if dt < now:

                # Add 1 hour and convert to milliseconds
                since = int((dt + timedelta(hours=1)).timestamp() * 1000)

                while True:

                    try:
                        # Fetch candles data
                        data = client.fetchOHLCV(market.symbol, '1h', since, 100)

                    except ccxt.BadSymbol as e:
                        log.warning('Unknown symbol {0}, break while loop'.format(market.symbol))
                        break

                    except ccxt.ExchangeError as e:
                        log.exception('Exchange error', exception=e)
                        return

                    except Exception as e:
                        log.exception('An unexpected error occurred', exception=e)
                        continue

                    else:

                        if data:

                            log.info('Market has {0} new candles'.format(len(data)))

                            # Drop the last candle if it's timestamp of the current hour
                            unix_time_last = data[-1][0]
                            end_dt = timezone.make_aware(datetime.fromtimestamp(unix_time_last / 1000))
                            if end_dt > now:
                                del data[-1]
                                unix_time_last = data[-1][0]

                            # Convert timestamp from ms to string
                            for idx, i in enumerate(data):
                                dt_aware = timezone.make_aware(datetime.fromtimestamp(i[0] / 1000))
                                st = dt_aware.strftime(directive)
                                data[idx][0] = st

                            # Iterate through years
                            for year in list(range(dt.year, timezone.now().year + 1)):

                                # Create lists of string '2022-01', '2022-02', '2022-03', etc.
                                filter_1 = [str(year) + '-' + i for i in ['01', '02', '03', '04', '05', '06']]
                                filter_2 = [str(year) + '-' + i for i in ['07', '08', '09', '10', '11', '12']]

                                # Filter records by semester 1 (January to July) and 2 (August to December)
                                data_1 = [i for i in data if i[0][:7] in filter_1]
                                data_2 = [i for i in data if i[0][:7] in filter_2]

                                for i in range(1, 3):
                                    var = eval('data_' + str(i))
                                    if var:

                                        try:
                                            obj = Candles.objects.get(year=year, semester=i, market=market)

                                        except ObjectDoesNotExist:

                                            # Create new object for semester 1
                                            Candles.objects.create(year=year,
                                                                   semester=i,
                                                                   market=market,
                                                                   data=var)

                                            log.info('Create object for {0} {1}-S{2}'.format(market.symbol, year, i))

                                        else:

                                            # Remove duplicate records
                                            diff = [c for c in var if c not in obj.data]

                                            # Concatenate the two lists if new candle is found
                                            if diff:
                                                obj.data += diff
                                                obj.save()
                                                log.info('Update object {0} {1}-S{2}'.format(market.symbol, year, i))

                                            else:
                                                log.info('No new candles received')

                            # Convert the latest timestamp to Python datetime object
                            # and break the while loop when the last candle is collected
                            dt = datetime.strptime(data[-1][0], directive).replace(tzinfo=pytz.UTC)
                            if dt >= now:
                                log.info('Market is updated')
                                break

                            else:
                                # Else add 1000 hours to since and restart loop
                                since = unix_time_last + (60 * 60 * 1000)

                        else:

                            # if an empty object is returned by exchange return 30 day in the past
                            if 'empty' not in locals():
                                since = int((now - timedelta(days=60)).timestamp() * 1000)
                                empty = True

                            else:
                                log.warning('Market not fully update')
                                del empty
                                break
    else:

        log.warning('Exchange {0} is not trading'.format(exchange.exid))


# Group and execute exchange's tickers snapshot update
@shared_task(base=BaseTaskWithRetry, name='Markets_____Hourly tasks')
def hourly_tasks():
    res = group(insert_current_tickers.s(exid) for exid in ['binance'])()
    while not res.ready():
        time.sleep(0.5)

    if res.successful():
        log.info('Tickers insert complete')
        for account in Account.objects.filter(active=True, exchange__exid='binance'):
            if datetime.now().hour in account.strategy.execution_hours():
                account.trade()

    else:
        log.error('Hourly tasks failed')


# Execute group of chains
@shared_task(base=BaseTaskWithRetry, name='Markets_____Update')
def update():

    # Return a list of exid
    exchanges = list(Exchange.objects.filter(exid='binance').values_list('exid', flat=True))

    chains = [chain(insert_current_tickers.s(exid),
                    update_weights(exid)
                    ) for exid in exchanges]

    log.info('Group and execute chains')

    res = group(*chains)()

    while not res.ready():
        print('wait group 1...')
        time.sleep(1)

    if res.successful():
        log.info('Update complete')

    elif res.failed():
        res.forget()
        log.error('Update failed')


# Group accounts and execute trades
@shared_task(base=BaseTaskWithRetry, name='Markets_____Accounts update')
def trade(strategy):
    from trading.models import Account
    accounts = Account.objects.filter(strategy__name=strategy)

    # Execute trades
    res = group(account.trade() for account in accounts)

    while not res.ready():
        print('wait group 3...')
        time.sleep(1)

    if res.successful():
        log.info('Accounts update complete')

    else:
        log.error('Accounts update failed')


# Insert current tickers
@shared_task(base=BaseTaskWithRetry, name='Markets_____Insert tickers')
def insert_current_tickers(exid):
    log.info('Start tickers insertion for {0}'.format(exid))

    def insert(data, wallet=None):

        # Recreate dictionaries with desired keys
        data = [data[i] for i in data]
        data = [
            {k: d[k] for k in ['symbol', 'bid', 'ask', 'last', 'bidVolume', 'askVolume', 'quoteVolume', 'baseVolume']}
            for d in data]

        symbols = [i['symbol'] for i in data if 'USDT' in i['symbol']]
        symbols.sort()

        log.info('Insert {0} tickers'.format(len(symbols)))

        # Iterate through symbols
        for symbol in symbols:

            dic = [i for i in data if i['symbol'] == symbol][0]
            dic['timestamp'] = int(dt.timestamp())
            dic['datetime'] = timestamp_st

            args = dict(exchange=exchange,
                        symbol=dic['symbol'],
                        wallet=wallet
                        )

            # Select market with Binance's symbol
            if exid == 'binance' and wallet == 'delivery':
                del args['symbol']
                args['response__info__symbol'] = dic['symbol']

            if not wallet:
                del args['wallet']

            try:
                market = Market.objects.get(**args)

            except ObjectDoesNotExist:
                continue

            else:

                try:
                    obj = Tickers.objects.get(year=year, semester=semester, market=market)

                except ObjectDoesNotExist:

                    log.info('Create tickers {0} {1} {2}'.format(market.symbol, year, semester))

                    # Create new object
                    Tickers.objects.create(year=year,
                                           semester=semester,
                                           market=market,
                                           data=[dic]
                                           )

                else:

                    # Avoid duplicate records
                    if timestamp_st not in [d['datetime'] for d in obj.data]:

                        # log.info('Update tickers {0} {1} {2}'.format(market.symbol, year, semester))

                        # Concatenate the two lists
                        obj.data.append(dic)
                        obj.save()

                    else:

                        log.info('Tickers for {0} updated'.format(market.symbol))

    dt = timezone.now().replace(minute=0, second=0, microsecond=0)
    timestamp_st = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
    year = dt.year
    semester = 1 if dt.month <= 6 else 2

    exchange = Exchange.objects.get(exid=exid)
    if exchange.is_trading():
        if exchange.has['fetchTickers']:

            client = exchange.get_ccxt_client()
            if exchange.wallets:
                for wallet in exchange.get_wallets():
                    log.info('Fetch tickers for {0}'.format(wallet))

                    client.options['defaultType'] = wallet
                    data = client.fetch_tickers()
                    insert(data, wallet)

            else:
                data = client.fetch_tickers()
                insert(data)

    log.info('Insertion for {0} complete'.format(exid))


@shared_task(base=BaseTaskWithRetry, name='Markets_____Fetch CoinPaprika listing history')
def fetch_coinpaprika_listing_history():
    log.info('Start fetching CoinPaprika records')

    from coinpaprika import client as Coinpaprika
    client = Coinpaprika.Client()
    listing = client.coins()
    listing = [i for i in listing if i['rank'] < 400 and i['is_active']]
    directive = '%Y-%m-%dT%H:%M:%SZ'

    for coin in listing:

        try:

            code = coin['symbol']
            currency = Currency.objects.get(code=code)

        except ObjectDoesNotExist:
            continue

        else:
            qs = CoinPaprika.objects.filter(currency=currency)
            now = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

            if not qs:
                # Set start datetime
                start_dt = datetime.strptime('2020-01-01T00:00:00Z', directive).replace(tzinfo=pytz.UTC)
            else:

                # Get latest timestamp
                end = qs.order_by('-year', '-semester')[0].data[-1]['timestamp']
                end_dt = datetime.strptime(end, directive).replace(tzinfo=pytz.UTC)

                if end_dt == now:
                    log.info('Object {0} is updated'.format(code))
                    continue

                else:
                    # Set start datetime
                    start_dt = end_dt + timedelta(hours=1)

            while start_dt < now:

                start = start_dt.strftime(directive)
                log.info('Fetch historical data for {0} starting {1}'.format(coin['symbol'], start))
                data = client.historical(coin['id'], start=start, interval='1h', limit=5000)

                if data:

                    # Iterate through years
                    for year in get_years('2020-01-01T00:00:00Z'):

                        filter_1 = [str(year) + '-' + i for i in ['01', '02', '03', '04', '05', '06']]
                        filter_2 = [str(year) + '-' + i for i in ['07', '08', '09', '10', '11', '12']]

                        # And filter records by semester
                        data_1 = [i for i in data if i['timestamp'][:7] in filter_1]
                        data_2 = [i for i in data if i['timestamp'][:7] in filter_2]

                        for i in range(1, 3):

                            var = eval('data_' + str(i))
                            if var:

                                try:
                                    obj = CoinPaprika.objects.get(year=year, semester=i, currency=currency)

                                except ObjectDoesNotExist:

                                    log.info('Create object {0} {1} {2}'.format(currency.code, year, i))

                                    # Create new object for semester 1
                                    CoinPaprika.objects.create(year=year,
                                                               semester=i,
                                                               name=coin['name'],
                                                               currency=currency,
                                                               data=var
                                                               )

                                else:
                                    # Remove duplicate records
                                    diff = [i for i in var if i not in obj.data]

                                    if diff:

                                        log.info('Update object {0} {1} {2}'.format(currency.code, year, i))

                                        # Concatenate the two lists
                                        obj.data += diff
                                        obj.save()

                                    else:
                                        log.info('Object {0} {1} {2} is up to date'.format(currency.code, year, i))

                    # Update start datetime
                    start_dt = datetime.strptime(data[-1]['timestamp'], directive).replace(tzinfo=pytz.UTC)
                    start_dt += timedelta(hours=1)

                else:
                    log.info('No data returned, add 30 days...')
                    start_dt += timedelta(days=30)

                time.sleep(1)

            log.info('While loop break')


@shared_task(base=BaseTaskWithRetry, name='Markets_____Insert CoinPaprika current listing')
def insert_coinpaprika_current_listing():
    log.info('Insertion of CoinPaprika listing start')

    from coinpaprika import client as Coinpaprika
    client = Coinpaprika.Client()
    listing = client.tickers()
    listing = [i for i in listing if i['rank'] < 400]

    # Sort indices
    ids = [i['id'] for i in listing]
    ids.sort()
    for c in ids:

        # Select dictionary
        i = [v for v in listing if v['id'] == c][0]

        code = i['symbol']
        name = i['name']
        price = i['quotes']['USD']['price']
        volume_24h = i['quotes']['USD']['volume_24h']
        market_cap = i['quotes']['USD']['market_cap']

        # Create timestamp
        timestamp = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        timestamp_st = timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

        record = dict(
            price=price,
            timestamp=timestamp_st,
            volume_24h=volume_24h,
            market_cap=market_cap
        )

        if None in record.values():
            pprint(record)
            raise Exception('None detect in {0}'.format(code))

        # Get year and semester
        year = timestamp.year
        semester = 1 if timestamp.month <= 6 else 2

        try:
            currency = Currency.objects.get(code=code)

        except ObjectDoesNotExist:
            continue

        else:

            try:
                obj = CoinPaprika.objects.get(year=year, semester=semester, currency=currency)

            except ObjectDoesNotExist:

                log.info('Create {0} {1} {2}'.format(currency.code, year, semester))

                # Create new object
                CoinPaprika.objects.create(year=year,
                                           semester=semester,
                                           name=name,
                                           currency=currency,
                                           data=[record]
                                           )

            else:

                # if timestamp_st not in [d['timestamp'] for d in obj.data]:
                if timestamp_st != obj.data[-1]['timestamp']:

                    # Concatenate the two lists
                    obj.data.append(record)
                    obj.save()

                else:

                    log.info('{0} already updated'.format(currency.code))

    log.info('Insertion of Paprika listing complete')


@shared_task(name='Exe')
def exe():

    t = group(run.s(i) for i in ['binance', 'ftx'])()

    while not t.ready():
        print('wait group 0...')
        time.sleep(1)

    if t.successful():

        log.info('group 1 update complete')


@shared_task(name='Run tasks')
def run(exid):

    res = task1.delay(exid)

    while not res.ready():
        print('wait tickers update', exid)
        time.sleep(1)

    if res.successful():

        log.info('Ticker update complete', exid)
        from strategy.models import Strategy

        # Select strategies
        strategies = list(Strategy.objects.filter(exchange__exid=exid).values_list('exid', flat=True))
        s = group(update_weights.s(strategy) for strategy in strategies)()

        while not s.ready():
            print('wait strategy group')
            time.sleep(1)

        if s.successful():
            log.info('Strategies update complete')

            from trading.models import Account
            accounts = Account.objects.filter(strategy__exchange__exid=exid)

            # Execute trades
            u = group(account.trade() for account in accounts)()

            while not u.ready():
                print('wait group 3...')
                time.sleep(1)

            if u.successful():
                log.info('group 3 update complete')

    else:
        log.error('group 1 update failed')


@shared_task()
def task1(exid):
    print('task1 for', exid)


# Update weights of a group of strategies
@shared_task(base=BaseTaskWithRetry, name='Markets_____Strategies update')
def update_weights(name):

    # Select strategies
    from strategy.models import Strategy
    strategy = Strategy.objects.filter(name=name)
    # strategy.execute('tickers', 10 * 24)
    print('Execute strategy', strategy.name)
