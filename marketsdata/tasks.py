from __future__ import absolute_import, unicode_literals

import asyncio
import configparser
import time
from datetime import datetime, timedelta
from pprint import pprint

from billiard.process import current_process
from capital.celery import app

import ccxt
import pandas as pd
import requests
import structlog
import urllib3

from celery import chain, group, shared_task, Task, Celery, states
from celery.result import AsyncResult, allow_join_result
from celery.exceptions import Ignore
from celery.signals import task_success, task_postrun, task_failure

from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.db import models
from django.db.models import Q
from django.utils import timezone

import itertools

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


# Create a custom task class
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

        from strategy.models import Strategy
        strategies = Strategy.objects.filter(exchange__exid='binance', production=True)
        gp = group(run_strategy.s(s.id) for s in strategies)()
        while not gp.ready():
            time.sleep(0.5)

        if gp.successful():

            accounts = Account.objects.filter(active=True, exchange__exid='binance')
            gp_acc = group(run_account.s(a.id) for a in accounts)()
            while not gp_acc.ready():
                time.sleep(0.5)

            if gp_acc.successful():
                log.info('Update update success')

            else:
                log.info('Update update failure')

        else:
            log.error('Strategies failed')

    else:
        log.error('Hourly tasks failed')


# Execute groups
@app.task(bind=True, base=BaseTaskWithRetry, name='Update')
def update(self):
    print(' ')
    print('TASK STARTING: {0.name} [{0.request.id}]'.format(self))
    print(' ')

    # Return a list of exid
    exchanges = list(Exchange.objects.filter(exid='binance').values_list('exid', flat=True))

    # Create job
    res = group(chain_tickers_strategy.s(exid).set(queue='default') for exid in ['ex0', 'ex1'])()

    while not res.ready():
        print('wait group tickers...')
        time.sleep(1)

    if res.successful():
        log.info(' ')
        log.info('Group ticker complete')
        log.info(' ')

    elif res.failed():
        log.info(' ')
        # group_tickers.forget()
        log.error('Group ticker failed')
        log.info(' ')


# Tickers update
@app.task(bind=True, name='Markets_____Tickers')
def chain_tickers_strategy(self, exid):
    print(' ')
    print('TICKERS update for {0} start'.format(self))

    job = insert_current_tickers.s(exid, test=True).apply_async(queue='default')

    while not job.ready():
        time.sleep(1)

    if job.successful():

        print('TICKERS update for {0} success'.format(self))
        print(' ')

        # Group strategies
        from strategy.models import Strategy
        strategies = Strategy.objects.filter(exchange__exid=exid)
        res = group(chain2.s(exid, i) for i in range(2)).apply_async(queue='slow')

        while not res.ready():
            time.sleep(1)

        if res.successful():
            print('EXID {0} CHAIN success...'.format(exid))
            print(' ')

        else:
            print('EXID {0} CHAIN failed...'.format(exid))
            print(' ')

    else:
        print('TICKERS update for {0} failed'.format(self))
        print(' ')


# Strategies update
@app.task(bind=True, name='Strategy_execution')
def run_strategy(self, strategy_id):

    from strategy.models import Strategy
    strategy = Strategy.objects.get(id=strategy_id)

    log.info('Update strategy {0}'.format(strategy.name), s=strategy.name)
    log.info('Process {0}'.format(current_process().index), s=strategy.name)

    exchange = Exchange.objects.get(exid='binance')
    data = exchange.load_data(10 * 24, strategy.get_codes_long())
    strategy.execute(data)


# Strategies update
@app.task(bind=True, name='Account_execution')
def run_account(self, account_id):

    from trading.models import Account
    account = Account.objects.get(id=account_id)

    log.info('Process {0}'.format(current_process().index), account=account.name)
    account.trade(cancel=True)


# Strategies update
@app.task(bind=True, name='Strategy execution')
def chain2(self, exid, strategy_id):
    print('EXID {1} STRATEGY {0} CHAIN start...'.format(strategy_id, exid))

    job = run_strategy.s(exid, strategy_id).apply_async(queue='default')

    while not job.ready():
        time.sleep(1)

    if job.successful():

        print('EXID {1} STRATEGY {0} CHAIN success...'.format(strategy_id, exid))

        acc = group(run_account.s(exid, strategy_id, account_id) for account_id in range(4)).apply_async(queue='slow')

        while not acc.ready():
            time.sleep(1)

        if acc.successful():
            print('EXID {1} STRATEGY {0} ACCOUNT CHAIN success...'.format(strategy_id, exid))

        else:
            print('EXID {1} STRATEGY {0} ACCOUNT CHAIN failed...'.format(strategy_id, exid))

    else:
        print('EXID {1} STRATEGY {0} CHAIN failed...'.format(strategy_id, exid))


# Strategies update
@app.task(bind=True, name='Markets_____Acc')
def group_account(self, strategy_id):
    from trading.models import Account
    accounts = Account.objects.filter(strategy__id=strategy_id)


# Group accounts and execute trades
@shared_task(base=BaseTaskWithRetry, name='Markets_____Trade')
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
@shared_task(bind=True, base=BaseTaskWithRetry, name='Markets_____Insert tickers')
def insert_current_tickers(self, exid):

    print('Tickers insertion for {0} start'.format(exid))

    def insert(data, wallet=None):

        symbols = [s for s in data.keys() if '/USDT' in s]
        symbols.sort()

        for symbol in symbols:

            # Create dictionary
            dic = {k: data[symbol][k] for k in ['bid',
                                                'ask',
                                                'last',
                                                'bidVolume',
                                                'askVolume',
                                                'quoteVolume',
                                                'baseVolume']}
            dic['timestamp'] = int(dt.timestamp())

            args = dict(exchange=exchange,
                        symbol=symbol,
                        wallet=wallet
                        )

            # Replace symbol name in the query if delivery
            if exid == 'binance' and wallet == 'delivery':
                del args['symbol']
                args['response__info__symbol'] = data[symbol]['symbol']
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
                                           data={dt_string: dic}
                                           )
                else:
                    if dt_string not in obj.data.keys():
                        obj.data[dt_string] = dic
                        obj.save()

                    else:
                        pass
                        # log.info('Dictionary already updated for {0}'.format(market.symbol))

    dt = timezone.now().replace(minute=0, second=0, microsecond=0)
    dt_string = dt.strftime('%Y-%m-%dT%H:%M:%SZ')
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


@app.task
def error_handler(uuid):
    result = AsyncResult(uuid)
    exc = result.get(propagate=False)
    print('Task {0} raised exception: {1!r}\n{2!r}'.format(
        uuid, exc, result.traceback))


@app.task(bind=True,
          name='sum-of-two-numbers',
          default_retry_delay=1,
          time_limit=120,
          max_retries=2
          )
def add(self, x, y):
    print(self.AsyncResult(self.request.id).state)
    try:
        return x / 0
    except Exception as e:
        log.error('Division by zero')
        raise self.retry(exc=e)


@app.task
def update():
    exchanges = Exchange.objects.filter(exid__in=['binance']).values_list('exid', flat=True)
    for exchange in exchanges:
        loader.delay(exchange)


@app.task
def loader(exid):
    log.info('Update tickers for exchange : {0}'.format(exid))
    log.info('###########################')
    log.info(' ')
    log.info(' ')
    return True


@task_success.connect(sender=loader)
def monitor(sender, **kwargs):
    log.info('task scan completed - %s', kwargs['result'])


@task_postrun.connect
def task_postrun_handler(task_id=None, **kwargs):
    log.info('CONNECT {0}'.format(task_id))
    log.info('Task {0}'.format(kwargs['task']))
    log.info('Task_id {0}'.format(kwargs['task_id']))
    log.info('args {0}'.format(kwargs['args']))
    log.info('retval {0}'.format(kwargs['retval']))
    log.info('state {0}'.format(kwargs['state']))


def test(self, exid):

    from strategy.models import Strategy
    strategies = Strategy.objects.filter(exchange__id=exid)
    exchange = Exchange.objects.get(exid=exid)

    # Create list of desired codes
    longs = list(set(s.get_codes_long() for s in strategies))

    # Load prices and volumes
    prices, volumes = exchange.load_tickers(10*24, longs)

    index = prices.index[:-1]

    while datetime.now().minute != 0:
        time.sleep(1)

    client = exchange.get_ccxt_client()
    tickers = client.fetch_tickers()
