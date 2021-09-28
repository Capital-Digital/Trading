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
import strategy.tasks as task

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


@shared_task(name='Markets_____Update market prices and execute strategies')
def update():
    from marketsdata.models import Exchange
    from strategy.models import Strategy

    # Create lists of exchanges
    exchanges = [e.exid for e in Exchange.objects.filter(enable=True)]
    exchanges_w_strat = list(set(Strategy.objects.filter(production=True).values_list('exchange__exid', flat=True)))
    exchanges_wo_strat = [e for e in exchanges if e not in exchanges_w_strat]

    if exchanges_w_strat:

        # Create a list of chains
        chains = [chain(
            prices.si(exid),
            top_markets.si(exid),
            task.strategies.si(exid)
        ) for exid in exchanges_w_strat]

        result = group(*chains).delay()

        # start by updating exchanges with a strategy
        # gp1 = group(update_prices.s(exid) for exid in exchanges_w_strat).delay()

        while not result.ready():
            time.sleep(0.5)

        if result.successful():
            log.info('Markets and strategies update complete')

            # Then update the rest of our exchanges
            result = group([prices.s(exid) for exid in exchanges_wo_strat]).delay()

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
        group(prices.s(exid) for exid in exchanges)()


@shared_task(name='Markets_____Update prices', base=BaseTaskWithRetry)
def update_price():
    exchanges = Exchange.objects.all().values_list('exid', flat=True)
    result = group([prices.s(exid) for exid in exchanges]).delay()

    while not result.ready():
        time.sleep(0.5)

    if result.successful():
        log.info('Task update price complete')

    else:
        log.error('Task update price failed')


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

                if exid == 'binance':
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


@shared_task(base=BaseTaskWithRetry)
def get_mcap():
    from requests import Request, Session
    from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
    import json

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {
        'start': '1',
        'limit': '5000',
        'convert': 'USD'
    }
    headers = {
        'Accepts': 'application/json',
        'X-CMC_PRO_API_KEY': '5e2d4f14-42a9-4108-868c-6fd0bb8c6186',
    }

    session = Session()
    session.headers.update(headers)

    try:
        res = session.get(url, params=parameters)
        data = json.loads(res.text)
        if data['status']['error_code'] == 0:
            return data
        else:
            log.error('Error while retrieving data from CoinMarketCap')
            log.error("Error: {0}".format(data['status']['error_message']))

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        log.error('Error while retrieving data from CoinMarketCap')
        log.error("Error: {0}".format(e))


@shared_task(base=BaseTaskWithRetry)
def prices(exid):
    from marketsdata.models import Exchange, Market, Candle
    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid)

    if exchange.is_trading():
        if exchange.has['fetchTickers']:

            # Update ticker price and volume
            def update(response):

                def get_quote_volume():

                    if exid == 'binance':
                        if 'quoteVolume' in response:
                            volume = response['quoteVolume']
                        elif 'baseVolume' in response['info']:
                            volume = float(response['info']['baseVolume']) * response['last']

                    elif exid == 'bybit':
                        if market.type == 'derivative':
                            if market.margined.code == 'USDT':
                                volume = float(response['info']['turnover_24h'])
                            else:
                                volume = float(response['info']['volume_24h'])

                    elif exid == 'okex':
                        if market.wallet == 'spot':
                            if 'quote_volume_24h' in response['info']:
                                volume = float(response['info']['quote_volume_24h'])
                            else:
                                volume = response['quoteVolume']
                        elif market.wallet == 'swap':
                            if market.margined.code == 'USDT':
                                volume = float(response['info']['volCcy24h'])
                            else:
                                volume = float(response['info']['volCcy24h']) * response['last']
                        elif market.wallet == 'futures':
                            if market.margined.code == 'USDT':
                                volume = float(response['info']['volCcy24h'])
                            else:
                                volume = float(response['info']['volCcy24h']) * response['last']

                    elif exid == 'ftx':
                        volume = float(response['info']['volumeUsd24h'])

                    elif exid == 'huobipro':
                        if not market.wallet:
                            volume = float(response['info']['vol'])

                    elif exid == 'bitfinex2':
                        volume = float(response['baseVolume']) * response['last']

                    if 'volume' in locals():
                        return volume
                    else:
                        pprint(response)
                        log.warning('Volume not in load_markets()')

                log.bind(wallet=market.wallet, symbol=market.symbol)

                # Select dictionary
                if market.symbol in response:
                    response = response[market.symbol]
                else:
                    log.warning('Symbol not in load_markets()')
                    log.info('Delete market')
                    market.delete()
                    return

                # Select last price
                if 'last' in response:
                    last = response['last']
                else:
                    log.warning('Last price not in load_markets()')
                    market.updated = False
                    market.save()
                    return

                # Select trading volume
                volume = get_quote_volume()
                if volume is None:
                    pprint(response)
                    log.warning('Volume not in load_markets()')
                    market.updated = False
                    market.save()
                    return

                # Select market cap
                market_cap = 0
                volume_mcap = 0
                quotes = [d['quote']['USD'] for d in mcap['data'] if d['symbol'] == market.base.code]
                if quotes:
                    quotes = quotes[0]
                    if quotes['market_cap']:
                        market_cap = quotes['market_cap']
                        volume_mcap = volume / market_cap
                else:
                    log.warning('Unable to retrieve mcap for {0}'.format(market.base.code))

                # Create datetime object
                dt = timezone.now().replace(minute=0,
                                            second=0,
                                            microsecond=0) - timedelta(minutes=exchange.update_frequency)

                try:
                    Candle.objects.get(market=market, exchange=exchange, dt=dt)

                except Candle.DoesNotExist:
                    Candle.objects.create(market=market,
                                          exchange=exchange,
                                          dt=dt,
                                          close=last,
                                          mcap=market_cap,
                                          volume_mcap=volume_mcap,
                                          volume_avg=volume / 24
                                          )
                    market.updated = True
                    market.save()

                else:
                    pass
                    # log.warning('Candle exists', dt=dt)

                finally:
                    log.unbind('wallet', 'symbol')

            client = exchange.get_ccxt_client()
            log.info('Prices update start')

            # Create a list of trading markets
            markets = Market.objects.filter(exchange=exchange,
                                            quote__code__in=exchange.get_supported_quotes(),
                                            trading=True)

            # Get marketcaps
            mcap = get_mcap()

            if exchange.wallets:

                # Iterate through wallets
                for wallet in exchange.get_wallets():

                    log.info('Prices update {0}'.format(wallet))

                    client.options['defaultType'] = wallet
                    if exchange.has_credit(wallet):

                        # Load markets
                        client.load_markets(True)
                        exchange.update_credit('load_markets', wallet)
                        if exchange.has_credit(wallet):

                            # FetchTickers
                            response = client.fetch_tickers()
                            exchange.update_credit('fetch_tickers', wallet)

                            for market in markets.filter(wallet=wallet):
                                update(response)

                                # # Download OHLCV if gap detected
                                # if market.has_gap():
                                #     insert_candle_history(exid,
                                #                           market.wallet,
                                #                           market.symbol,
                                #                           recent=True)
                                # else:
                                #     # Else select price and volume from response
                                #     update(response, market)

                    log.info('Prices update {0} complete'.format(wallet))

            else:
                if exchange.has_credit():
                    client.load_markets(True)
                    exchange.update_credit('load_markets')

                    if exchange.has_credit():
                        response = client.fetch_tickers()
                        exchange.update_credit('fetch_tickers')

                        for market in markets:
                            update(response)

                            # # Download OHLCV if gap detected
                            # if market.has_gap():
                            #     insert_ohlcv(exid,
                            #                           market.wallet,
                            #                           market.symbol,
                            #                           recent=True)
                            # else:
                            #     # Else select price and volume from response
                            #     update(response)

            # Set exchange last update time
            exchange.last_price_update_dt = timezone.now()
            exchange.save()

            # log.info('Prices update complete')

        else:
            raise ('Exchange {0} does not support FetchTickers()'.format(exid))


@shared_task(base=BaseTaskWithRetry)
def top_markets(exid):
    import operator
    exchange = Exchange.objects.get(exid=exid)

    if exid == 'binance':

        for wallet in exchange.get_wallets():

            log.info('Set top = True')
            markets = Market.objects.filter(exchange__exid=exid,
                                            quote__code=exchange.dollar_currency,
                                            wallet=wallet,
                                            updated=True,
                                            trading=True)

            v = [[m.candle.first().volume_avg, m.candle.first().id] for m in markets if m.candle.first().volume_avg]

            top = sorted(v, key=operator.itemgetter(0))[-20:]

            for pk in [pk for pk in [t[1] for t in top]]:
                market = Candle.objects.get(pk=pk).market
                market.top = True
                market.save()

            log.info('Set top complete')


# Bulk insert OHLCV candles history
@shared_task(base=BaseTaskWithRetry)
def insert_ohlcv_bulk(exid, mcap, recent=None):

    return [chain(insert_ohlcv.si(exid,
                                  market.wallet,
                                  market.symbol,
                                  mcap,
                                  recent
                                  ).set(queue='slow')
                  for market in Market.objects.filter(exchange__exid=exid).order_by('symbol'))]


# Insert OHLCV candles history
@shared_task(bind=True, name='Markets_____Insert candle history', base=BaseTaskWithRetry)
def insert_ohlcv(self, exid, wallet, symbol, mcap, recent=None):

    exchange = Exchange.objects.get(exid=exid)
    log.bind(exchange=exid, symbol=symbol, wallet=wallet)
    if exchange.is_trading():

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
                if exchange.wallets:
                    client.options['defaultType'] = market.wallet

                if exchange.has_credit(market.wallet):
                    client.load_markets(True)
                    market.exchange.update_credit('load_markets', market.wallet)

                while True:

                    try:
                        if exchange.has_credit(market.wallet):
                            response = client.fetchOHLCV(market.symbol, '1h', since_ts, limit)
                            exchange.update_credit('fetchOHLCV', market.wallet)

                    except ccxt.BadSymbol as e:

                        log.error('Bad symbol')
                        log.info('Delete market')
                        market.delete()
                        return

                    except ccxt.ExchangeError as e:
                        print(since_ts, limit)
                        log.exception('Exchange error')
                        return

                    except Exception as e:
                        print(getattr(e, 'message', repr(e)))
                        print(getattr(e, 'message', str(e)))
                        log.exception('An unexpected error occurred', exception=e.__class__.__name__)
                        return

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

                                        # Select market cap
                                        market_cap = 0
                                        volume_mcap = 0
                                        quotes = [d['quote']['USD'] for d in mcap['data'] if
                                                  d['symbol'] == market.base.code]
                                        if quotes:
                                            quotes = quotes[0]
                                            if quotes['market_cap']:
                                                market_cap = quotes['market_cap']
                                                volume_mcap = vo / market_cap
                                        else:
                                            log.warning('Unable to retrieve mcap for {0}'.format(market.base.code))

                                        Candle.objects.create(market=market,
                                                              exchange=exchange,
                                                              close=cl,
                                                              volume=vo,
                                                              market_cap=market_cap,
                                                              volume_mcap=volume_mcap,
                                                              dt=dt)
                                    else:
                                        # Candles returned by exchange can be into database
                                        continue

                                log.info(
                                    'Candles inserted : {0}'.format(
                                        insert))  # since=since_dt.strftime("%Y-%m-%d %H:%M"))

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

                return True

        market = Market.objects.get(exchange=exchange, wallet=wallet, symbol=symbol)

        if market.quote.code == market.exchange.dollar_currency:

            log.info('Insert candle {0}'.format(symbol))

            # Set start date
            if market.is_populated():
                if recent:
                    start = market.get_candle_datetime_last()
                else:
                    start = timezone.make_aware(datetime.combine(exchange.start_date, datetime.min.time()))
            else:
                log.info('Download full OHLCV history')
                start = timezone.make_aware(datetime.combine(exchange.start_date, datetime.min.time()))

            try:
                res = insert(market)

            except ccxt.DDoSProtection as e:
                log.error('DDoS protection')
                print(e)
            except Exception as e:
                log.error('Unable to fetch OHLCV: {0}'.format(e))

            else:
                if res:
                    if market.has_gap():
                        log.warning('Insert complete with gaps')
                        log.info('Delete market')
                        market.delete()
                    else:
                        log.info('Candles complete')
                        market.updated = True
                        market.save()

#
# # Insert OHLCV candles history
# @shared_task(bind=True, name='Markets_____Set_volume_mcap_zero')
# def volume_mcap(self):
#     candles = Candle.objects.all()
#     log.info('Start zeroing')
#
#     for c in candles.iterator(10000):
#         if not c.volume_mcap:
#             c.volume_mcap = 0
#             c.save()
#
#     log.info('Zeroing complete')