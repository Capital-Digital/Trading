from __future__ import absolute_import, unicode_literals

import configparser
import time

import ccxt
import requests
import urllib3
from billiard.process import current_process
from celery import group, shared_task, Task
from celery.result import AsyncResult

from capital.celery import app

from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from capital.methods import *
from marketsdata.methods import *
from marketsdata.models import Exchange, Market, Currency, Candles, Tickers
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


@shared_task(name='Markets_____Periodic_update')
def periodic_update():
    #
    exchanges = Exchange.objects.filter(enable=True)
    for exchange in exchanges:
        exid = exchange.exid
        log.info('Periodic update of {0}'.format(exid))

        update_ex_status.delay(exid)
        update_ex_properties.delay(exid)
        update_ex_currencies.delay(exid)
        update_ex_markets.delay(exid)


@shared_task(base=BaseTaskWithRetry, name='Markets_____Periodic_update_status')
def update_ex_status(exid):
    #
    log.bind(exid=exid)
    log.info('Update status')
    exchange = Exchange.objects.get(exid=exid)

    try:
        client = exchange.get_ccxt_client()
        response = client.fetchStatus()

    except ccxt.ExchangeNotAvailable:
        exchange.status = 'nok'
        exchange.status_at = timezone.now()
        exchange.save()
        log.error('Exchange not available', status=exchange.status)

    else:

        if 'status' in response:
            if response['status']:
                exchange.status = response['status']
            else:
                exchange.status = 'Unknown'
                log.warning('Status is unknown')

        if 'updated' in response:
            if response['updated']:
                dt = datetime.fromtimestamp(response['updated'] / 1e3)
                exchange.status_at = timezone.make_aware(dt)
            else:
                log.warning('Status update datetime unknown')

        if 'eta' in response:
            if response['eta']:
                dt = datetime.fromtimestamp(response['eta'] / 1e3)
                exchange.eta = timezone.make_aware(dt)

        if 'url' in response:
            if response['url']:
                exchange.url = response['url']

        exchange.save(update_fields=['status_at',
                                     'status',
                                     'eta',
                                     'url']
                      )

    log.unbind('exid')


@shared_task(base=BaseTaskWithRetry, name='Markets_____Periodic_update_properties')
def update_ex_properties(exid):
    #
    log.bind(exid=exid)
    log.info('Update properties')
    exchange = Exchange.objects.get(exid=exid)

    try:
        client = exchange.get_ccxt_client()

    except ccxt.ExchangeNotAvailable as e:
        log.error('Properties update failure: {0}'.format(e))

    else:
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

        log.debug('Update properties complete')

    log.unbind('exid')


@shared_task(base=BaseTaskWithRetry, name='Markets_____Periodic_update_currencies')
def update_ex_currencies(exid):
    #
    log.bind(exid=exid)
    log.info('Update currencies')
    exchange = Exchange.objects.get(exid=exid)

    try:
        client = exchange.get_ccxt_client()

    except Exception as e:
        log.error('Currencies update failure: {0}'.format(e))

    else:

        def update(code, dic):

            try:
                obj = Currency.objects.get(code=code, exchange=exchange)

            except MultipleObjectsReturned:
                log.error('Duplicate currency {0}'.format(code))

            # Currency is new on this exchange ?
            except ObjectDoesNotExist:

                try:
                    obj = Currency.objects.get(code=code)

                except MultipleObjectsReturned:
                    log.error('Duplicate currency {0}'.format(code))
                    return

                except ObjectDoesNotExist:
                    obj = Currency.objects.create(code=code)

                finally:

                    # Add an exchange to an existing currency
                    log.info('{0} is now listed'.format(code))
                    obj.exchange.add(exchange)
                    obj.response = dic
                    obj.save()

            # Tag currency as stablecoin
            if code in config['MARKETSDATA']['supported_stablecoins']:
                if not Currency.objects.get(code=code).stable_coin:
                    obj.stable_coin = True
                    obj.save()

            elif Currency.objects.get(code=code).stable_coin:
                obj.stable_coin = False
                obj.save()

        # Skip OKEx wallets because all currencies
        # are returned with a single call ccxt.okex.currencies
        if exchange.wallets and exid != 'okex':

            for wallet in exchange.get_wallets():

                log.bind(wallet=wallet)
                client.options['defaultType'] = wallet
                client.load_markets(True)
                for code, dic in client.currencies.items():
                    update(code, dic)
                log.unbind('wallet')

        else:
            if exchange.has_credit():
                client.load_markets(True)
                for code, dic in client.currencies.items():
                    update(code, dic)


@shared_task(base=BaseTaskWithRetry, name='Markets_____Periodic_update_markets')
def update_ex_markets(exid):
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

    log.info('Update market complete')


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


########################


# Update dataframes of all exchanges
@app.task(bind=True, name='Update_exchanges')
def update_exchanges(self):
    exchanges = Exchange.objects.filter(enable=True)
    for exchange in exchanges:
        update_dataframe.delay(exchange.exid, True)


# Add a new row to dataframe (signal strategies update)
@app.task(bind=True, base=BaseTaskWithRetry, name='Update_dataframe')
def update_dataframe(self, exid, signal):
    #
    log.info('Update dataframe ({0})'.format(current_process().index))
    log.bind(exid=exid)

    # Select instance and create self.data dataframe with available prices
    exchange = Exchange.objects.get(exid=exid)
    codes = exchange.get_strategies_codes()
    exchange.load_data(10 * 24, codes)

    if signal:

        # wait...
        while datetime.now().minute > 0:
            log.info('Wait {0} minutes and {1}s'.format(60 - datetime.now().minute, 60 - datetime.now().second))
            time.sleep(1)

    if exchange.is_trading():
        if exchange.has['fetchTickers']:

            # Download snapshot of spot markets
            client = exchange.get_ccxt_client()
            dic = client.fetch_tickers()
            df = pd.DataFrame()
            dt = timezone.now().replace(minute=0, second=0, microsecond=0)
            dt_string = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

            log.info('Update dataframe')

            # Select codes of our strategies
            codes = list(set(exchange.data.columns.get_level_values(1).tolist()))

            for code in codes:

                try:
                    d = {k: dic[code + '/USDT'][k] for k in ['last', 'quoteVolume']}

                except KeyError:
                    log.warning('Market {0} not found in dictionary')
                    continue

                tmp = pd.DataFrame(index=[pd.to_datetime(dt_string)], data=d)
                tmp.columns = pd.MultiIndex.from_product([tmp.columns, [code]])
                df = pd.concat([df, tmp], axis=1)

            df = df.reindex(sorted(df.columns), axis=1)
            df = pd.concat([exchange.data, df])
            df = df[~df.index.duplicated(keep='first')]

            log.info('Save new dataframe')
            log.info(df)

            exchange.data = df
            exchange.save()

            if exchange.is_data_updated():
                log.info('1 row added with timestamp {0}'.format(df.index[-1]))
                log.info('Update dataframe complete')

            else:
                raise Exception('Dataframe update problem')
        else:
            log.error("Exchange doesn't support fetchTickers")
    else:
        log.error('Exchange is not trading')

    log.info('Finally')
    log.info(exchange.is_data_updated())
    log.unbind('exid')


# Fetch markets snapshot of all exchanges at 00:00
@app.task(bind=True, name='Update_prices')
def update_prices(self):
    exchanges = Exchange.objects.filter(enable=True)
    for exchange in exchanges:
        update_tickers.delay(exchange.exid)


# Update tickers objects
@shared_task(bind=True, base=BaseTaskWithRetry, name='Update_tickers')
def update_tickers(self, exid):
    log.info('#')
    log.info('#')
    log.info('Update tickers ({0})'.format(current_process().index))
    log.info('#')
    log.info('#')

    log.bind(exid=exid)
    exchange = Exchange.objects.get(exid=exid)

    dt = timezone.now().replace(minute=0, second=0, microsecond=0)
    dt_string = dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    def insert(data, wallet=None):

        log.info('Insert {0} data'.format(wallet))

        semester = 1 if dt.month <= 6 else 2
        symbols = [s for s in data.keys() if '/USDT' in s]
        symbols.sort()
        insert = 0

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
                    obj = Tickers.objects.get(year=dt.year, semester=semester, market=market)

                except ObjectDoesNotExist:
                    log.info('Create object for {0} {1} {2}'.format(market.symbol, dt.year, semester))

                    # Create new object
                    Tickers.objects.create(year=dt.year,
                                           semester=semester,
                                           market=market,
                                           data={dt_string: dic}
                                           )
                else:
                    if dt_string not in obj.data.keys():
                        obj.data[dt_string] = dic
                        obj.save()
                        insert += 1

                    else:
                        pass

        log.info('Insert {0} data complete ({1})'.format(wallet, insert))

    if exchange.is_trading():
        if exchange.has['fetchTickers']:

            # Download tickers
            client = exchange.get_ccxt_client()
            if exchange.wallets:
                for wallet in exchange.get_wallets():
                    client.options['defaultType'] = wallet
                    data = client.fetch_tickers()
                    insert(data, wallet)

            else:
                data = client.fetch_tickers()
                insert(data)

        else:
            log.error("Exchange doesn't support fetchTickers")
    else:
        log.error('Exchange is not trading')

    log.unbind('exid')


# Update all strategies
@app.task(bind=True, name='Update_strategies')
def update_strategies(self, exid, signal):
    if Exchange.objects.get(exid=exid).is_data_updated():
        from strategy.models import Strategy
        strategies = Strategy.objects.filter(exchange__exid=exid, production=True)
        for strategy in strategies:
            update_strategy.delay(strategy.name, signal)


# Update a strategy
@app.task(bind=True, base=BaseTaskWithRetry, name='Update_strategy')
def update_strategy(self, name, signal):
    log.info('Update strategy {0} ({1})'.format(name, current_process().index))
    from strategy.models import Strategy
    strategy = Strategy.objects.get(name=name)
    strategy.execute()


# Update all accounts
@app.task(bind=True, name='Update_accounts')
def update_accounts(self, strategy_name, signal):
    from trading.models import Account
    accounts = Account.objects.filter(strategy__name=strategy_name, active=True)
    for account in accounts:
        update_account.delay(account.id, signal)


# Update an account
@app.task(bind=True, base=BaseTaskWithRetry, name='Update_account')
def update_account(self, account_id, signal):
    from trading.models import Account
    account = Account.objects.get(id=account_id)
    log.info('Update account {0} ({1})'.format(account.name, current_process().index))
    account.trade()


#########################################


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
