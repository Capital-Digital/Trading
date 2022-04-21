import time
from datetime import datetime, date
import ccxt
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.db import models
from django_pandas.managers import DataFrameManager
from capital.methods import *
from marketsdata.methods import *
from capital.methods import *
import pandas as pd
from tqdm import tqdm
from pprint import pprint
import json
import cloudscraper
import itertools
from picklefield.fields import PickledObjectField
from billiard.process import current_process

log = structlog.get_logger(__name__)


class Exchange(models.Model):
    objects = models.Manager()
    exid = models.CharField(max_length=12, blank=True, null=True)
    wallets = models.CharField(max_length=50, blank=True, null=True)
    name, version = [models.CharField(max_length=12, blank=True, null=True) for i in range(2)]
    api, countries, urls, has, timeframes, credentials, options = [models.JSONField(blank=True, null=True) for i in
                                                                   range(7)]

    is_spot_inserted = models.BooleanField(default=False)
    is_futu_inserted = models.BooleanField(default=False)

    data = PickledObjectField(null=True)
    timeout = models.IntegerField(default=3000)
    rate_limit = models.IntegerField(default=1000)
    precision_mode = models.IntegerField(null=True, blank=True)
    status_at, eta = [models.DateTimeField(blank=True, null=True) for i in range(2)]
    url = models.URLField(blank=True, null=True)
    start_date = models.DateTimeField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    update_frequency = models.SmallIntegerField(null=True, default=60)
    last_price_update_dt = models.DateTimeField(null=True, blank=True)
    supported_quotes = models.CharField(max_length=50, blank=True, null=True)
    supported_stablecoins = models.CharField(max_length=50, blank=True, null=True)
    verbose = models.BooleanField(default=False)
    enable = models.BooleanField(default=False)
    enable_rate_limit = models.BooleanField(default=True)
    limit_ohlcv = models.PositiveIntegerField(null=True, blank=True)
    credit = models.JSONField(blank=True, null=True)
    credit_max = models.JSONField(blank=True, null=True)
    rate_limits = models.JSONField(blank=True, null=True)
    funding_rate_freq = models.PositiveSmallIntegerField(null=True, blank=True, default=8)

    status = models.CharField(max_length=12, default='ok', null=True, blank=True,
                              choices=[('ok', 'ok'), ('maintenance', 'maintenance'),
                                       ('shutdown', 'shutdown'), ('error', 'error')])
    orderbook_limit = models.PositiveSmallIntegerField(default=100)
    objects = models.Manager()

    class Meta:
        verbose_name_plural = "exchanges"

    def save(self, *args, **kwargs):
        if self.urls:
            self.url = self.urls['www']
        super(Exchange, self).save(*args, **kwargs)

    def __str__(self):
        return self.name

    # Return True if all markets are updated
    def are_markets_updated(self):
        markets = Market.objects.filter(exchange=self,
                                        updated=False
                                        ).order_by('wallet', 'symbol')
        if markets.exists:
            log.warning('Markets are not updated')
            [print(m.wallet, m.symbol) for m in markets]
            return False
        else:
            return True

    def get_supported_quotes(self):
        return str(self.supported_quotes).replace(' ', '').split(',')

    def get_supported_stablecoins(self):
        return str(self.supported_stablecoins).replace(' ', '').split(',')

    # Return True is exchange status is OK
    def is_trading(self):
        if self.status == 'ok':
            return True
        else:
            return False

    # Return True if it's time to update markets prices
    def is_update_time(self):

        if self.last_price_update_dt:
            last = self.last_price_update_dt
        else:
            last = timezone.now()

        elapsed = (timezone.now() - last).seconds
        hours, remainder = divmod(elapsed, 3600)
        minutes, seconds = divmod(remainder, 60)

        if elapsed / 60 > self.update_frequency:
            return True
        else:
            log.info('Time since last update is {0} hour(s) {1} minute(s) and {2} seconds'.format(int(hours),
                                                                                                  int(minutes),
                                                                                                  int(seconds)))
            return False

    # Return exchange class (ccxt)
    def get_ccxt_client(self, account=None, wallet=None):

        client = getattr(ccxt, self.exid)
        client = client({
            'timeout': self.timeout,
            'verbose': self.verbose,
            'enableRateLimit': self.enable_rate_limit,
            'rateLimit': self.rate_limit,
            'adjustForTimeDifference': True,
            # 'session': cloudscraper.create_scraper(allow_brotli=True),
        })

        # Set API key/secret
        if account:
            client.secret = account.api_secret
            client.apiKey = account.api_key
            if self.credentials['password']:
                client.password = account.password

        if wallet:
            if 'defaultType' in client.options:
                client.options['defaultType'] = wallet

        self.options = client.options
        self.save()

        return client

    def get_ccxt_client_pro(self, account=None, market=None):

        if not self.is_trading():
            log.error('Exchange is trading', exchange=self.exid)
            return

        client = getattr(ccxt, self.exid)
        client = client({
            'timeout': self.timeout,
            'verbose': self.verbose,
            'enableRateLimit': self.enable_rate_limit,
            'rateLimit': self.rate_limit
        })

        # Set API key/secret
        if account:
            from trading.models import Account
            account = Account.objects.get(name=account)
            client.secret = account.api_secret
            client.apiKey = account.api_key
            if self.credentials['password']:
                client.password = account.password

        if market:
            if 'defaultType' in client.options:
                client.options['defaultType'] = market.type

        # create a new method
        def get_market_types(client):
            return list(set(Market.objects.filter(exchange=self).values_list('type', flat=True)))

        import types
        client.get_market_types = types.MethodType(get_market_types, client)

        return client

    # Return exchange's wallets
    def get_wallets(self):

        # Return a list of supported ccxt types
        if self.wallets:
            return str(self.wallets).replace(" ", "").split(',')
        else:
            raise Exception('Exchange {0} requires a parameter defaultType'.format(self.exid))

    # return a list of market types (spot or derivative)
    def get_market_types(self):
        return list(set(Market.objects.filter(exchange=self).values_list('type', flat=True)))

    # return accounts linked to this exchange
    def get_trading_accounts(self):
        from trading.models import Account
        return Account.objects.filter(exchange=self, trading=True)

    # Return a list of stablecoins
    def get_stablecoins(self):
        return [c.code for c in Currency.objects.filter(exchange=self, stable_coin=True)]

    # Return a list of codes of our strategies
    def get_strategies_codes(self):
        from strategy.models import Strategy
        strategies = Strategy.objects.filter(exchange__exid=self.exid,
                                             production=True)

        # Count number of codes so that codes from strategies
        # with the lowest codes length are updated first
        for s in strategies:
            s.set_codes_length()

        # Sort strategies
        strategies = Strategy.objects.filter(exchange__exid=self.exid,
                                             production=True).order_by('codes_length')

        codes = []
        for s in strategies:
            codes.append(s.get_codes())

        # Flatten lists and drop duplicate whilst preserving order
        codes = list(itertools.chain.from_iterable(codes))
        codes = list(dict.fromkeys(codes))
        return codes

    # Return a list of symbols of our strategies
    def get_strategies_symbols(self):
        from strategy.models import Strategy
        strategies = Strategy.objects.filter(exchange__exid=self.exid)

        # Count number of codes so that codes from strategies
        # with the lowest codes length are updated first
        for s in strategies:
            s.set_codes_length()

        # Sort strategies
        strategies = Strategy.objects.filter(exchange__exid=self.exid).order_by('codes_length')

        symbols = []
        for s in strategies:
            symbols.append(s.get_symbols())

        # Flatten lists and drop duplicate whilst preserving order
        symbols = list(itertools.chain.from_iterable(symbols))
        symbols = list(dict.fromkeys(symbols))
        return symbols

    # Return True if there is available credit
    def has_credit(self, wallet=None):

        credit = self.credit

        if not credit:
            credit = dict()

        ts = get_datetime(timestamp=True)

        # Return True is new
        if wallet not in credit:
            return True

        credit = credit[wallet]

        # Return a list with total number of weight and orders
        def count():

            # Filter dictionary and count weights
            req = dict(filter(
                lambda elem: float(elem[0]) > ts - (request_weight_interval * request_weight_interval_num),
                credit.items())
            )
            weight = sum([v['weight'] for k, v in req.items()])

            # Count order for rule 1
            tensec = dict(filter(
                lambda elem: float(elem[0]) > ts - (order_1_count_interval * order_1_count_interval_num),
                credit.items())
            )
            order_count_1 = len([v['order'] for k, v in tensec.items() if v['order']])

            # Count orders for rule 2
            if wallet in ['spot', 'future']:

                day = dict(filter(
                    lambda elem: float(elem[0]) > ts - (order_2_count_interval * order_2_count_interval_num),
                    credit.items())
                )
                order_count_2 = len([v['order'] for k, v in day.items() if v['order']])

            else:
                order_count_2 = 0

            return [weight, order_count_1, order_count_2]

        if self.exid == 'binance':

            if wallet == 'spot':

                request_weight_interval = 60  # 60 seconds
                request_weight_interval_num = 1  # 1 * 60 sec
                request_weight_limit = 1200

                order_1_count_interval = 1
                order_1_count_interval_num = 10
                order_1_count_limit = 100

                order_2_count_interval = 60 * 60 * 24
                order_2_count_interval_num = 1
                order_2_count_limit = 200000

            elif wallet == 'future':

                request_weight_interval = 60
                request_weight_interval_num = 1
                request_weight_limit = 2400

                order_1_count_interval = 60
                order_1_count_interval_num = 1
                order_1_count_limit = 1200

                order_2_count_interval = 1
                order_2_count_interval_num = 10
                order_2_count_limit = 300

            elif wallet == 'delivery':

                request_weight_interval = 60
                request_weight_interval_num = 1
                request_weight_limit = 2400

                order_1_count_interval = 60
                order_1_count_interval_num = 1
                order_1_count_limit = 1200

            # Count
            weight, order_count_1, order_count_2 = count()

            # Check thresholds
            if weight > request_weight_limit:
                return False

            if order_count_1 > order_1_count_limit:
                return False

            if wallet in ['spot', 'future']:
                if order_count_2 > order_2_count_limit:
                    return False

            # Update credit max reached
            self.update_credit_max(wallet, weight, order_count_1, order_count_2)

            # Finally filter out expired entries
            self.credit[wallet] = dict(filter(lambda elem: float(elem[0]) > ts - (60 * 60 * 24), credit.items()))
            self.save()

        return True

    # Append weights and order to credit dictionary
    def update_credit(self, method, wallet=None):
        credit = self.credit
        ts = get_datetime(timestamp=True)

        if self.exid == 'binance':

            if wallet in ['spot', 'future', 'delivery']:

                if method == 'fetchBalance':
                    weight = 5 + 1  # +1 for exchangeInfo
                    order = False

                elif method == 'load_markets':
                    weight = 1
                    order = False

                elif method == 'fetchOHLCV':
                    weight = 1
                    order = False

                elif method == 'fetch_tickers':
                    weight = 40
                    order = False

                elif method == 'positionRisk':
                    weight = 5
                    order = False

                elif method == 'fetchAllOpenOrders':
                    weight = 40 + 1
                    order = False

                elif method == 'create_order':
                    weight = 1 + 1
                    order = True

                elif method == 'fetchOrder':
                    weight = 1 + 1
                    order = True

                elif method == 'fetchOpenOrders':
                    weight = 1 + 1
                    order = True

                elif method == 'cancel_order':
                    weight = 1 + 1
                    order = True

                elif method == 'transfer':
                    weight = 1 + 1
                    order = True

                else:
                    raise Exception('Method unknown : {0}'.format(method))

            else:
                raise Exception('{0} is not valid defaultType for {1}'.format(wallet, self.name))

            # Create new dictionary or append to an existing dictionary
            if wallet in credit:
                credit[wallet][ts] = dict(weight=weight, order=order, method=method)
            else:
                dic = dict()
                dic[ts] = dict(weight=weight,
                               order=order,
                               method=method)
                credit[wallet] = dic

            self.credit = credit
            self.save()

    # Replace max credit reached if a new high is reached
    def update_credit_max(self, wallet, weight, order_count_1, order_count_2):

        credit_max = self.credit_max

        if not credit_max:
            credit_max = dict()

        if not wallet:
            wallet = 'default'

        if wallet in credit_max:

            # Select max values
            if 'weight' in credit_max[wallet]:
                if 'max' in credit_max[wallet]['weight']:
                    max_w = credit_max[wallet]['weight']['max']
            if 'order_count_1' in credit_max[wallet]:
                if 'max' in credit_max[wallet]['order_count_1']:
                    max_o1 = credit_max[wallet]['order_count_1']['max']
            if 'order_count_2' in credit_max[wallet]:
                if 'max' in credit_max[wallet]['order_count_2']:
                    max_o2 = credit_max[wallet]['order_count_2']['max']

            # Compare to current values and update field credit_max_reached
            if weight > max_w:
                log.info('Max requests weigh is now {0}'.format(weight), exchange=self.exid, wallet=wallet)
                w = dict(date=get_datetime(string=True), max=weight)
                self.credit_max[wallet]['weight'] = w

            if order_count_1 > max_o1:
                log.info('Max order count is now {0}'.format(weight), exchange=self.exid, wallet=wallet)
                order_1 = dict(date=get_datetime(string=True), max=order_count_1)
                self.credit_max[wallet]['order_count_1'] = order_1

            if order_count_2 > max_o2:
                log.info('Max order count is now {0}'.format(weight), exchange=self.exid, wallet=wallet)
                order_2 = dict(date=get_datetime(string=True), max=order_count_2)
                self.credit_max[wallet]['order_count_2'] = order_2

            self.save()

        else:

            # Create a new dictionary if necessary
            w = dict(date=get_datetime(string=True), max=weight)
            order_1 = dict(date=get_datetime(string=True), max=order_count_1)
            order_2 = dict(date=get_datetime(string=True), max=order_count_2)

            self.credit_max = dict()
            self.credit_max[wallet] = dict(weight=w,
                                           order_count_1=order_1,
                                           order_count_2=order_2
                                           )
            self.save()

    # Save spot prices and volume to CSV files
    def save_csv_file(self, quote, dtype):

        from pathlib import Path
        filename = 'df_' + quote + '_' + dtype + '.csv'
        file = Path(filename)

        # Determine indice
        if dtype == 'prices':
            indice = 4
        elif dtype == 'volumes':
            indice = 5
        else:
            raise Exception("Data type must be 'price' or 'volume'")

        # Update CSV file
        if file.exists():

            log.info('Update {0} candles dataframe for {1}'.format(dtype, quote))

            # Load file and determine start datetime
            df = pd.read_csv(filename, sep=',', encoding='utf-8').set_index('index')
            df.index = pd.to_datetime(df.index)

            print('Before update')
            print(df['BTC'].tail(20))

            start = df.tail(1).index[0] + timedelta(hours=1)
            years = get_years(start)
            semester = get_semesters(start)

            # Query candles from all spot markets
            qs = Candles.objects.filter(market__quote__code=quote,
                                        market__type='spot',
                                        market__exchange=self,
                                        year__in=years,
                                        semester__in=semester
                                        )

        # Create empty dataframe
        else:
            log.info('Create {0} candles dataframe for {1}'.format(dtype, quote))
            start = datetime(2018, 1, 1, 0, 0).replace(tzinfo=pytz.UTC)
            df = pd.DataFrame()
            qs = Candles.objects.filter(market__quote__code=quote,
                                        market__type='spot',
                                        market__exchange=self
                                        )

        for i in qs.iterator(10):

            # Filter data based on timestamps
            directive = '%Y-%m-%dT%H:%M:%SZ'
            ts = [e[0] for e in i.data if string_to_date(e[0], directive) >= start]
            data = [i[indice] for i in i.data if i[0] in ts]

            if i.market.symbol == 'ACA/USDT':
                print('ACA\n', len(data))
                print('ACA\n', len(ts))

            if data:

                temp = pd.DataFrame(data, index=ts, columns=[i.market.base.code])
                temp.index = pd.to_datetime(temp.index)
                axis = 0 if i.market.base.code in df.columns else 1
                df = pd.concat([df, temp], axis=axis).groupby(level=0).first()

        df = df.reset_index()

        # Save dataframe to file
        df.to_csv(filename, sep=',', encoding='utf-8', index=False)
        log.info("Update complete")

    # Create dataframes from tickers
    def load_data(self, length, codes):

        if codes:

            log.info('Preload dataframe', length=len(codes))

            now = datetime.now().replace(minute=0, second=0, microsecond=0)
            start = now - timedelta(hours=length)

            self.data = pd.DataFrame()

            # Query tickers objects
            qs = Tickers.objects.filter(year__in=get_years(start),
                                        semester__in=get_semesters(start),
                                        market__base__code__in=codes,
                                        market__type='spot',
                                        market__quote__code='USDT',
                                        market__exchange=self
                                        )

            for ticker in qs.iterator(10):

                # Select dictionaries
                dic = {k: v for k, v in ticker.data.items() if v['timestamp'] > start.timestamp()}

                df = pd.DataFrame(dic).T[['last', 'quoteVolume']]
                df.columns = pd.MultiIndex.from_product([df.columns, [ticker.market.base.code]])
                df.index = pd.to_datetime(df.index, format="%Y-%m-%dT%H:%M:%SZ", utc=True)
                self.data = pd.concat([self.data, df], axis=1)

            # Group by columns
            self.data = self.data.groupby(self.data.columns, axis=1).sum()
            self.data.columns = pd.MultiIndex.from_tuples(self.data.columns)

            # Check and fix rows
            self.data = fix(self.data)
            self.save()

            # log.info('Preload dataframe complete')

            log.info('Preload dataframe complete', length=len(codes))
            return self.data

        else:
            raise Exception('List of codes is empty')

    # Return True if the dataframe is updated
    def is_data_updated(self):
        if hasattr(self, 'data'):
            if isinstance(self.data, pd.DataFrame):
                if self.data.index[-1] == dt_aware_now(0):
                    return True
                else:
                    log.error('Dataframe is not updated', exid=self.exid)
                    return False
            else:
                log.error('exchange.data is None', exid=self.exid)
                return False
        else:
            log.error('Instance has not data attribute', exid=self.exid)
            return False


class Currency(models.Model):
    name, code = [models.CharField(max_length=100, blank=True, null=True) for i in range(2)]
    exchange = models.ManyToManyField(Exchange, related_name='currency')
    stable_coin = models.BooleanField(default=False, null=False, blank=False)
    response = models.JSONField(null=True)
    objects = models.Manager()

    class Meta:
        verbose_name_plural = "currencies"
        ordering = [
            "code"
        ]

    def __str__(self):
        return self.code if self.code else ''

    def get_latest_price(self, exchange, quote, key):
        if self.code != quote:
            try:
                tickers = Tickers.objects.get(market__quote__code=quote,
                                              market__base__code=self.code,
                                              market__type='spot',
                                              market__exchange=exchange,
                                              year=get_year(),
                                              semester=get_semester())
            except ObjectDoesNotExist:
                log.error('Unable to get price for {0}'.format(self.code))

            else:
                dt = datetime.now().replace(minute=0, second=0, microsecond=0)
                now = dt.strftime(datetime_directive_ISO_8601)

                try:
                    dic = tickers.data[now]

                except KeyError:
                    log.error('Key {0} not found'.format(now),
                              key=key,
                              code=self.code,
                              wallet='spot',
                              quote=quote)
                else:
                    if isinstance(key, list):
                        return [dic[k] for k in key]
                    else:
                        return dic[key]
        else:
            if isinstance(key, list):
                return 1, 1
            else:
                return 1


class Market(models.Model):
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name='market', null=True)
    type = models.CharField(max_length=20, blank=True, null=True, choices=(('spot', 'spot'),
                                                                           ('derivative', 'derivative'),))
    wallet = models.CharField(max_length=20, blank=True, null=True)
    contract_type = models.CharField(max_length=20, blank=True, null=True)
    status = models.CharField(max_length=20, blank=True, null=True)
    order_types = models.CharField(max_length=20, blank=True, null=True)

    delivery_date = models.DateTimeField(null=True, blank=True)
    onboard_date = models.DateTimeField(null=True, blank=True)
    dt_created = models.DateTimeField(auto_now=True)

    margined = models.ForeignKey(Currency, on_delete=models.CASCADE,
                                 related_name='market_margined',
                                 null=True, blank=True)

    contract_currency = models.ForeignKey(Currency,
                                          on_delete=models.CASCADE,
                                          related_name='market_contract',
                                          null=True,
                                          blank=True
                                          )
    contract_value = models.FloatField(null=True, blank=True)
    base = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='market_base', null=True)
    quote = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='market_quote', null=True)
    maker, taker = [models.FloatField(null=True, blank=True) for i in range(2)]
    amount_min, amount_max = [models.FloatField(null=True, blank=True) for i in range(2)]
    price_min, price_max = [models.FloatField(null=True, blank=True) for i in range(2)]
    cost_min, cost_max = [models.FloatField(null=True, blank=True) for i in range(2)]
    trading = models.BooleanField(null=True, default=None)
    symbol = models.CharField(max_length=50, null=True, blank=True)
    limits, precision, response = [models.JSONField(null=True) for i in range(3)]
    listing_date = models.DateTimeField(null=True, blank=True)
    order_book = models.JSONField(null=True, blank=True)
    config = models.JSONField(null=True, blank=True)
    updated = models.BooleanField(null=True, default=False)
    funding_rate = models.JSONField(null=True, blank=True)
    top = models.BooleanField(null=True, default=None)
    objects = models.Manager()

    class Meta:
        verbose_name_plural = "markets"

    def __str__(self):
        space = '____' if self.exchange.exid in ['ftx'] else '__'
        space = '___' if self.exchange.exid in ['bybit'] else space
        ex = self.exchange.exid[:4]
        # type = self.derivative[:4] if self.derivative in ['perpetual', 'future'] else 'spot'
        type = self.type[:4] if self.type == 'derivative' else 'spot'
        return ex + space + type + '__' + self.symbol

    # Return latest price
    def get_latest_price(self):

        try:
            tickers = Tickers.objects.get(market=self,
                                          year=get_year(),
                                          semester=get_semester())
        except ObjectDoesNotExist:
            log.error('Unable to get price for {0}'.format(self.symbol))

        else:

            dt = datetime.now().replace(minute=0, second=0, microsecond=0)
            now = dt.strftime(datetime_directive_ISO_8601)

            try:
                price = tickers.data[now]['last']
            except KeyError:
                log.error('Key {0} not found'.format(now),
                          symbol=self.symbol,
                          wallet=self.wallet
                          )
            else:
                return price

    # Return True if prices and volume are updated
    def is_updated(self):

        # Determine datetime and semester
        dt = timezone.now().replace(minute=0, second=0, microsecond=0)
        now = dt.strftime(datetime_directive_ISO_8601)
        semester = 1 if dt.month <= 6 else 2

        ticker = Tickers.objects.get(year=dt.year, semester=semester, market=self)

        if now in ticker.data.keys():
            return True
        else:
            return False

    #######################

    # Return True if a market has candles
    def is_populated(self):
        if Candle.objects.filter(market=self).exists():
            return True
        else:
            # log.info('Market not populated',
            #          wallet=self.wallet,
            #          symbol=self.symbol,
            #          exchange=self.exchange.exid
            #          )
            return False

    # Return True if recent candles are missing
    def has_gap(self):
        if self.is_populated():
            last = self.get_candle_datetime_last()
            now = timezone.now()
            gap = (now - last).seconds / 60

            # Multiply by 3 because datetime is at the open
            if gap > self.exchange.update_frequency * 3:
                log.warning('Gap detected in market',
                            market=self.symbol,
                            wallet=self.wallet,
                            exchange=self.exchange.exid
                            )
                return True
            else:
                return False
        else:
            return False

    # return sum of volume over last n hours
    def get_candle_volume_sum(self, hours):
        return Candle.objects.filter(market=self,
                                     dt__gte=timezone.now() - timedelta(hours=hours)
                                     ).aggregate(models.Sum('vo_avg'))

    # Create a Pandas dataframe
    def get_candle_dataframe(self, fields):

        import numpy as np
        import pandas as pd

        df = Candle.objects.filter(market=self).reverse().to_timeseries(fields, index='dt', verbose=True)

        # create array for multiindex
        index = [
            np.array([self.symbol]),
            np.array(["close"]),
        ]

        # merge columns vol and vol_avg
        if 'volume' in fields:
            df['volume'] = df['volume'].fillna(df['volume_avg'])
            del df['volume_avg']

            # create multiindex for columns
            index = [
                np.array([self.symbol] * 3),
                np.array(["close", "volume", "volume_mcap"]),
            ]

        df.columns = pd.MultiIndex.from_arrays(index)
        return df

    # Return last price
    def get_candle_price_last(self):
        if self.is_updated():
            return Candle.objects.filter(market=self).latest('dt').close

    # Return last datetime
    def get_candle_datetime_last(self):
        return Candle.objects.filter(market=self).latest('dt').dt

    # Return first datetime
    def get_candle_datetime_first(self):
        return Candle.objects.filter(market=self).latest('-dt').dt

    # Return all datetime
    def get_candle_datetime_all(self):
        return Candle.objects.filter(market=self).all.dt


class Candle(models.Model):
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name='candle', null=True)
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='candle', null=True)
    dt = models.DateTimeField()
    close, volume, volume_avg, mcap, volume_mcap = [models.FloatField(null=True) for i in range(5)]
    dt_created = models.DateTimeField(auto_now=True)
    objects = DataFrameManager()  # activate custom manager

    class Meta:
        verbose_name_plural = 'candles'
        unique_together = ['market', 'dt']
        ordering = ['-dt']
        get_latest_by = 'dt'

    def __str__(self):
        return str(self.dt.strftime("%Y-%m-%d %H:%M:%S"))


class Listing(models.Model):
    data = models.JSONField(null=True, blank=True)
    dt = models.DateTimeField(null=True)
    dt_created = models.DateTimeField(auto_now=True)
    objects = DataFrameManager()  # activate custom manager

    class Meta:
        verbose_name_plural = 'listings'
        unique_together = ['dt_created']
        ordering = ['-dt_created']
        get_latest_by = 'dt_created'

    def __str__(self):
        return str(self.dt.strftime("%Y-%m-%d %H:%M:%S"))


class CoinPaprika(models.Model):
    year = models.IntegerField(blank=True, null=True)
    semester = models.IntegerField(blank=True, null=True)
    name = models.CharField(max_length=100, blank=True, null=True)
    currency = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='coinpaprika', null=True)
    data = models.JSONField(null=True, blank=True)
    dt_created = models.DateTimeField(auto_now=True)
    objects = DataFrameManager()  # activate custom manager

    class Meta:
        verbose_name_plural = 'coinpaprika'
        unique_together = ['currency', 'year', 'semester']
        get_latest_by = 'dt_created'

    def __str__(self):
        return str(self.dt_created.strftime("%Y-%m-%d %H:%M:%S"))


class Candles(models.Model):
    year = models.IntegerField(blank=True, null=True)
    semester = models.IntegerField(blank=True, null=True)
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='candles', null=True)
    data = models.JSONField(null=True, blank=True)
    dt_created = models.DateTimeField(auto_now=True)
    objects = DataFrameManager()  # activate custom manager

    class Meta:
        verbose_name_plural = 'candles'
        unique_together = ['market', 'year', 'semester']
        get_latest_by = 'dt_created'

    def __str__(self):
        return str(self.dt_created.strftime("%Y-%m-%d %H:%M:%S"))


class Tickers(models.Model):
    year = models.IntegerField(blank=True, null=True)
    semester = models.IntegerField(blank=True, null=True)
    market = models.ForeignKey(Market, on_delete=models.CASCADE, related_name='tickers', null=True)
    data = models.JSONField(null=True, blank=True)
    dt_created = models.DateTimeField(auto_now=True)
    objects = DataFrameManager()  # activate custom manager

    class Meta:
        verbose_name_plural = 'tickers'
        unique_together = ['market', 'year', 'semester']
        get_latest_by = 'dt_created'

    def __str__(self):
        return str(self.dt_created.strftime("%Y-%m-%d %H:%M:%S"))
