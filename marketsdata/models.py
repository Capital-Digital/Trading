import time
from datetime import datetime, date
import ccxt, ccxtpro
from django.contrib.postgres.fields import JSONField
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned
from django.db import models
from django_pandas.managers import DataFrameManager
from marketsdata.methods import *
import pandas as pd
from pprint import pprint

log = structlog.get_logger(__name__)


class Exchange(models.Model):
    objects = models.Manager()
    exid = models.CharField(max_length=12, blank=True, null=True)
    supported_market_types = models.CharField(max_length=50, blank=True, null=True)
    dollar_currency = models.CharField(max_length=4, blank=False, null=True)
    name, version = [models.CharField(max_length=12, blank=True, null=True) for i in range(2)]
    api, countries, urls, has, timeframes, credentials, options = [JSONField(blank=True, null=True) for i in range(7)]
    timeout = models.IntegerField(default=30000)
    rate_limit = models.IntegerField(default=10000)
    precision_mode = models.IntegerField(null=True, blank=True)
    status_at, eta = [models.DateTimeField(blank=True, null=True) for i in range(2)]
    url = models.URLField(blank=True, null=True)
    start_date = models.DateField(null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    verbose = models.BooleanField(default=False)
    enable_rate_limit = models.BooleanField(default=True)
    limit_ohlcv = models.PositiveIntegerField(null=True, blank=True)
    # api_credit = models.PositiveSmallIntegerField(null=True, default=0)
    credit = JSONField(blank=True, null=True)

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

    # Return True is exchange status is OK
    def is_active(self):
        if self.status == 'ok':
            return True
        else:
            log.error('Exchange status is not OK', status=self.status, status_at=self.status_at, eta=self.eta)
            return False

    # Return exchange class (ccxt)
    def get_ccxt_client(self, account=None, ccxt_type_options=None):

        if not self.is_active():
            log.error('Exchange is inactive', exchange=self.exid)
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
            client.secret = account.api_secret
            client.apiKey = account.api_key
            if self.credentials['password']:
                client.password = account.password

        if ccxt_type_options:
            if 'defaultType' in client.options:
                client.options['defaultType'] = ccxt_type_options

        self.options = client.options
        self.save()

        return client

    def get_ccxt_client_pro(self, account=None, market=None):

        if not self.is_active():
            log.error('Exchange is inactive', exchange=self.exid)
            return

        client = getattr(ccxtpro, self.exid)
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

    # return a list of market types (spot or derivative)
    def get_market_types(self):
        return list(set(Market.objects.filter(exchange=self).values_list('type', flat=True)))

    # return a list of supported market types ccxt ('', spot, swap, futures, futures)
    def get_market_ccxt_type_options(self):
        if 'defaultType' in self.get_ccxt_client().options:

            # Return a list of supported ccxt types
            if self.supported_market_types:
                return str(self.supported_market_types).replace(" ", "").split(',')
            else:
                raise Exception('Exchange {0} requires a parameter defaultType'.format(self.exid))
        else:
            return None

    # return accounts linked to this exchange
    def get_trading_accounts(self):
        from trading.models import Account
        return Account.objects.filter(exchange=self, trading=True)


class CurrencyType(models.Model):
    type = models.CharField(max_length=20, null=True, choices=(('quote', 'quote'),
                                                               ('base', 'base')))

    class Meta:
        verbose_name_plural = "Types (currencies)"

    def __str__(self):
        return self.type


class Currency(models.Model):
    name, code = [models.CharField(max_length=100, blank=True, null=True) for i in range(2)]
    exchange = models.ManyToManyField(Exchange, related_name='currency')
    type = models.ManyToManyField(CurrencyType, related_name='currency')
    stable_coin = models.BooleanField(default=False, null=False, blank=False)
    objects = models.Manager()

    class Meta:
        verbose_name_plural = "currencies"
        ordering = [
            "code"
        ]

    def __str__(self):
        return self.code if self.code else ''


class Market(models.Model):
    exchange = models.ForeignKey(Exchange, on_delete=models.CASCADE, related_name='market', null=True)
    type = models.CharField(max_length=20, blank=True, null=True, choices=(('spot', 'spot'),
                                                                           ('derivative', 'derivative'),))
    ccxt_type_response = models.CharField(max_length=20, blank=True, null=True)
    ccxt_type_options = models.CharField(max_length=20, blank=True, null=True)
    derivative = models.CharField(max_length=20, blank=True, null=True, choices=(('perpetual', 'perpetual'),
                                                                                 ('future', 'future'),))
    delivery_date = models.DateTimeField(null=True, blank=True)
    margined = models.ForeignKey(Currency, on_delete=models.CASCADE,
                                 related_name='market_margined',
                                 null=True, blank=True)
    contract_value_currency = models.ForeignKey(Currency, on_delete=models.CASCADE,
                                                related_name='market_contract',
                                                null=True, blank=True)
    contract_value = models.FloatField(null=True, blank=True)
    base = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='market_base', null=True)
    quote = models.ForeignKey(Currency, on_delete=models.CASCADE, related_name='market_quote', null=True)
    maker, taker = [models.FloatField(null=True, blank=True) for i in range(2)]
    amount_min, amount_max = [models.FloatField(null=True, blank=True) for i in range(2)]
    price_min, price_max = [models.FloatField(null=True, blank=True) for i in range(2)]
    cost_min, cost_max = [models.FloatField(null=True, blank=True) for i in range(2)]
    active = models.BooleanField(null=True, default=None)
    symbol = models.CharField(max_length=50, null=True, blank=True)
    limits, precision, response = [JSONField(null=True) for i in range(3)]
    listing_date = models.DateTimeField(null=True, blank=True)
    order_book = JSONField(null=True, blank=True)
    config = JSONField(null=True, blank=True)
    excluded = models.BooleanField(null=True, default=False)
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

    # Return True is a market has candles
    def is_populated(self):
        if Candle.objects.filter(market=self).exists():
            return True
        else:
            return False

    # Return True is a market is updated
    def is_updated(self):
        if self.is_populated():
            if self.is_future_expired():
                return None
            else:
                dt = timezone.now().replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
                if self.get_candle_datetime_last() == dt:
                    return True
                else:
                    return False
        else:
            return False

    # Return True if a future has expired
    def is_future_expired(self):
        if self.type == 'derivative' and self.derivative == 'future':
            if self.delivery_date:
                if timezone.now() > self.delivery_date:
                    return True
                else:
                    return False
            else:
                return False
                # raise Exception('Future {0} has not delivery date'.format(self.symbol))

    # return sum of volume over last hours
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
                np.array([self.symbol] * 2),
                np.array(["close", "volume"]),
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
    close, volume, volume_avg = [models.FloatField(null=True) for i in range(3)]
    dt_created = models.DateTimeField(auto_now=True)
    objects = DataFrameManager()  # activate custom manager

    class Meta:
        verbose_name_plural = 'candles'
        unique_together = ['market', 'dt']
        ordering = ['-dt']
        get_latest_by = 'dt'

    def __str__(self):
        return str(self.dt.strftime("%Y-%m-%d %H:%M:%S"))


class OrderBook(models.Model):
    name = models.CharField(max_length=20, null=True)
    data = JSONField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    objects = models.Manager()

    class Meta:
        verbose_name_plural = "order books"

    def __str__(self):
        return str(self.updated_at)
