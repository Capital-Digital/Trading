import ccxt
from django.db import models
from strategy.models import Strategy, Allocation
from marketsdata.models import Exchange, Market, Currency
from trading.error import *
from django.contrib.postgres.fields import JSONField
from django.utils import timezone
import structlog
from django.db.models import Q
from datetime import timedelta, datetime
from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from pprint import pprint
from decimal import Decimal

log = structlog.get_logger(__name__)

datetime_directives_std = '%Y-%m-%dT%H:%M:%S.%fZ'


class Account(models.Model):
    objects = models.Manager()
    name = models.CharField(max_length=100, null=True, blank=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='account', blank=True, null=True)
    strategy = models.ForeignKey(Strategy, on_delete=models.SET_NULL, related_name='account', null=True)
    spot_preference = models.CharField(max_length=20, choices=[('USDT', 'USDT'), ('BTC', 'BTC')], blank=True)
    contract_preference = models.CharField(max_length=20, choices=[('perp', 'perpetual'), ('fut', 'delivery')], blank=True)
    contract_margin = models.CharField(max_length=20, choices=[('USDT', 'USDT'), ('BTC', 'BTC'), ('coin', 'underlying asset')], blank=True)
    limit_order, valid_credentials, trading = [models.BooleanField(null=True, default=None) for i in range(3)]
    limit_price_tolerance = models.DecimalField(default=0, max_digits=4, decimal_places=3)
    position_mode = models.CharField(max_length=20, choices=[('dual', 'dual'), ('hedge', 'hedge')], blank=True)
    email = models.EmailField(max_length=100, blank=True)
    api_key, api_secret = [models.CharField(max_length=100, blank=True) for i in range(2)]
    password = models.CharField(max_length=100, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True)

    class Meta:
        verbose_name_plural = "Accounts"

    def __str__(self):
        return self.name

    def is_valid_credentials(self):
        #
        # Return True if credentials are valid
        ######################################

        self.set_credentials()
        return self.valid_credentials

    def set_credentials(self):
        #
        # Check crendentials validity
        #############################

        try:
            client = self.exchange.get_ccxt_client(self)
            client.load_markets(True)
            cred = client.checkRequiredCredentials()

        except ccxt.AuthenticationError as e:
            self.valid_credentials = False

        except Exception as e:
            self.valid_credentials = False

        else:
            self.valid_credentials = cred

        finally:
            self.save()

    def get_position_mode(self):
        #
        # Determine if an account can hold long and short position at same time (hedge)
        ###############################################################################

        if self.strategy.margin:
            if self.exchange.ccxt == 'binance':

                client = self.exchange.get_client(self)
                if self.margin_preference == 'swap':
                    mode = client.fapiPrivateGetPositionSideDual()['dualSidePosition']
                elif self.margin_preference == 'future':
                    mode = client.dapiPrivateGetPositionSideDual()['dualSidePosition']
                self.position_mode = 'dual' if mode else False
                self.save()

            elif self.exchange.ccxt == 'okex':

                self.position_mode = 'hedge'
                self.save()

    def get_latest_order_dt(self, market):
        #
        # return datetime of latest order
        #################################

        orders = Order.objects.filter(account=self, market=market)
        if orders.exists():
            return orders.latest('dt_create').dt

    def get_funds(self, account_type):
        #
        # return funds
        ##############

        dt = timezone.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        funds = self.funds.all().filter(dt=dt, type=account_type)
        if funds.exists():
            lst = []
            for fund in funds:
                lst.append(fund)
            return lst
        else:
            raise TradingError('Unable to select {1} funds for {0}'.format(self.name, account_type))

    def get_positions(self):
        #
        # Return positions
        ##################

        positions = self.positions.all()
        if positions.exists():
            lst = []
            for p in positions:
                lst.append(p)
            return lst
        else:
            return []

    def get_positions_long(self):
        #
        # Return long positions
        #######################

        positions = self.positions.all().filter(side='buy')
        if positions.exists():
            lst = []
            for p in positions:
                lst.append(p)
            return lst
        else:
            return []

    def get_positions_short(self):
        #
        # Return short positions
        ########################

        positions = self.positions.all().filter(side='sell')
        if positions.exists():
            lst = []
            for p in positions:
                lst.append(p)
            return lst
        else:
            return []

    def bases_alloc_no_margin(self):
        #
        # Select currencies that need to be traded without margin
        #########################################################

        dt = self.strategy.get_latest_alloc_dt()
        allocations = Allocation.objects.filter(strategy=self.strategy, dt=dt)
        if allocations.exists():
            lst = []
            for a in allocations:
                if not a.margin or (a.margin and a.side == 'buy'):
                    lst.append(a)
            return lst
        else:
            return []

    # Select bases (margin short) from the new allocations
    ######################################################
    def get_allocations_short(self):

        dt = self.strategy.get_latest_alloc_dt()
        allocations = Allocation.objects.filter(strategy=self.strategy, dt=dt)
        if allocations.exists():
            lst = []
            for a in allocations:
                if a.margin and a.side == 'sell':
                    lst.append(a)
            return lst

    # Transfer funds between accounts
    #################################
    def transfer_fund(self):

        if self.exchange.ccxt == 'okex':
            from . import okex
            okex.transfer_funds(self)

    # Return True if fund account was created in the last 5 minutes
    ###############################################################
    def is_fund_updated(self):

        return True if (timezone.now() - self.fund.latest('dt_create').dt_create).seconds < 60 * 5 else False

    # Return True if position has been updated recently
    ###################################################
    def is_positions_updated(self):

        return True if all([position.is_updated() for position in self.position.all()]) else False

    # Return True if account exchange is compatible with strategy
    #############################################################
    def is_compatible(self):

        # Fire an exception if one currency from the strategy isn't available on exchange
        def check(strategy):

            # set type to future/swap if strategy.margin else all types
            bases, quote, margin = strategy.get_markets(flat=True)
            tp = ['swap', 'future', 'futures'] if margin else ['swap', 'spot', 'future', 'futures', None]

            # try to select all bases from the strategy
            for base in bases:
                try:
                    Market.objects.get(exchange=self.exchange, type__in=tp, base__code=base)
                except ObjectDoesNotExist:
                    mk = 'future market' if margin else 'spot market'
                    raise SettingError('{1} {0} is not available on {2}'.format(mk,
                                                                                        base,
                                                                                        self.exchange.ccxt))
                except MultipleObjectsReturned:
                    pass

        if self.strategy.master:
            for strategy in self.strategy.sub_strategies.all():
                check(strategy)
        else:
            check(self.strategy)
        return True

    # Create/update an order object with response returned by exchange
    ##################################################################
    def order_create_update(self, response, tp=None):

        market = Market.objects.get(exchange=self.exchange, type=tp, symbol=response['symbol'])
        args = dict(account=self, market=market, orderId=response['id'])

        defaults = dict(
            clientOrderId=response['clientOrderId'],
            timestamp=response['timestamp'],
            datetime=response['datetime'],
            last_trade_timestamp=response['lastTradeTimestamp'],
            type=response['type'],
            side=response['side'],
            price=response['price'],
            amount=response['amount'],
            cost=response['cost'],
            average=response['average'],
            filled=response['filled'],
            remaining=response['remaining'],
            status=response['status'],
            fee=response['fee'],
            trades=response['trades']
        )

        object, created = Order.objects.update_or_create(**args, defaults=defaults)

        if created:
            log.info('Order object created', orderId=object.orderId)
        else:
            log.info('Order object updated', orderId=object.orderId)

    # Create an order object (to open, add, remove or close a position)
    ###################################################################
    def order_create(self, market, market_type, type, side, size):

        from trading.methods import format_decimal
        market = Market.objects.get(symbol=market, type=market_type, exchange=self.exchange)

        # Format decimal
        size = format_decimal(size, market.precision['amount'], self)

        defaults = dict(
            account=self,
            api=True,
            market=market,
            size=str(size),
            status='created',
            price_strategy=market.get_last_price(),
            type=type
        )

        # Set price
        if type == 'limit':
            price = market.get_last_price()
            price = format_decimal(price, market.precision['price'], self)
            defaults['price'] = str(price)

        log.info('Create order object')
        pprint(defaults)
        Order.objects.create(**defaults)

    # Create, delete or update a position object
    ############################################
    def create_update_delete_position(self, market, defaults):

        side = defaults['side']
        size = defaults['size']

        # calculate position value in usd
        value = size * market.contract_value if side == 'buy' else -size * market.contract_value
        if market.contract_value_currency.code not in ['USD', 'USDT']:
            value = value * float(defaults['last'])

        # set new value
        defaults['value_usd'] = round(value, 2)

        # create search arguments
        args = dict(exchange=self.exchange, account=self, market=market)

        try:

            position = Position.objects.get(**args)

        except Position.DoesNotExist:

            if size > 0:
                Position.objects.update_or_create(**args, defaults=defaults)
        else:

            if size == 0:
                log.info('Delete position', account=self.name, market=market.symbol)
                position.delete()
            else:
                Position.objects.update_or_create(**args, defaults=defaults)


class Fund(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='funds', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='funds', null=True)
    currency = models.ForeignKey(Currency, on_delete=models.SET_NULL, related_name='funds', null=True)
    type = models.CharField(max_length=20, null=True, blank=True)
    free, total, used, margin = [models.FloatField(null=True) for i in range(4)]
    response = JSONField(null=True)
    dt = models.DateTimeField(null=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        verbose_name_plural = "Funds"
        ordering = ['-dt_create']
        get_latest_by = 'dt_create'

    def __str__(self):
        return str(self.id)


class Order(models.Model):
    objects = models.Manager()
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='order', null=True)
    market = models.ForeignKey(Market, on_delete=models.SET_NULL, related_name='order', null=True)
    orderId, clientOrderId, status, type = [models.CharField(max_length=150, null=True) for i in range(4)]
    amount, filled, remaining, max_qty = [models.CharField(max_length=10, null=True) for i in range(4)]
    side = models.CharField(max_length=10, null=True, choices=(('buy', 'buy'), ('sell', 'sell')))
    cost = models.FloatField(null=True)
    trades = models.CharField(max_length=10, null=True)
    average, price, price_strategy, contract_val, leverage = [models.FloatField(null=True) for i in range(5)]
    api = models.BooleanField(null=True)
    fee = JSONField(null=True)
    params = JSONField(null=True)
    response = JSONField(null=True)
    datetime, last_trade_timestamp = [models.DateTimeField(null=True) for i in range(2)]
    timestamp = models.BigIntegerField(null=True)
    dt_update = models.DateTimeField(auto_now=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        verbose_name_plural = "Orders"

    def __str__(self):
        return self.type


class Position(models.Model):
    objects = models.Manager()
    market = models.ForeignKey(Market, on_delete=models.SET_NULL, related_name='positions', null=True)
    exchange = models.ForeignKey(Exchange, on_delete=models.SET_NULL, related_name='positions', null=True)
    account = models.ForeignKey(Account, on_delete=models.CASCADE, related_name='positions', null=True)
    last, liquidation_price = [models.FloatField(null=True) for i in range(2)]
    size, size_available = [models.CharField(max_length=10, null=True) for i in range(2)]
    entry_price = models.FloatField(null=True)
    margin, margin_maint_ratio, sub_account_equity, margin_ratio = [models.FloatField(null=True) for i in range(4)]
    realized_pnl, unrealized_pnl, value_usd = [models.FloatField(null=True) for i in range(3)]
    instrument_id, side, margin_mode = [models.CharField(max_length=150, null=True) for i in range(3)]
    leverage = models.DecimalField(null=True, max_digits=5, decimal_places=2)
    created_at = models.DateTimeField(null=True)
    response = JSONField(null=True)
    dt_update = models.DateTimeField(auto_now=True)
    dt_create = models.DateTimeField(default=timezone.now, editable=False)

    class Meta:
        verbose_name_plural = "Positions"

    def __str__(self):
        return self.exchange.exid

    # Return margin ratio
    def get_margin_ratio(self):
        if self.is_updated():
            if self.account.is_fund_updated():
                fund = self.account.fund.latest('dt_create')
                return (self.margin * float(self.leverage)) / fund.equity
            else:
                log.error('Cannot calculate margin ratio, fund is not updated')
        else:
            log.error('Cannot calculate margin ratio, position is not updated')

    # Return True if a position has been updated recently
    def is_updated(self):
        return True if (timezone.now() - self.dt_update).seconds < 60 * 5 else False

    # Create an order to create a new position
    def close(self):
        log.bind(account=self.account.name)

        log.info('Create position')

        type_order = 'open_long' if self.side == 'long' else 'open_short'
        self.account.create_order(self.market, self.size, type_order)

    # Create an order to add contracts to a position
    def add(self, size):
        log.bind(account=self.account.name)

        log.info('Add contracts to position')

        type_order = 'open_long' if self.side == 'long' else 'open_short'
        self.account.create_order(self.market, size, type_order)

    # Create an order to remove contracts to a position
    def remove(self, size):
        log.bind(account=self.account.name)

        log.info('Remove contracts to position')

        type_order = 'close_long' if self.side == 'long' else 'close_short'
        self.account.create_order(self.market, size, type_order)

    # Create an order to close an open position
    def close(self):
        log.bind(account=self.account.name)

        log.info('Close position')

        type_order = 'close_long' if self.side == 'long' else 'close_short'
        self.account.create_order(self.market, self.size, type_order)
