import marketsdata.models as m
import strategy.models as s
from trading.models import Position, Fund, Order, Account
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


# Create/update an order object with response returned by exchange
def order_create_update(account, response, default_type=None):

    order = Order.objects.get(orderid=response['id'])

    # Create dictionary
    defaults = dict(
        amount=response['amount'],
        average=response['average'],
        cost=response['cost'],
        datetime=response['datetime'],
        fee=response['fee'],
        filled=float(response['filled']),
        response=response,
        last_trade_timestamp=response['lastTradeTimestamp'],
        price=response['price'],
        remaining=response['remaining'],
        side=response['side'],
        status=response['status'],
        timestamp=response['timestamp'],
        trades=response['trades'],
        type=response['type']
    )

    market = Market.objects.get(exchange=account.exchange,
                                default_type=default_type,
                                symbol=response['symbol']
                                )

    # Create filter and create / update
    args = dict(account=account, market=market, orderid=response['id'])
    obj, created = Order.objects.update_or_create(**args, defaults=defaults)

    if created:
        pass
    else:

        dic = dict(
            amount=obj.amount,
            filled=obj.filled,
            orderid=obj.orderid,
            pk=str(obj.id),
            remaining=obj.remaining,
            side=obj.side
        )

        if obj.filled > float(order.filled):

            dic['new_trade'] = True

            # Signal trade engine an order is filled
            if obj.status == 'close':
                log.info('Order filled', orderid=obj.orderid, filled=obj.filled, amount=obj.amount)
                dic['status'] = 'close'

            # Signal trade engine new trade occurred but order isn't filled
            else:
                log.info('Trade detected', orderid=obj.orderid, filled=obj.filled, amount=obj.amount)
                dic['status'] = 'open'

        else:
            dic['new_trade'] = False
            dic['status'] = 'open'

        return dic


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

    return ccxt.decimal_to_precision(n,
                                     rounding_mode=0,
                                     precision=precision,
                                     counting_mode=counting_mode,
                                     padding_mode=5
                                     )


# Return amount limit min or amount limit max if condition is not satisfy
def limit_amount(market, amount):

    # Check amount limits
    if market.limits['amount']['min']:
        if amount < market.limits['amount']['min']:
            log.warning('Amount < limit min', market=market.symbol, type=market.type,
                        amount=amount, limit=market.limits['amount']['min'])
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
def limit_cost(market, amount, price):

    # Check cost limits
    if market.limits['cost']['min']:
        if amount * price < market.limits['cost']['min']:
            log.warning('Cost < limit min', cost=amount * price, limit=market.limits['cost']['min'])
            return False

    if market.limits['cost']['max']:
        if amount * price > market.limits['cost']['max']:
            log.warning('Cost > limit max', cost=amount * price, limit=market.limits['cost']['max'])
            return False

    return True


# Convert currency amount to derivative contract quantity
def amount_to_contract(market, amount):

    if market.exchange.exid == 'binance':
        if market.type == 'derivative':

            last = market.get_candle_price_last()

            # COIN-margined see https://www.binance.com/en/futures/trading-rules/quarterly
            if market.response['info']['marginAsset'] == market.response['base']:
                contract_value = market.response['info']['contractSize']  # Select USD value of 1 contract
                return amount * last / contract_value

            # USDT-margined see https://www.binance.com/en/futures/trading-rules
            elif market.response['info']['marginAsset'] == market.response['quote']:
                return amount


# Convert contract quantity to currency amount
def contract_to_amount(market, contract):

    if market.exchange.exid == 'binance':
        if market.type == 'derivative':

            last = market.get_candle_price_last()

            # COIN-margined see https://www.binance.com/en/futures/trading-rules/quarterly
            if market.response['info']['marginAsset'] == market.response['base']:
                contract_value = market.response['info']['contractSize']  # Select USD value of 1 contract
                return contract * contract_value / last

            # USDT-margined see https://www.binance.com/en/futures/trading-rules
            elif market.response['info']['marginAsset'] == market.response['quote']:
                return contract


# return USD value of spot account
def get_spot_balance_usd(account):
    try:
        balance = sum([convert_to_usd(f.free, f.currency.code, f.type, f.exchange) for f in account.get_funds('spot')])
    except Exception as e:
        print(e)
        raise TradingError('Unable to get balance of spot account in USD for {0}'.format(account.name))
    else:
        return balance


# return USD value of future account
def get_future_balance_usd(account):
    try:
        balance = sum([convert_to_usd(f.total, f.currency.code, f.type, f.exchange) for f in account.get_funds('future')])
    except Exception as e:
        print(e)
        raise TradingError('Unable to get balance of future account in USD for {0}'.format(account.name))
    else:
        return balance


# Convert base
##############

# quantity in USD
def convert_to_usd(quantity, base, tp, exchange):
    if base not in ['USD', 'USDT']:
        try:
            market = Market.objects.get(base__code=base, type=tp, exchange=exchange, quote__code='USD')
        except ObjectDoesNotExist:
            market = Market.objects.get(base__code=base, type=tp, exchange=exchange, quote__code='USDT')
            return quantity * market.get_last_price()
        except Exception as e:
            print(e)
            raise TradingError('Unable to select {0}/USDT {2} market for {1}'.format(base.code, exchange.ccxt, tp))
        else:
            return quantity * market.get_last_price()
    else:
        return 1


# Convert USD amount to base
def convert_to_base(amount_usd, base, exchange):
    try:
        market = Market.objects.get(base=base, type__in=['spot', None], exchange=exchange, quote__code='USD')
    except ObjectDoesNotExist:
        try:
            market = Market.objects.get(base=base, type__in=['spot', None], exchange=exchange, quote__code='USDT')
        except ObjectDoesNotExist:
            raise TradingError('Unable to select {0}/USD spot market on {1}'.format(base.code, exchange.ccxt))
        else:
            return amount_usd / market.get_last_price()
    else:
        return amount_usd / market.get_last_price()


# Return spot balance
# ####################

# account total fund
def get_spot_balance_total(account, base):
    for f in account.get_funds('spot'):
        if f.currency == base:
            return f.total


# Return spot account available fund
def get_spot_balance_free(account, base):
    for f in account.get_funds('spot'):
        if f.currency == base:
            return f.free


# Return spot account used fund (open orders)
def get_spot_balance_used(account, base):
    for f in account.get_funds('spot'):
        if f.currency == base:
            return f.used

# Return future account balance
###############################

# Return future account total fund
def get_future_balance_total(account, base):
    for f in account.get_funds('future'):
        if f.currency == base:
            return f.total


# Return future account available fund
def get_future_balance_free(account, base):
    for f in account.get_funds('future'):
        if f.currency == base:
            return f.free


# Return quantity
#################

# Return position quantity
def get_position_size(account, base):
    if account.exchange.ccxt == 'binance':
        for position in account.get_positions():
            if position.market.base == base:
                if position.market.contract_value_currency == base:
                    return float(position.size)
                else:
                    raise TradingError('get_position_quantity() cannot get position quantity')
        # if no position is found return 0
        return 0
    else:
        raise TradingError('get_position_quantity() cannot get position quantity')


# Calculate target quantity of an allocation (base)
def calculate_target_quantity(account, exchange, base):
    for allocation in account.bases_alloc_no_margin() + account.bases_alloc_margin():
        if base == allocation.market.base:

            # calculate USD value from weight (%)
            balance_total = get_spot_balance_usd(account) + get_future_balance_usd(account)
            usd = allocation.weight * balance_total

            # convert from USD to base
            quantity = convert_to_base(usd, base, exchange)

            return quantity

