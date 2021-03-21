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

    # Create dictionary
    defaults = dict(
        amount=response['amount'],
        average=response['average'],
        cost=response['cost'],
        datetime=response['datetime'],
        fee=response['fee'],
        filled=response['filled'],
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
        log.info('Order object created', id=obj.id)
    else:
        log.info('Order object updated', id=obj.id)


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
            log.warning('Amount < limit min', amount=amount, limit=market.limits['amount']['min'])
            return 0

    if market.limits['amount']['max']:
        if amount > market.limits['amount']['max']:
            log.warning('Amount > limit max', amount=amount, limit=market.limits['amount']['max'])
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


# Calculate target position
# Calculate target position
# Calculate target position
# Calculate target position
# Calculate target position
# Calculate target position
# Calculate target position
# Calculate target position
# Calculate target position
def target_size_n_side(account, allocation):
    log.bind(account=account.name)

    # select the best market to trade
    market = select_market(account, allocation)

    # Calculate the value of 1 unit in USD
    if market.type is 'spot':
        contract_value_usd = 1 / market.get_last_price()
    elif market.type in ['swap', 'future', 'futures']:
        contract_value_usd = market.get_contract_value()

    # Get latest equity value
    equity_value = Fund.objects.get(account=account, total=True, dt=dt).equity

    # Calculate the target position value in USD
    position_value_target = allocation.weight * equity_value

    # Calculate synthetic short
    if allocation.market.settlement == allocation.market.base \
            and allocation.market.settlement not in ['USD', 'USDT']:
        synthetic_short = equity_value / contract_value_usd
    else:
        synthetic_short = 0

    # Calculate total number of contracts needed
    size = abs((position_value_target / contract_value_usd) - synthetic_short)
    side = 'buy' if allocation.weight > 0 else 'sell'

    # Format decimals
    size = float(format_decimal(size, allocation.market.precision['amount'], account))

    return size, side


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

