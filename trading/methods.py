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
        trades=response['trades'],
        response=response
    )

    market = Market.objects.get(exchange=account.exchange,
                                default_type=default_type,
                                symbol=response['symbol']
                                )
    args = dict(account=account, market=market, orderId=response['id'])

    obj, created = Order.objects.update_or_create(**args, defaults=defaults)

    if created:
        log.info('Order object created', orderId=obj.orderId)
    else:
        log.info('Order object updated', orderId=obj.orderId)


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


# Format decimal
def format_decimal(number, precision, account):
    # # rounding mode
    # TRUNCATE = 0
    # ROUND = 1
    #
    # # digits counting mode
    # DECIMAL_PLACES = 2
    # SIGNIFICANT_DIGITS = 3
    # TICK_SIZE = 4
    #
    # # padding mode
    # NO_PADDING = 5
    # PAD_WITH_ZERO = 6

    # if account.name == 'OKEx future USD':
    #     print('\naccount', account.name)
    #     print('precision_mode', account.exchange.precision_mode)
    #     test1 = ccxt.decimal_to_precision(number,
    #                                       rounding_mode=0,
    #                                       precision=precision,
    #                                       counting_mode=account.exchange.precision_mode)
    #
    #     test2 = ccxt.decimal_to_precision(number,
    #                                       rounding_mode=1,
    #                                       precision=precision,
    #                                       counting_mode=account.exchange.precision_mode)
    #
    #     print('test', number, test1, test2, '\n')

    return ccxt.decimal_to_precision(number,
                                     rounding_mode=0,
                                     precision=precision,
                                     counting_mode=account.exchange.precision_mode)


# Return USD value
##################

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

