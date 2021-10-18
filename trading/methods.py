from capital.methods import *
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


def convert_balance(bal, exchange):
    def convert_value(row):
        print(row.index)
        price = Currency.objects.get(code=row.index).get_latest_price(exchange)
        return row.value * price

    return bal.apply(lambda row: convert_value(row), axis=0)


def sum_wallet_balances(dic):
    total = []
    dic = dict(dic)
    for wallet, values in dic.items():
        for key, val in values.items():
            print(val)
            total.append(dic[wallet][key])
    return sum(total)


# Create/update an order object with response returned by exchange
def order_create_update(id, response, price_hourly):
    from trading.models import Account, Order
    account = Account.objects.get(id=id)

    try:
        # Select order object with client order ID
        order = Order.objects.get(id=float(response['clientOrderId']))

        # Convert cost to USDT
        if order.market.quote.code != account.exchange.dollar_currency:
            rate = get_price_hourly(account.exchange, order.market.quote.code, account.exchange.dollar_currency)
            cost_dollar_curr = response['cost'] * rate
        else:
            cost_dollar_curr = response['cost']

        # Calculate distance with strategy price and weight with order cost
        distance = (response['price'] / price_hourly) - 1
        distance = distance if response['side'] == 'buy' else - distance
        delta = distance * cost_dollar_curr
        distance_weighted = delta / account.get_fund_latest().balance

    except ObjectDoesNotExist:
        log.warning('Order object not created')

    finally:

        # pprint(response)

        # Create filter
        args = dict(account=account, id=response['clientOrderId'])

        # Select Binance datetime
        if account.exchange.exid == 'binance':
            if not response['timestamp']:
                if 'transactTime' in response['info']:
                    response['timestamp'] = float(response['info']['transactTime'])
                elif 'updateTime' in response['info']:
                    response['timestamp'] = float(response['info']['updateTime'])

        # Convert datetime
        datetime = convert_timestamp_to_datetime(response['timestamp'] / 1000, datetime_directive_binance_order)

        # Create dictionary
        defaults = dict(
            orderid=response['id'],
            amount=response['amount'],
            average=response['average'],
            cost=response['cost'],
            datetime=datetime,
            fee=response['fee'],
            filled=float(response['filled']),
            response=response,
            last_trade_timestamp=response['lastTradeTimestamp'],
            price=response['price'],
            price_strategy=price_hourly,
            distance=distance_weighted,
            remaining=response['remaining'],
            side=response['side'],
            status=response['status'],
            timestamp=int(response['timestamp']),
            trades=response['trades'],
            type=response['type']
        )
        # pprint(defaults)
        obj, created = Order.objects.update_or_create(**args, defaults=defaults)

        # Creation
        if created:
            return

        else:
            # An order with trade is updated
            if order.filled:
                if obj.filled > order.filled:
                    return obj.orderid
            # An order with no trade is updated
            elif obj.filled:
                return obj.orderid


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

    return float(ccxt.decimal_to_precision(n,
                                           rounding_mode=0,
                                           precision=precision,
                                           counting_mode=counting_mode,
                                           padding_mode=5
                                           ))


# Return amount limit min or amount limit max if condition is not satisfy
def limit_amount(market, amount):
    # Check amount limits
    if market.limits['amount']['min']:
        if amount < market.limits['amount']['min']:
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
def limit_cost(market, cost):
    # Check cost limits
    if market.limits['cost']['min']:
        if cost < market.limits['cost']['min']:
            return False

    if market.limits['cost']['max']:
        if cost > market.limits['cost']['max']:
            # log.warning('Cost > limit max', cost=amount * price, limit=market.limits['cost']['max'])
            return False

    return True


# Return last websocket spot price if available else last hourly price
def get_price_ws(exchange, code, prices):
    if prices is not None:
        if code in prices['spot']:
            return float(prices['spot'][code]['ask'])
        else:
            log.warning('{0} not found in prices dictionary'.format(code))
            return get_price_hourly(exchange, code, exchange.dollar_currency)
    else:
        # log.warning('Spot price from websocket not found for {0}'.format(code))
        return get_price_hourly(exchange, code, exchange.dollar_currency)


# Get hourly spot price
def get_price_hourly(exchange, base, quote):

    if base == exchange.dollar_currency:
        if quote == exchange.dollar_currency:
            return 1
    try:
        market = Market.objects.get(base__code=base,
                                    quote__code=quote,
                                    exchange=exchange,
                                    type='spot'
                                    )
    except ObjectDoesNotExist:
        raise Exception('Unable to found spot market {0}/{1}'.format(base, quote))
    else:
        return market.get_candle_price_last()


# Convert contract quantity to currency amount
def contract_to_amount(market, contract):
    if market.exchange.exid == 'binance':
        if market.type == 'derivative':

            last = market.get_candle_price_last()

            # COIN-margined see https://www.binance.com/en/futures/trading-rules/quarterly
            if market.response['info']['marginAsset'] == market.response['base']:
                contract_value = float(market.response['info']['contractSize'])  # Select USD value of 1 contract
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
        balance = sum(
            [convert_to_usd(f.total, f.currency.code, f.type, f.exchange) for f in account.get_funds('future')])
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
