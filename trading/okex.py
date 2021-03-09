import marketsdata.models as m
import strategy.models as s
from .models import Position, Fund, Order
from django.utils import timezone
import structlog
from datetime import timedelta, datetime
from pprint import pprint
import ccxt
from .methods import insert_remove_position

log = structlog.get_logger(__name__)

datetime_directives_std = '%Y-%m-%dT%H:%M:%S.%fZ'


# Retrieve information on open positions of a single contract.
# Rate Limit: 20 requests per 2 seconds (Depending on the underlying speed limit)
def refresh_positions(account):
    client = account.exchange.get_client(account)
    markets = account.markets.all()

    for market in markets:

        # Download position by 'instrument_id'
        params = {'instrument_id': market.info['instrument_id']}
        if market.type == 'swap':
            response = client.swap_get_instrument_id_position(params)
        elif market.type in ['future', 'futures']:
            response = client.futures_get_instrument_id_position(params)
            if not response['result']:
                log.error('Position response is incorrect', account=account.name, response=response)
                return

        # Loop through positions
        for p in response['holding']:

            # Create a dictionary with common keys for swap and future
            defaults = {'last': float(p['last']),
                        'liquidation_price': float(p['liquidation_price']),
                        'instrument_id': p['instrument_id'],
                        'response': p,
                        'leverage': p['leverage']
                        }

            if market.type == 'swap':

                dic = {'size_available': p['avail_position'],
                       'size': float(p['position']),
                       'side': p['side'],
                       'margin_mode': response['margin_mode'],
                       'entry_price': float(p['avg_cost']),
                       'margin_maint_ratio': float(p['maint_margin_ratio']),
                       'realized_pnl': float(p['realized_pnl']),
                       'unrealized_pnl': float(p['unrealized_pnl']),
                       'created_at': timezone.make_aware(datetime.strptime(p['timestamp'],
                                                                           datetime_directives_std))
                       }
                insert_remove_position(account, market, {**defaults, **dic})

            elif market.type in ['future', 'futures']:

                dic = {'created_at': timezone.make_aware(datetime.strptime(p['created_at'],
                                                                           datetime_directives_std)),
                       'margin_mode': p['margin_mode'],
                       'realized_pnl': float(p['realised_pnl']),
                       'size_available': float(p['long_avail_qty']),
                       'size': float(p['long_qty']),
                       'entry_price': float(p['long_avg_cost']),
                       'margin': float(p['long_margin']),
                       'unrealized_pnl': float(p['long_unrealised_pnl']),
                       'side': 'long'}

                insert_remove_position(account, market, {**defaults, **dic})

                dic['size_available'] = float(p['short_avail_qty'])
                dic['size'] = float(p['short_qty'])
                dic['entry_price'] = float(p['short_avg_cost'])
                dic['margin'] = float(p['short_margin'])
                dic['unrealized_pnl'] = float(p['short_unrealised_pnl'])
                dic['side'] = 'short'

                insert_remove_position(account, market, {**defaults, **dic})


# Retrieve the futures account information of a single token.
# Rate Limit: 20 requests per 2 seconds (Depending on the underlying speed limit)
def create_fund(account):
    markets = account.markets.all()

    def create_fund(equity, balance, currency, response, total=False):
        kwargs = dict(
            account=account,
            exchange=account.exchange,
            balance=balance,
            equity=equity,
            currency=currency,
            total=total,
            response=response,
            dt=timezone.now().replace(minute=0, second=0, microsecond=0)
        )
        Fund.objects.create(**kwargs)

    total_balance = []
    total_equity = []
    total_response = []

    # Create one fund object per account
    for market in markets:

        client = account.exchange.get_client(account)

        if market.type == 'swap':
            params = {'instrument_id': market.info['instrument_id']}
            response = client.swap_get_instrument_id_accounts(params)['info']

        elif market.type in ['future', 'futures']:
            params = {'underlying': market.info['underlying']}
            response = client.futures_get_accounts_underlying(params)

        # Select balance and equity of markets and instrument_id (ex:BTC-USD-SWAP)
        balance = float(response['total_avail_balance'])
        equity = float(response['equity'])
        currency = m.Currency.objects.get(code=response['currency'])

        # Create fund for each account
        create_fund(equity, balance, currency, response)

        if response['currency'] not in ['USD', 'USDT']:
            balance = balance*market.get_last_price()
            equity = equity*market.get_last_price()
            currency = market.quote

        total_balance.append(balance)
        total_equity.append(equity)
        total_response.append(response)

    # Create total funds
    create_fund(sum(total_equity), sum(total_balance), currency, total_response, total=True)


# Transfer fund between OKEx accounts
def transfer_funds(account):

    markets = account.markets.all()

    if len(set([m.settlement for m in markets])) > 1:
        log.warning('Cannot move fund between account of different currencies')
        return

    fund = account.fund.filter(total=True).latest('dt_create')

    for market in markets:

        allocation = s.Allocation.objects.filter(market__base=market.base, strategy=account.strategy).latest('dt')
        target = abs(allocation.weight) * fund.equity

        # Extract equity in the fund of this market
        for i in fund.response:
            if market.base.code in i['underlying']:  # 'underlying' = 'BTC-USDT'
                equity = float(i['equity'])
                instrument_id = i['underlying']
                max_withdraw = float(i['max_withdraw' if market.type == 'swap' else 'can_withdraw'])

        # print('\naccount', account.name)
        # print('market', market.symbol)
        # print('target', target)
        # print('equity', equity)

        delta = equity - target

        if delta == 0 :
            log.info('No need to transfer fund')
            return

        # Withdrawal
        if delta > fund.equity * 0.1:

            amount = round(min(delta, max_withdraw), 5)
            instrument_id = instrument_id
            to_instrument_id = 'BTC-USDT' if instrument_id == 'ETH-USDT' else 'ETH-USDT'

            if amount < 10:
                log.info('Amount to transfer is too small')
                return

            if market.type == 'swap':
                source, destination = 9, 9
            elif market.type in ['future', 'futures']:
                source, destination = 3, 3

            params = {
                'currency': market.settlement.code,
                'amount': amount,
                'from': source,
                'to': destination,
                'instrument_id': instrument_id,
                'to_instrument_id': to_instrument_id
            }

            client = account.exchange.get_client(account)
            try:
                client.account_post_transfer(params)
            except Exception as e:
                log.error('Transfer failed', account=account.name, e=e)
            else:
                log.info('Transfer success', account=account.name)



# Place order
# Rate Limit: 40 requests per 2 seconds (Swap)
# Rate Limit: 60 requests per 2 seconds (Future)
# def place(order):
#     client = order.account.exchange.get_client(order.account)
#
#     if order.account.limit_order:
#         order_type = 0
#     else:
#         order_type = 4  # market
#
# 1:open long
# 2:open short
# 3:close long
# 4:close short

# Set order type
# otype = 1 if order.type == 'open_long' else 2 if order.type == 'open_short' \
#     else 3 if order.type == 'close_long' else 4 if order.type == 'close_short' else None
#
# log.bind(account=order.account.name, market=order.market.symbol, type=order.type, size=order.size)
#
# params = dict(
#     size=order.size,
#     instrument_id=order.market.info['instrument_id'],
#     type=str(otype),
#     order_type=str(order_type)
# )
#
# if order.price:
#     params['price'] = str(order.price)
#
# try:
#     if order.market.type == 'swap':
#         response = client.swap_post_order(params)
#     elif order.market.type in ['future', 'futures']:
#         response = client.futures_post_order(params)
# except ccxt.ExchangeError as e:
#     log.error('ExchangeError when placing an order', params=params, e=e)
# except Exception as e:
#     log.error('Unknown exception when placing an order', params=params, e=e)
# else:
#
#     log.info('Placing order verification', params=params)
#
#     if float(response['error_code']) == 0:
#         log.info('Placing order verification OK', response=response)
#         order.orderId = response['order_id']
#         order.status = 'open'
#     else:
#         order.status = 'verification_failed'
#         log.error('Placing order verification failed', response=response)
#
#     order.response = response
#     order.save()


# Retrieve order details by order ID
# Rate limit: 10 requests per 2 seconds
# def refresh_order(order):
#     client = order.account.exchange.get_client(order.account)
#     params = dict(
#         instrument_id=order.market.info['instrument_id'],
#         order_id=order.orderId
#     )
#     try:
#         if order.market.type == 'swap':
#             response = client.swap_get_orders_instrument_id_order_id(params)
#         else:
#             response = client.futures_get_orders_instrument_id_order_id(params)
#     except:
#         log.exception('Unable to retrieve order details')
#     else:
#         order.fee = response['fee']
#         order.size = response['size']
#         order.filled = response['filled_qty']
#         order.price = response['price']
#         order.price_average = response['price_avg']
#         order.contract_val = response['contract_val']
#         order.leverage = response['leverage']
#         order.response = response
#
#         # Set order_type
#         order_type = float(response['order_type'])
#         if order_type == 0:
#             order.order_type = 'limit'
#         if order_type == 4:
#             order.order_type = 'market'
#         if order_type == 3:
#             order.order_type = 'immediate or cancel'
#         if order_type == 2:
#             order.order_type = 'fill or kill'
#         if order_type == 1:
#             order.order_type = 'post only'
#
#         # Set status
#         state = float(response['state'])
#         if state == -2:
#             order.status = 'failed'
#         if state == -1:
#             order.status = 'canceled'
#         if state == 2:
#             order.status = 'filled'
#         elif state == 1:
#             order.status = 'partially filled'
#         elif state == 0:
#             order.status = 'open'
#         elif state == 3:
#             order.status = 'submitting'
#         elif state == 4:
#             order.status = 'cancelling'
#         elif state == 6:
#             order.status = 'incomplete'
#         elif state == 7:
#             order.status = 'complete'
#
#         order.save()


# Cancel an unfilled order
# Rate limit: 40 requests per 2 seconds
# def cancel(order):
#     client = order.account.exchange.get_client(order.account)
#     params = dict(
#         instrument_id=order.market.info['instrument_id'],
#         order_id=order.orderId
#     )
#     try:
#         if order.market.type == 'swap':
#             response = client.swap_post_cancel_order_instrument_id_order_id(params)
#         elif order.market.type in ['future', 'futures']:
#             response = client.futures_post_cancel_order_instrument_id_order_id(params)
#     except:
#         log.exception('Unable to cancel order')
#     else:
#         if float(response['error_code']) == 0:
#             order.status = 'canceled'
#             order.response = response
#             order.save()
#             log.info('Order canceled')
#         else:
#             log.error('Error while canceling order', error_message=response['error_message'])

