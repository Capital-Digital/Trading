from datetime import timedelta
from django.utils import timezone
from marketsdata.error import *
import structlog
from datetime import datetime
from pprint import pprint
import pandas as pd
import pytz

log = structlog.get_logger(__name__)


def get_volume_quote_from_ohlcv(market, vo, cl):
    #
    # Convert trading volumes in OHLCV to quote currency
    #
    exid = market.exchange.exid

    if exid == 'binance':
        if market.type == 'spot':
            # Binance returns volume of base for spot
            return vo * cl

        elif market.type == 'derivative':
            if market.margined.code == 'USDT':
                # Binance returns volume of base for USDT-margined swap and futures
                # Note that multiplying by the close price doesn't reflect the true
                # trading volumes in USDT
                return vo * cl
            else:
                # Binance returns volume of quote/10 for COIN-margined swap and futures
                return vo * 10

    elif exid == 'okex':
        if market.type == 'spot':
            # OKEx returns volume of base for spot
            return vo * cl
        elif market.type == 'derivative':
            if market.margined.code == 'USDT':
                # OKEx returns volume of base for USDT-margined swap and futures
                # Note that multiplying by the close price doesn't reflect the true
                # trading volumes in USDT
                return vo * cl
            else:
                # OKEx returns volume of base for COIN-margined swap and futures
                # Note that multiplying by the close price doesn't reflect the true
                # trading volumes in USD
                return vo * cl

    elif exid == 'huobipro':
        if market.type == 'spot':
            # Huobipro returns volume of base for spot
            return vo * cl

    elif exid == 'bybit':
        if market.type == 'derivative':
            if market.margined.code == 'USDT':
                # Bybit returns volume of quote for USDT-Margined perp
                return vo
            else:
                # Bybit returns volume of base for COIN-Margined perp
                return vo * cl

    elif exid == 'ftx':
        if market.type in ['derivative', 'spot']:
            # FTX returns volume of quote for spot, perp and futures
            return vo

    elif exid == 'bitfinex2':
        if market.type in ['spot', 'derivative']:
            return vo * cl

    elif exid == 'bitmex':
        return vo

    return False


def get_volume_quote_from_ticker(market, response):
    #
    # Extract rolling 24h trading volume in USD (fetch_tickers())
    #
    exid = market.exchange.exid

    # Select volume 24h
    if exid == 'binance':
        if market.wallet in ['spot', 'future']:
            return float(response['quoteVolume'])
        elif market.wallet == 'delivery':
            if 'baseVolume' in response['info']:
                # Quote volume not reported by the COIN-margined api. baseVolume is string
                return float(response['info']['baseVolume']) * response['last']

    elif exid == 'bybit':
        if market.type == 'derivative':
            if market.contract_type in ['perpetual', 'future']:
                if market.margined.code == 'USDT':
                    return float(response['info']['turnover_24h'])
                else:
                    return float(response['info']['volume_24h'])

    elif exid == 'okex':
        if market.wallet == 'spot':
            return float(response['info']['quote_volume_24h'])
        elif market.wallet == 'swap':
            if market.margined.code == 'USDT':
                # volume_24h is the volume of contract priced in ETH
                return float(response['info']['volume_24h']) * market.contract_value * response['last']
            else:
                # volume_24h is the volume of contract priced in USD
                return float(response['info']['volume_24h']) * market.contract_value
        elif market.wallet == 'futures':
            return float(response['info']['volume_token_24h']) * response['last']

    elif exid == 'ftx':
        return float(response['info']['volumeUsd24h'])

    elif exid == 'huobipro':
        if not market.wallet:
            return float(response['info']['vol'])

    elif exid == 'bitmex':
        return float(response['quoteVolume'])

    elif exid == 'bitfinex2':
        return float(response['baseVolume']) * response['last']

    pprint(response)
    log.error('No rule 24h volume for {1} at {0}'.format(exid, market.symbol, derivative=market.contract_type))
    return False


def get_derivative_type(exid, values):
    #
    # Determine the type derivative (update_markets())
    #
    if exid == 'okex':
        if values['type'] == 'swap':
            return 'perpetual'
        elif values['type'] == 'futures':
            return 'future'

    elif exid == 'binance':
        if 'contractType' in values['info']:
            if values['info']['contractType'] == 'PERPETUAL':
                return 'perpetual'
            elif values['info']['contractType'] in ['CURRENT_QUARTER',
                                                    'CURRENT_QUARTER DELIVERING',
                                                    'NEXT_QUARTER DELIVERING',
                                                    'NEXT_QUARTER']:
                return 'future'
            # Return None so market is not created
            elif not values['info']['contractType'] and values['info']['status'] == 'PENDING_TRADING':
                return None

    elif exid == 'bybit':

        if values['type'] == 'swap':
            return 'perpetual'

        if values['type'] in ['future', 'futures']:
            return 'future'

        if values['type'] == 'spot':
            return 'spot'

    elif exid == 'ftx':
        if values['type'] == 'future' and '-PERP' in values['symbol']:
            return 'perpetual'
        else:
            return 'future'

    elif exid == 'bitmex':
        if values['type'] == 'swap':
            return 'perpetual'
        elif values['type'] == 'future':
            return 'future'

    elif exid == 'bitfinex2':
        if values['type'] == 'futures' and values['info']['expiration'] == 'NA':
            return 'perpetual'

    pprint(values)
    log.error('No rule for derivative type for {1} at {0}'.format(exid, values['symbol']))
    return False


def get_derivative_margined(exid, values):
    #
    # Determine the margined currency of derivative (update_markets())
    #
    from marketsdata.models import Currency

    if exid == 'okex':
        if values['type'] == 'swap':
            return Currency.objects.get(code=values['info']['coin'])
        elif values['type'] == 'futures':
            return Currency.objects.get(code=values['info']['settlement_currency'])

    elif exid == 'binance':
        return Currency.objects.get(code=values['info']['marginAsset'])

    elif exid == 'bybit':
        if values['inverse']:
            return Currency.objects.get(code=values['base'])
        elif not values['inverse']:
            return Currency.objects.get(code=values['quote'])

    elif exid == 'ftx':
        if values['future']:
            return Currency.objects.get(code=values['quote'])

    elif exid == 'bitmex':
        if values['info']['settlCurrency'] == 'XBt':
            return Currency.objects.get(code='BTC')

    elif exid == 'bitfinex2':
        if values['type'] == 'futures' and values['info']['expiration'] == 'NA':
            return Currency.objects.get(code='USDT')

    pprint(values)
    log.error('No rule margined currency for {1} at {0}'.format(exid, values['symbol']))
    return False


def get_derivative_contract_value(exid, values):
    #
    # Determine the contract value of a derivative (update_markets())
    #
    if exid == 'okex':
        if values['type'] in ['swap', 'futures']:
            return values['info']['contract_val']

    elif exid == 'binance':
        # COIN-margined see https://www.binance.com/en/futures/trading-rules/quarterly
        if values['info']['marginAsset'] == values['base']:
            return values['info']['contractSize']  # Select USD value of 1 contract

        # USDT-margined see https://www.binance.com/en/futures/trading-rules
        elif values['info']['marginAsset'] == values['quote']:
            return

    elif exid == 'bybit':
        return

    elif exid == 'ftx':
        return

    elif exid == 'bitmex':

        if values['type'] in ['swap', 'future']:

            if values['info']['isInverse']:
                # 1 contract = 1 USD relationship is valid for inverse contracts
                return

            elif values['info']['isQuanto']:
                # Contract value in XBT = multiplier (in satoschi) * Quanto contract price
                multiplier = float(values['info']['multiplier']) / 100000000
                return multiplier * float(values['info']['lastPrice'])

            elif not values['info']['isQuanto'] and not values['info']['isInverse']:
                # 1 contract = 1 base relationship
                return

    elif exid == 'bitfinex2':
        return

    pprint(values)
    log.error('No rule contract value for {1} at {0}'.format(exid, values['symbol']))
    return False


def get_derivative_contract_value_currency(exid, wallet, values):
    #
    # Determine the currency of the contract value for a derivative (update_markets())
    #

    from marketsdata.models import Currency

    if exid == 'okex':
        if values['type'] in ['swap', 'futures']:
            return Currency.objects.get(code=values['info']['contract_val_currency'])

    elif exid == 'binance':
        # base asset are used as contract value currency for USDT-margined and COIN-margined
        if wallet == 'future':
            return
        elif wallet == 'delivery':
            return Currency.objects.get(code=values['quote'])

    elif exid == 'bybit':

        if values['inverse']:
            # COIN-Margined perp
            return Currency.objects.get(code=values['quote'])

        elif not values['inverse']:
            # USDT-margined perp
            return

    elif exid == 'ftx':
        return

    elif exid == 'bitmex':

        if values['info']['isInverse']:
            return Currency.objects.get(code='USD')

        elif values['info']['isQuanto']:
            # Contract value is calculated in XBt
            return Currency.objects.get(code='BTC')

        elif not values['info']['isQuanto'] and not values['info']['isInverse']:
            # Contract value is calculated in base
            return Currency.objects.get(code=values['base'])

    elif exid == 'bitfinex2':
        if values['type'] == 'futures' and values['info']['expiration'] == 'NA':
            return Currency.objects.get(code=values['base'])

    pprint(values)
    log.error('No rule for contract value currency for {1} at {0}'.format(exid, values['symbol']))
    return False


def get_derivative_delivery_date(exid, values):
    #
    # Determine derivative delivery date (update_markets())
    #
    if exid == 'okex':
        if values['type'] == 'futures':
            return timezone.make_aware(datetime.strptime(values['info']['delivery'], '%Y-%m-%d'))
        if values['type'] == 'swap':
            return timezone.make_aware(datetime.strptime(values['info']['delivery'], '%Y-%m-%dT%H:%M:%S.000Z'))

    elif exid == 'binance':
        return timezone.make_aware(datetime.fromtimestamp(float(values['info']['deliveryDate']) / 1000))

    elif exid == 'bybit':
        return None

    elif exid == 'ftx':
        return None

    elif exid == 'bitmex':
        if values['type'] == 'future':
            # '2021-03-26T12:00:00.000Z'
            return timezone.make_aware(datetime.strptime(values['info']['expiry'], '%Y-%m-%dT%H:%M:%S.000Z'))
        else:
            return None

    elif exid == 'bitfinex2':
        if values['type'] == 'futures' and values['info']['expiration'] == 'NA':
            return None

    pprint(values)
    log.error('No rule contract delivery date for {1} at {0}'.format(exid, values['symbol']))
    return False


def get_derivative_listing_date(exid, values):
    #
    # Determine the listing date of a derivative (update_markets())
    #
    if exid == 'okex':
        if values['type'] == 'futures':
            return timezone.make_aware(datetime.strptime(values['info']['listing'], '%Y-%m-%d'))
        if values['type'] == 'swap':
            return timezone.make_aware(datetime.strptime(values['info']['listing'], '%Y-%m-%dT%H:%M:%S.000Z'))

    elif exid == 'binance':
        return timezone.make_aware(datetime.fromtimestamp(float(values['info']['onboardDate']) / 1000))

    elif exid == 'bybit':
        return None

    elif exid == 'ftx':
        return None

    elif exid == 'bitmex':
        if values['type'] in ['future', 'swap']:
            # '2021-03-26T12:00:00.000Z'
            return timezone.make_aware(datetime.strptime(values['info']['listing'], '%Y-%m-%dT%H:%M:%S.000Z'))

    elif exid == 'bitfinex2':
        if values['type'] == 'futures' and values['info']['expiration'] == 'NA':
            return None

    pprint(values)
    log.error('No rule contract listing date for {1} at {0}'.format(exid, values['symbol']))
    return False


