from datetime import timedelta
from django.utils import timezone
from marketsdata.error import *
import structlog
from datetime import datetime
from pprint import pprint

log = structlog.get_logger(__name__)


def get_volume_usd_from_ohlcv(market, vo, cl):
    #
    # Convert trading volumes in OHLCV to quote currency
    #
    exid = market.exchange.exid

    if exid == 'binance':
        if market.type == 'spot':
            # Binance returns volume of base for spot
            vo = vo * cl

        elif market.type == 'derivative':
            if market.margined.code == 'USDT':
                # Binance returns volume of base for USDT-margined swap and futures
                # Note that multiplying by the close price doesn't reflect the true
                # trading volumes in USDT
                vo = vo * cl
            else:
                # Binance returns volume of quote/10 for COIN-margined swap and futures
                vo = vo * 10

    elif exid == 'okex':
        if market.type == 'spot':
            # OKEx returns volume of base for spot
            vo = vo * cl
        elif market.type == 'derivative':
            if market.margined.code == 'USDT':
                # OKEx returns volume of base for USDT-margined swap and futures
                # Note that multiplying by the close price doesn't reflect the true
                # trading volumes in USDT
                vo = vo * cl
            else:
                # OKEx returns volume of base for COIN-margined swap and futures
                # Note that multiplying by the close price doesn't reflect the true
                # trading volumes in USD
                vo = vo * cl

    elif exid == 'huobipro':
        if market.type == 'spot':
            # Huobipro returns volume of base for spot
            vo = vo * cl

    elif exid == 'bybit':
        if market.type == 'derivative':
            if market.margined.code == 'USDT':
                # Bybit returns volume of quote for USDT-Margined perp
                vo = vo
            else:
                # Bybit returns volume of base for COIN-Margined perp
                vo = vo * cl

    elif exid == 'ftx':
        if market.type == 'derivative':
            # FTX returns volume of quote for perp and futures
            vo = vo

    else:
        raise Exception('There is no volume conversion rules for exchange {0}'.format(exid))

    return vo


def get_volume_usd_from_ticker(market, response):
    #
    # Extract rolling 24h trading volume in USD (fetch_tickers())
    #
    exid = market.exchange.exid

    # Select volume 24h
    if exid == 'binance':

        if market.type_ccxt in ['spot', 'future']:
            vo = float(response['quoteVolume'])

        elif market.type_ccxt == 'delivery':
            # Quote volume not reported by the COIN-margined api. baseVolume is string
            vo = float(response['info']['baseVolume']) * response['last']

        else:
            raise Exception('Unable to extract 24h volume from fetch_tickers() for type {0}'.format(market.type))

    elif exid == 'bybit':
        if market.type == 'derivative':
            if market.derivative == 'perpetual':
                if market.margined.code == 'USDT':
                    vo = float(response['info']['turnover_24h'])
                else:
                    vo = float(response['info']['volume_24h'])

    elif exid == 'okex':

        if market.type_ccxt == 'spot':
            vo = response['info']['quote_volume_24h']
        elif market.type_ccxt == 'swap':
            if market.margined.code == 'USDT':
                # volume_24h is the volume of contract priced in ETH
                vo = float(response['info']['volume_24h']) * market.contract_value * response['last']
            else:
                # volume_24h is the volume of contract priced in USD
                vo = float(response['info']['volume_24h']) * market.contract_value
        elif market.type_ccxt == 'futures':
            vo = float(response['info']['volume_token_24h']) * response['last']
        else:
            raise Exception('Unable to extract 24h volume from fetch_tickers() for type {0}'.format(market.type_ccxt))

    elif exid == 'ftx':
        vo = response['info']['volumeUsd24h']

    elif exid == 'huobipro':
        if not market.type_ccxt:
            vo = float(response['info']['vol'])
        else:
            raise Exception('Unable to extract 24h volume from fetch_tickers() for type {0}'.format(market.type_ccxt))
    else:
        raise Exception('Unable to extract 24h volume from fetch_tickers() for exchange {0}'.format(exid))

    return vo


def get_derivative_type(exid, values):
    #
    # Determine the type derivative (update_markets())
    #
    if exid == 'okex':
        if values['type'] == 'swap':
            return 'perpetual'
        elif values['type'] == 'futures':
            return 'future'
        else:
            pprint(values)
            raise Exception('Unknown derivative type at exchange {0}'.format(exid))

    elif exid == 'binance':
        if 'contractType' in values['info']:
            if values['info']['contractType'] == 'PERPETUAL':
                return 'perpetual'
            elif values['info']['contractType'] in ['CURRENT_QUARTER', 'NEXT_QUARTER DELIVERING', 'NEXT_QUARTER']:
                return 'future'
        else:
            pprint(values)
            raise Exception('Unknown derivative type at exchange {0}'.format(exid))

    elif exid == 'bybit':
        if values['type'] == 'future' and values['future']:
            return 'perpetual'
        else:
            pprint(values)
            raise Exception('Unknown derivative type at exchange {0}'.format(exid))

    elif exid == 'ftx':
        if values['type'] == 'future' and '-PERP' in values['symbol']:
            return 'perpetual'
        else:
            return 'future'

    else:
        raise Exception('Please set rules for derivative type exchange {0}'.format(exid))


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
        else:
            pprint(values)
            raise Exception('Unknown derivative at exchange {0}'.format(exid))

    elif exid == 'binance':
        return Currency.objects.get(code=values['info']['marginAsset'])

    elif exid == 'bybit':
        if values['future'] and values['inverse']:
            return Currency.objects.get(code=values['base'])
        elif values['future'] and not values['inverse']:
            return Currency.objects.get(code=values['quote'])
        else:
            pprint(values)
            raise Exception('Unknown derivative at exchange {0}'.format(exid))

    elif exid == 'ftx':
        if values['future']:
            return Currency.objects.get(code=values['quote'])
        else:
            pprint(values)
            raise Exception('Unknown derivative at exchange {0}'.format(exid))

    else:
        raise Exception('Please set rules for magined for exchange {0}'.format(exid))


def get_derivative_contract_value(exid, values):
    #
    # Determine the contract value of a derivative (update_markets())
    #
    if exid == 'okex':
        if values['type'] in ['swap', 'futures']:
            return values['info']['contract_val']

    elif exid == 'binance':
        return 1

    elif exid == 'bybit':
        return 1

    elif exid == 'ftx':
        return 1

    else:
        raise Exception('Please set rules for contract value for exchange {0}'.format(exid))


def get_derivative_contract_value_currency(exid, values):
    #
    # Determine the currency of the contract value for a derivative (update_markets())
    #
    from marketsdata.models import Currency
    if exid == 'okex':
        if values['type'] in ['swap', 'futures']:
            return Currency.objects.get(code=values['info']['contract_val_currency'])

    elif exid == 'binance':
        return Currency.objects.get(code=values['base'])

    elif exid == 'bybit':
        if values['future'] and values['inverse']:
            # COIN-Margined perp
            return Currency.objects.get(code=values['quote'])
        elif values['future'] and not values['inverse']:
            # USDT-margined perp
            return Currency.objects.get(code=values['base'])

    elif exid == 'ftx':
        return Currency.objects.get(code=values['base'])

    else:
        raise Exception('Please set rules for contract currency for exchange {0}'.format(exid))


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
        return timezone.make_aware(datetime.fromtimestamp(values['info']['deliveryDate'] / 1000))

    elif exid == 'bybit':
        return None

    elif exid == 'ftx':
        return None

    else:
        raise Exception('Please set rules for contract delivery date for exchange {0}'.format(exid))


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
        return timezone.make_aware(datetime.fromtimestamp(values['info']['onboardDate'] / 1000))

    elif exid == 'bybit':
        return None

    elif exid == 'ftx':
        return None

    else:
        raise Exception('Please set rules for contract listing date for exchange {0}'.format(exid))