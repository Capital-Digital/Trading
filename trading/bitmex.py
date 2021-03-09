from .models import Position
import marketsdata.models as marketsdata
from django.utils import timezone
import warnings, json
from decimal import Decimal
import structlog
from swagger_spec_validator.common import SwaggerValidationWarning
from pprint import pprint

warnings.simplefilter("ignore", SwaggerValidationWarning)

log = structlog.get_logger(__name__)


def update(account, client=None):

    # This download open position from a bitmex account
    # and updates existing positions or creates a new object
    # when a new position is detected

    global pos, market
    if client is None:
        client = exchanges.get_bitmex_client(account)
    exchange = account.exchange

    log.bind(exchange=exchange.name_ccxt)
    log.info('Pull open positions')

    try:
        pos = client.Position.Position_get(filter=json.dumps({'isOpen': True})).result()[0]
    except:
        log.exception('Pull open positions ERROR')
    else:
        log.bind(position_nb=len(pos))
        log.info('Pull open positions OK')

    if pos:
        for i, p in enumerate(pos):

            # Select Market ForeignKey
            position = p
            # pprint(p)

            base = position['underlying']
            quote = position['quoteCurrency']
            log.bind(base=base,
                     quote=quote
                     )
            try:
                market = marketsdata.Market.objects.get(exchange=exchange,
                                                        symbol=base + quote
                                                        )

            except marketsdata.Market.DoesNotExist:
                log.warning('Unknown position')
                market = None

            finally:
                log.info('Update or create position')
                try:

                    obj, created = Position.objects.update_or_create(
                        market=market,
                        account=account,
                        defaults={
                            'id_account': position['account'],
                            'symbol': position['symbol'],

                            'average_entry_price': round(position['avgEntryPrice'], 2),
                            'liquidation_price': position['liquidationPrice'],
                            'commission': position['commission'],
                            'market_price': round(position['markPrice'], 4),

                            'cross_margin': position['crossMargin'],
                            'is_open': position['isOpen'],

                            'current_commission': position['currentComm'],
                            'maintenance_margin': position['maintMargin'],
                            'position_maintenance': position['posMaint'],
                            'position_margin': position['posMargin'],
                            'unrealised_pnl': position['unrealisedPnl'],
                            'realised_pnl': position['realisedPnl'],
                            'quantity': position['currentQty'],
                            'market_value': position['markValue']
                        },
                    )
                except:
                    log.exception('Update or create position FAILED')
                    log.unbind('base', 'quote')
                else:
                    log.info('Update or create position OK', created=created)
                    log.unbind('base', 'quote')
    else:
        log.info('No position open')

    return client
