import structlog

log = structlog.get_logger(__name__)


# define Python user-defined exceptions
class MarketsDataError(Exception):
    """Base class for other exceptions"""
    pass


class InactiveExchange(MarketsDataError):
    """Exchange is inactive"""
    pass


class InactiveMarket(MarketsDataError):
    """Market is inactive"""
    pass


class MethodNotSupported(MarketsDataError):
    """Method isn't supported"""
    pass


class TooMuchTimeElapsed(MarketsDataError):
    """Method fetchTickers() is executed >120s after minute 00:00"""
    pass


class ConfigurationError(MarketsDataError):
    """Settings error"""
    pass


class FutureSpecsError(MarketsDataError):
    """Problem with future specs"""
    pass


