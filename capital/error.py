# define Python user-defined exceptions
class ApplicationError(Exception):
    pass


class MethodUnsupported(ApplicationError):
    pass


class MarketsdataError(ApplicationError):
    pass


class StrategyError(ApplicationError):
    pass


class TradingError(ApplicationError):
    pass