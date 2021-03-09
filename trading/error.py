# define Python user-defined exceptions
class TradingError(Exception):
    """Base class for other exceptions"""
    pass


class InvalidCredentials(TradingError):
    """Account credentials are invalid"""
    pass


class InactiveAccount(TradingError):
    """Account trading is deactivated"""
    pass


class MarketSelectionError(TradingError):
    """Account exchange is incompatible with the strategy"""
    pass


class SettingError(TradingError):
    """Account exchange is incompatible with the strategy"""
    pass


class StrategyNotUpdated(TradingError):
    """Strategy isn't updated"""
    pass


class WaitUpdateTime(TradingError):
    """Not the time for an updated"""
    pass
