from enum import Enum

class ExceptionType(str, Enum):
    CANCELLED_TRADE = "cancelled_trade"
    INVALID_TIMESTAMP = "invalid_timestamp"
    INVALID_SYMBOL = "invalid_symbol"
    MISSING_FIELD = "missing_field"

EXCEPTION_TEMPLATES = {
    ExceptionType.CANCELLED_TRADE: "Trade {trade_id} is cancelled.",
    ExceptionType.INVALID_TIMESTAMP: "Trade {trade_id} has invalid or missing timestamp.",
    ExceptionType.INVALID_SYMBOL: "Symbol '{symbol}' is not in reference.",
    ExceptionType.MISSING_FIELD: "This trade is missing required field '{field}'.",
}