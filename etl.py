import pandas as pd
import pandasql as ps

from exception_types import ExceptionType, EXCEPTION_TEMPLATES

trades = pd.read_csv('trades.csv')
cp = pd.read_csv('counterparty_fills.csv')
symbols = pd.read_csv('symbols_reference.csv')
active_symbols = symbols[symbols['is_active']]['symbol'].tolist()
exceptions = pd.DataFrame()

# First, move all cancelled trades to exceptions table

def add_exceptions(df, exception: ExceptionType, source_file, missing_field: None):
    template = EXCEPTION_TEMPLATES[exception]

    match template:
        case ExceptionType.CANCELLED_TRADE:
            df["details"] = df["trade_id"].apply(lambda v: template.format(trade_id=v))
        case ExceptionType.INVALID_TIMESTAMP:
            df["details"] = df["trade_id"].apply(lambda v: template.format(trade_id=v))
        case ExceptionType.INVALID_SYMBOL:
            df["details"] = df["symbol"].apply(lambda v: template.format(symbol=v))
        case ExceptionType.MISSING_FIELD:
            df["details"] = template.format(missing_field=missing_field)

    # then, jsonify each record and add to exceptions
    #TODO

# Drop all cancelled trades
filter = trades['trade_status']=="CANCELLED"
cancelled = trades[filter]
add_exceptions(cancelled, ExceptionType.CANCELLED_TRADE, 'trades.csv')
trades = trades[~filter]



# in_clause = "(" + ", ".join(f"'{s}'" for s in active_symbols) + ")"
# for table_name in ['trades', 'counterparty_fills']:
#     # Put all trades with INVALID_SYM in exceptions
#     query = f"""
#     SELECT * FROM {table_name} WHERE symbol NOT IN {in_clause}
#     """
#     result = ps.sqldf(query)

    



# Phase 2: