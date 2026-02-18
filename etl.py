import pandas as pd
import numpy as np
from exception_types import ExceptionType, EXCEPTION_TEMPLATES

trades = pd.read_csv('data/trades.csv')
counterparty_fills = pd.read_csv('data/counterparty_fills.csv')
symbols = pd.read_csv('data/symbols_reference.csv')
active_symbols = symbols[symbols['is_active']]['symbol'].tolist()
exceptions = pd.DataFrame()

def normalize_timestamps(series):
    """
    Normalize timestamp strings to ISO format:
    - ISO format strings: kept unchanged
    - US format strings: converted to ISO
    - Other formats: set to NaN
    """
    result = pd.Series(index=series.index, dtype='object')
    
    # Pass 1: Try ISO format (with or without Z, with or without milliseconds)
    iso_mask = pd.to_datetime(series, format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce').notna()
    
    # Keep ISO strings as-is
    result[iso_mask] = series[iso_mask]
    
    # Pass 2: Try US format on remaining values
    remaining_mask = ~iso_mask
    us_parsed = pd.to_datetime(series[remaining_mask], format='%m/%d/%Y %H:%M:%S', errors='coerce')
    
    # Convert US format to ISO
    us_converted = us_parsed.dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')
    result[remaining_mask] = us_converted
    
    # Pass 3: Set any remaining NaT to NaN
    result[result.isna()] = np.nan
    
    return result


def add_exceptions(df, exception: ExceptionType, source_file, missing_field: str | None = None):
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

in_clause = "(" + ", ".join(f"'{s}'" for s in active_symbols) + ")"
for table in [trades, counterparty_fills]:
    print(str(table) + " start length: " + str(len(table)))
    print()
    # Put all trades with INVALID_SYM in exceptions
    invalid_symbols_filter = ~table["symbol"].isin(active_symbols)
    invalid_symbols = table[invalid_symbols_filter]
    # print(invalid_symbols)
    add_exceptions(invalid_symbols, ExceptionType.INVALID_SYMBOL, str(table) + '.csv')
    table = table[~invalid_symbols_filter]

    # Put all trades with missing prices in exceptions
    missing_prices_filter = table["price"].isna()
    missing_prices = table[missing_prices_filter]
    # print(missing_prices)
    add_exceptions(missing_prices, ExceptionType.MISSING_FIELD, str(table) + '.csv', 'price')
    table = table[~missing_prices_filter]

    # Put all trades with missing quantities in exceptions
    missing_quantities_filter = table["quantity"].isna()
    missing_quantities = table[missing_quantities_filter]
    # print(missing_quantities)
    add_exceptions(missing_quantities, ExceptionType.MISSING_FIELD, str(table) + '.csv', 'quantity')
    table = table[~missing_quantities_filter]

    # Put all trades with INVALID_TIMESTAMPS in exceptions
    table["timestamp"] = normalize_timestamps(table["timestamp"])
    timestamp_filter = table["timestamp"].isna()
    invalid_timestamps = table[timestamp_filter]
    # print(invalid_timestamps)
    add_exceptions(invalid_timestamps, ExceptionType.INVALID_TIMESTAMP, str(table) + '.csv')
    table = table[~timestamp_filter]
    print(str(table) + " end length: " + str(len(table)))
    print()

    



# Phase 2: