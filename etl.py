import pandas as pd
import numpy as np
import pandasql as ps
import json
from exception_types import ExceptionType, EXCEPTION_TEMPLATES

# load in dataframs
trades = pd.read_csv('data/trades.csv')
counterparty_fills = pd.read_csv('data/counterparty_fills.csv')
symbols = pd.read_csv('data/symbols_reference.csv')
active_symbols = symbols[symbols['is_active']]['symbol'].tolist()
exceptions = []  # List of JSON objects

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
    global exceptions
    details = EXCEPTION_TEMPLATES[exception]
    # Generate details column based on exception type
    if exception == ExceptionType.MISSING_FIELD:
        details = details.format(field=missing_field)

    # Convert each row to JSON object matching schema
    for idx, row in df.iterrows():
        # Convert row to dict and replace NaN values with None (JSON null)
        raw_data = row.to_dict()
        raw_data = {k: (None if pd.isna(v) else v) for k, v in raw_data.items()}
        
        exception_record = {
            "record_id": str(row.get("trade_id", idx)),  # Use trade_id or index as record_id
            "source_file": source_file,
            "exception_type": exception.value,  # Get string value from enum
            "details": details,
            "raw_data": raw_data
        }
        exceptions.append(exception_record)

# Drop all cancelled trades
filter = trades['trade_status']=="CANCELLED"
cancelled = trades[filter]
add_exceptions(cancelled, ExceptionType.CANCELLED_TRADE, 'trades.csv')
trades = trades[~filter]

tables = {'trades.csv': trades, 'counterparty_fills.csv': counterparty_fills}

for table_name, table in tables.items():
    print(table_name + " start length: " + str(len(table)))
    print()
    # Put all trades with INVALID_SYM in exceptions
    invalid_symbols_filter = ~table["symbol"].isin(active_symbols)
    invalid_symbols = table[invalid_symbols_filter]
    add_exceptions(invalid_symbols, ExceptionType.INVALID_SYMBOL, table_name)
    table = table[~invalid_symbols_filter]

    # Put all trades with missing prices in exceptions
    missing_prices_filter = table["price"].isna()
    missing_prices = table[missing_prices_filter]
    add_exceptions(missing_prices, ExceptionType.MISSING_FIELD, table_name, 'price')
    table = table[~missing_prices_filter]

    # Put all trades with missing quantities in exceptions
    missing_quantities_filter = table["quantity"].isna()
    missing_quantities = table[missing_quantities_filter]
    add_exceptions(missing_quantities, ExceptionType.MISSING_FIELD, table_name, 'quantity')
    table = table[~missing_quantities_filter]

    # Put all trades with INVALID_TIMESTAMPS in exceptions
    table = table.copy()
    table["timestamp"] = normalize_timestamps(table["timestamp"])
    timestamp_filter = table["timestamp"].isna()
    invalid_timestamps = table[timestamp_filter]
    add_exceptions(invalid_timestamps, ExceptionType.INVALID_TIMESTAMP, table_name)
    table = table[~timestamp_filter]
    
    # Round all prices to 2 decimal points
    table["price"] = table["price"].round(2)

    # Save filtered table back
    tables[table_name] = table
    print(table_name + " end length: " + str(len(table)))
    print()

# Reassign filtered DataFrames
trades = tables['trades.csv']
counterparty_fills = tables['counterparty_fills.csv']

# Phase 2: Check counterparty confirmation and flag discrepancies
counterparty_query = """
SELECT trades.trade_id, trades.timestamp, trades.symbol, trades.quantity as trades_quantity, trades.price as trades_price, cp.quantity as cp_quantity, cp.price as cp_price
FROM trades
JOIN counterparty_fills cp ON trades.trade_id = cp.our_trade_id
WHERE trades.symbol = cp.symbol
"""
counterparty_confirmed = ps.sqldf(counterparty_query)

# Find records with no discrepancies (price diff <= $0.01 AND quantity matches)
# Using pandas operations for this check
no_discrepancy_mask = (
    (counterparty_confirmed['trades_price'] - counterparty_confirmed['cp_price']).abs() <= 0.01
) & (
    counterparty_confirmed['trades_quantity'] == counterparty_confirmed['cp_quantity']
)
no_discrepancy_trade_ids = set(counterparty_confirmed.loc[no_discrepancy_mask, 'trade_id'].tolist())

# Create set for fast lookup of confirmed trades
confirmed_trade_ids = set(counterparty_confirmed['trade_id'].tolist())

cleaned_trades = []
for idx, row in trades.iterrows():
    trade_id = row.get("trade_id", idx)
    
    # Check if counterparty confirmed (exists in counterparty_confirmed with matching symbol)
    counterparty_confirmed = trade_id in confirmed_trade_ids
    
    # Determine discrepancy flag
    if not counterparty_confirmed:
        # No counterparty confirmation → discrepancy
        discrepancy_flag = True
    else:
        # Counterparty confirmed → check if there's a discrepancy
        # If in no_discrepancies, no discrepancy; otherwise, there is one
        discrepancy_flag = trade_id not in no_discrepancy_trade_ids
    
    trade_record = {
        "trade_id": str(trade_id),
        "timestamp_utc": str(row.get("timestamp", "")),
        "symbol": str(row.get("symbol", "")),
        "quantity": float(row.get("quantity", 0)) if pd.notna(row.get("quantity")) else None,
        "price": float(row.get("price", 0)) if pd.notna(row.get("price")) else None,
        "buyer_id": str(row.get("buyer_id", "")),
        "seller_id": str(row.get("seller_id", "")),
        "counterparty_confirmed": counterparty_confirmed,
        "discrepancy_flag": discrepancy_flag
    }
    cleaned_trades.append(trade_record)

# Export cleaned_trades and exceptions to JSON files
with open('output/cleaned_trades.json', 'w') as f:
    json.dump(cleaned_trades, f, indent=2)

with open('output/exceptions_report.json', 'w') as f:
    json.dump(exceptions, f, indent=2)

print(f"Exported {len(cleaned_trades)} cleaned trades to cleaned_trades.json")
print(f"Exported {len(exceptions)} exceptions to exceptions_report.json")
