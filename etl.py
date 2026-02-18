import pandas as pd
import numpy as np
import pandasql as ps
import json
import os
from exception_types import ExceptionType, EXCEPTION_TEMPLATES

DATA_DIR = "data"
OUTPUT_DIR = "output"


def load_data():
    trades = pd.read_csv(os.path.join(DATA_DIR, "trades.csv")).drop_duplicates()
    counterparty_fills = pd.read_csv(os.path.join(DATA_DIR, "counterparty_fills.csv")).drop_duplicates()
    symbols = pd.read_csv(os.path.join(DATA_DIR, "symbols_reference.csv")).drop_duplicates()
    return trades, counterparty_fills, symbols


def get_active_symbols(symbols: pd.DataFrame) -> list[str]:
    return symbols.loc[symbols["is_active"], "symbol"].tolist()

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


def add_exceptions(exceptions: list[dict], df, exception: ExceptionType, source_file, missing_field: str | None = None):
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

def phase1_filter_and_collect_exceptions(
    *,
    trades: pd.DataFrame,
    counterparty_fills: pd.DataFrame,
    active_symbols: list[str],
    exceptions: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Drop all cancelled trades (trades table only)
    cancelled_filter = trades["trade_status"] == "CANCELLED"
    cancelled = trades.loc[cancelled_filter]
    add_exceptions(exceptions, cancelled, ExceptionType.CANCELLED_TRADE, "trades.csv")
    trades = trades.loc[~cancelled_filter]

    tables: dict[str, pd.DataFrame] = {"trades.csv": trades, "counterparty_fills.csv": counterparty_fills}

    for table_name, table in tables.items():
        print(table_name + " start length: " + str(len(table)))
        print()

        # Drop rows with invalid symbols
        invalid_symbols_filter = ~table["symbol"].isin(active_symbols)
        invalid_symbols = table.loc[invalid_symbols_filter]
        add_exceptions(exceptions, invalid_symbols, ExceptionType.INVALID_SYMBOL, table_name)
        table = table.loc[~invalid_symbols_filter]

        # Drop rows with missing price values
        missing_prices_filter = table["price"].isna()
        missing_prices = table.loc[missing_prices_filter]
        add_exceptions(exceptions, missing_prices, ExceptionType.MISSING_FIELD, table_name, "price")
        table = table.loc[~missing_prices_filter]
        
        # Drop rows with missing quantity values
        missing_quantities_filter = table["quantity"].isna()
        missing_quantities = table.loc[missing_quantities_filter]
        add_exceptions(exceptions, missing_quantities, ExceptionType.MISSING_FIELD, table_name, "quantity")
        table = table.loc[~missing_quantities_filter]

        # Drop rows with invalid timestamp values
        table = table.copy()
        table["timestamp"] = normalize_timestamps(table["timestamp"])
        invalid_timestamps_filter = table["timestamp"].isna()
        invalid_timestamps = table.loc[invalid_timestamps_filter]
        add_exceptions(exceptions, invalid_timestamps, ExceptionType.INVALID_TIMESTAMP, table_name)
        table = table.loc[~invalid_timestamps_filter]

        # Round all prices to 2 decimals
        table["price"] = table["price"].round(2)

        tables[table_name] = table
        print(table_name + " end length: " + str(len(table)))
        print()

    return tables["trades.csv"], tables["counterparty_fills.csv"]

def phase2_build_cleaned_trades(*, trades: pd.DataFrame, counterparty_fills: pd.DataFrame) -> list[dict]:
    counterparty_query = """
    SELECT trades.trade_id, trades.timestamp, trades.symbol,
           trades.quantity as trades_quantity, trades.price as trades_price,
           cp.quantity as cp_quantity, cp.price as cp_price
    FROM trades
    JOIN counterparty_fills cp ON trades.trade_id = cp.our_trade_id
    WHERE trades.symbol = cp.symbol
    """
    confirmed_df = ps.sqldf(counterparty_query)

    no_discrepancy_mask = (
        (confirmed_df["trades_price"] - confirmed_df["cp_price"]).abs() <= 0.01
    ) & (confirmed_df["trades_quantity"] == confirmed_df["cp_quantity"])

    confirmed_trade_ids = set(confirmed_df["trade_id"].tolist())
    no_discrepancy_trade_ids = set(confirmed_df.loc[no_discrepancy_mask, "trade_id"].tolist())

    cleaned_trades: list[dict] = []
    for idx, row in trades.iterrows():
        trade_id = row.get("trade_id", idx)

        counterparty_confirmed = trade_id in confirmed_trade_ids
        if not counterparty_confirmed:
            discrepancy_flag = True
        else:
            discrepancy_flag = trade_id not in no_discrepancy_trade_ids

        trade_record = {
            "trade_id": str(trade_id),
            "timestamp_utc": str(row.get("timestamp")),
            "symbol": str(row.get("symbol")),
            "quantity": float(row.get("quantity")),
            "price": float(row.get("price")),
            "buyer_id": str(row.get("buyer_id")),
            "seller_id": str(row.get("seller_id")),
            "counterparty_confirmed": counterparty_confirmed,
            "discrepancy_flag": discrepancy_flag,
        }
        cleaned_trades.append(trade_record)

    return cleaned_trades


def export_results(*, cleaned_trades: list[dict], exceptions: list[dict]) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    with open(os.path.join(OUTPUT_DIR, "cleaned_trades.json"), "w") as f:
        json.dump(cleaned_trades, f, indent=2)

    with open(os.path.join(OUTPUT_DIR, "exceptions_report.json"), "w") as f:
        json.dump(exceptions, f, indent=2)

    print(f"Exported {len(cleaned_trades)} cleaned trades to cleaned_trades.json")
    print(f"Exported {len(exceptions)} exceptions to exceptions_report.json")


def main() -> None:
    trades, counterparty_fills, symbols = load_data()
    active_symbols = get_active_symbols(symbols)
    exceptions: list[dict] = []

    trades, counterparty_fills = phase1_filter_and_collect_exceptions(
        trades=trades,
        counterparty_fills=counterparty_fills,
        active_symbols=active_symbols,
        exceptions=exceptions,
    )

    cleaned_trades = phase2_build_cleaned_trades(trades=trades, counterparty_fills=counterparty_fills)
    export_results(cleaned_trades=cleaned_trades, exceptions=exceptions)


if __name__ == "__main__":
    main()
