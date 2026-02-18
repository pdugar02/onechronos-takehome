import pandas as pd
import pandasql as ps

trades = pd.read_csv('trades.csv')
cp = pd.read_csv('counterparty_fills.csv')
symbols = pd.read_csv('symbols_reference.csv')

query = """
SELECT trades.trade_id, trades.timestamp, trades.symbol, trades.quantity, trades.price, cp.timestamp, cp.quantity, cp.price
FROM trades
JOIN cp ON trades.trade_id = cp.our_trade_id
WHERE trades.symbol = cp.symbol
"""

result = ps.sqldf(query)

print(result)