import pandas as pd
import pandasql as ps

trades = pd.read_csv('trades.csv')
cp = pd.read_csv('counterparty_fills.csv')
symbols = pd.read_csv('symbols_reference.csv')

# Phase 1:
exceptions = pd.DataFrame()
for table_name in ['trades', 'counterparty_fills']:
    query = f"""
    SELECT * FROM {table_name} WHERE trade_status = 'CANCELLED'
    """

    result = ps.sqldf(query)




# Phase 2: