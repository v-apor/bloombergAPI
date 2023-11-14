import blpapi
from xbbg import blp
import pandas as pd

DATA_DIR = './data/'

tickers = ['NVDA US Equity', 'AAPL US Equity']
fields = ['High', 'Low', 'Last_Price']
start_date = '2023-09-01'
end_date = '2023-09-20'

hist_tick_data = blp.bdh(tickers=tickers, flds=fields, start_date=start_date, end_date=end_date)

filename = f'tick_data_{start_date}_{end_date}.csv'
hist_tick_data.to_csv(DATA_DIR + filename)