import requests
import time

DATE = 'http://45.77.31.8:3000/task/DATE'
TICKER = 'http://45.77.31.8:3000/task/TICKER'
STOCKINFO = 'http://45.77.31.8:3000/task/STOCKINFO'
INDEX = 'http://45.77.31.8:3000/task/INDEX'
ETF =  'http://45.77.31.8:3000/task/ETF'
OHLCV = 'http://45.77.31.8:3000/task/OHLCV'
MARKETCAPITAL = 'http://45.77.31.8:3000/task/MARKETCAPITAL'
BUYSELL = 'http://45.77.31.8:3000/task/BUYSELL'
FACTOR = 'http://45.77.31.8:3000/task/FACTOR'
UPDATE = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=SET_UPDATE_TASKS'

cache_ticker_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_TICKER_DATA'
get_ticker_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=_GET_TICKERS'
cache_index_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_INDEX_DATA'
cache_ohlcv_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_OHLCV_DATA'
cache_full_ohlcv_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_FULL_OHLCV_DATA'
cache_buysell_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_BUYSELL_DATA'

r = requests.get(DATE)
time.sleep(120)
r = requests.get(UPDATE)
time.sleep(120)
r = requests.get(TICKER)
time.sleep(120)
r = requests.get(STOCKINFO)
time.sleep(120)
r = requests.get(INDEX)
time.sleep(120)
r = requests.get(ETF)
time.sleep(120)
r = requests.get(OHLCV)
time.sleep(120)
r = requests.get(MARKETCAPITAL)
time.sleep(120)
r = requests.get(BUYSELL)
time.sleep(120)
r = requests.get(FACTOR)
time.sleep(120)

r1 = requests.get(cache_ticker_data)
r2 = requests.get(get_ticker_data)
r3 = requests.get(cache_index_data)
r4 = requests.get(cache_ohlcv_data)
r5 = requests.get(cache_full_ohlcv_data)
r6 = requests.get(cache_buysell_data)

print("all process is completed!")
