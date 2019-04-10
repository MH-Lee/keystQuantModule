import requests
import time
import pandas as pd
from module.cache import RedisClient

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
cache_factor_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=CACHE_FACTOR_DATA'

r = requests.get(DATE)
time.sleep(60)
print("DATE")
r = requests.get(UPDATE)
time.sleep(70)
print("Update")
r = requests.get(TICKER)
time.sleep(60)
print("TICKER")
r = requests.get(STOCKINFO)
# time.sleep(60)
# print("today STOCKINFO")
r = requests.get(INDEX)
time.sleep(60)
print("INDEX")
r = requests.get(ETF)
time.sleep(60)
print("ETF")
r = requests.get(OHLCV)
time.sleep(60)
print("OHLCV")
r = requests.get(MARKETCAPITAL)
time.sleep(60)
print("MARKETCAPITAL")
r = requests.get(BUYSELL)
time.sleep(60)
print("BUYSELL")
r = requests.get(FACTOR)
time.sleep(120)

print("Data Crawler Quit")
print("Make Cache Data")

r1 = requests.get(cache_ticker_data)
r2 = requests.get(get_ticker_data)
r3 = requests.get(cache_index_data)
r4 = requests.get(cache_ohlcv_data)
r5 = requests.get(cache_full_ohlcv_data)
r6 = requests.get(cache_buysell_data)
r7 = requests.get(cache_factor_data)


cache_etf_ticker = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=cache_etf_tickers'
cache_mktcap_ticker = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=cache_mktcap_tickers'
cache_frg_ticker = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=cache_frg_gte_tickers'

cache_mktcap_data = 'http://45.76.202.71:3000/api/v1/stocks/task/?type=cache_mktcap_data'


etf_ticker = requests.get(cache_etf_ticker)
mktcap_ticker = requests.get(cache_mktcap_ticker)
frg_ticker = requests.get(cache_frg_ticker)

r7 = requests.get(cache_mktcap_data)
print("all process is completed!")

cache_buysell_mk = 'http://207.148.99.218:3000/mined/api/v1/task/?type=cache_buysell_mkt'
r = requests.get(cache_buysell_mk)


KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'
ETF_OHLCV = 'ETF_OHLCV'
MKT_DF_KEY = "MKTCAP_DF"

redis = RedisClient()
kp_ohlcv = redis.get_df(KOSPI_OHLCV)
samsung_ohlcv = redis.get_df('005930_FULL_OHLCV')
samsung_ohlcv.shape
samsung_ohlcv = redis.get_df('000020_FULL_OHLCV')
samsung_ohlcv.shape

kd_ohlcv = redis.get_df(KOSDAQ_OHLCV)
etf_ohlcv = redis.get_df(ETF_OHLCV)

kp_ohlcv.tail()

etf_ohlcv.tail()


mkt_cap_df = redis.get_df(MKT_DF_KEY)

mkt_cap_df.tail()
