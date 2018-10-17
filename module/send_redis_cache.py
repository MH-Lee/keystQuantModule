import redis
from module.keystQuant import KeystQuant

keystq = KeystQuant()

cache_ip = '198.13.60.19'
cache_pw = 'da56038fa453c22d2c46e83179126e97d4d272d02ece83eb83a97357e842d065'

r = redis.StrictRedis(host=cache_ip, port=6379, password=cache_pw)

KOSPI_TICKERS = 'KOSPI_TICKERS'
KOSDAQ_TICKERS = 'KOSDAQ_TICKERS'

KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'

KOSPI_VOL = 'KOSPI_VOL'
KOSDAQ_VOL = 'KOSDAQ_VOL'


kp_tickers = [ticker.decode() for ticker in r.lrange(KOSPI_TICKERS, 0 ,-1)]
kd_tickers = [ticker.decode() for ticker in r.lrange(KOSDAQ_TICKERS, 0 ,-1)]

kp_tickers_list, kd_tickers_list, kp_tickers_dict, kd_tickers_dict = keystq.make_ticker_data(kp_tickers, kd_tickers)
print(len(kp_tickers_list), len(kd_tickers_list))

kp_ohlcv, kp_vol = keystq.make_ohlcv_df('kp', kp_tickers_list, kd_tickers_list)
kd_ohlcv, kd_vol = keystq.make_ohlcv_df('kd', kp_tickers_list, kd_tickers_list)

r.set(KOSPI_OHLCV, kp_ohlcv.to_msgpack(compress='zlib'))
r.set(KOSDAQ_OHLCV, kd_ohlcv.to_msgpack(compress='zlib'))
r.set(KOSPI_VOL, kp_vol.to_msgpack(compress='zlib'))
r.set(KOSDAQ_VOL, kd_vol.to_msgpack(compress='zlib'))
