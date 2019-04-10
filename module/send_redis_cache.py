import redis
from module.keystQuant import KeystQuant

keystq = KeystQuant()

cache_ip = '198.13.60.19'
cache_pw = 'da56038fa453c22d2c46e83179126e97d4d272d02ece83eb83a97357e842d065'

r = redis.StrictRedis(host=cache_ip, port=6379, password=cache_pw)

KOSPI_TICKERS = 'KOSPI_TICKERS'
KOSDAQ_TICKERS = 'KOSDAQ_TICKERS'
ETF_TICKERS = 'ETF_TICKERS'

KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'
ETF_OHLCV = 'ETF_OHLCV'

KOSPI_VOL = 'KOSPI_VOL'
KOSDAQ_VOL = 'KOSDAQ_VOL'
ETF_VOL = 'ETF_VOL'

kp_tickers = [ticker.decode() for ticker in r.lrange(KOSPI_TICKERS, 0 ,-1)]
kd_tickers = [ticker.decode() for ticker in r.lrange(KOSDAQ_TICKERS, 0 ,-1)]
etf_tickers = [ticker.decode() for ticker in r.lrange(ETF_TICKERS, 0 ,-1)]


kp_tickers_list, kd_tickers_list, etf_tickers_list, kp_tickers_dict, kd_tickers_dict, etf_tickers_dict = keystq.make_ticker_data(kp_tickers, kd_tickers, etf_tickers, mode='except_etf')
print(len(kp_tickers_list), len(kd_tickers_list), len(etf_tickers_list))

# kp_tickers_dict['003160']
kp_ohlcv, kp_vol = keystq.make_redis_ohlcv_df('kp', kp_tickers_list, kd_tickers_list, etf_tickers_list)
kd_ohlcv, kd_vol = keystq.make_redis_ohlcv_df('kd', kp_tickers_list, kd_tickers_list, etf_tickers_list)
etf_ohlcv, etf_vol = keystq.make_redis_ohlcv_df('etf', kp_tickers_list, kd_tickers_list, etf_tickers_list)

print(kp_ohlcv.shape, kd_ohlcv.shape, kp_vol.shape, kd_vol.shape, etf_ohlcv.shape ,etf_vol.shape)

for key in [KOSPI_OHLCV, KOSDAQ_OHLCV, ETF_OHLCV, KOSPI_OHLCV, KOSDAQ_VOL, ETF_VOL]:
    response = r.exists(key)
    if key_exists != False:
        r.delete(key)
        print('{} 이미 있음, 삭제하는 중...'.format(key))

r.set(KOSPI_OHLCV, kp_ohlcv.to_msgpack(compress='zlib'))
r.set(KOSDAQ_OHLCV, kd_ohlcv.to_msgpack(compress='zlib'))
r.set(ETF_OHLCV, etf_ohlcv.to_msgpack(compress='zlib'))
r.set(KOSPI_VOL, kp_vol.to_msgpack(compress='zlib'))
r.set(KOSDAQ_VOL, kd_vol.to_msgpack(compress='zlib'))
r.set(ETF_VOL, etf_vol.to_msgpack(compress='zlib'))
