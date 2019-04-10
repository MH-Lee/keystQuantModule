import time
import pandas as pd

from module.cache import RedisClient

redis = RedisClient()

KOSPI_TICKERS = 'KOSPI_TICKERS'
KOSDAQ_TICKERS = 'KOSDAQ_TICKERS'
ETF_TICKERS = 'ETF_TICKERS'

ETF_FULL_TICKERS = 'ETF_FULL_TICKERS'
ETN_LIST = 'ETN_LIST'

df1 = pd.read_csv('./etn_list.csv')
add_etn = df1['code'].tolist()

etf_ticker = redis.get_list(ETF_FULL_TICKERS)
etn_list = redis.get_list(ETN_LIST)
except_list = etf_ticker + etn_list
len(etn_list)
etn_new_list = list(set(etn_list+add_etn))

etn_new_list2 = [ETN_LIST]
new_list = etn_new_list2 + etn_new_list
redis.del_key(ETN_LIST)
redis.set_list(new_list)

MKT_DF_KEY = "MKTCAP_DF"
PRI_DF_KEY = "PRI_SELL_DF"
FRG_DF_KEY = "FRG_NET_DF"
INS_DF_KEY = "INS_NET_DF"

KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'
ETF_OHLCV = 'ETF_OHLCV'

pri_df = redis.get_df(PRI_DF_KEY)
pri_df.shape
frg_df = redis.get_df(FRG_DF_KEY)
ins_df = redis.get_df(INS_DF_KEY)
mkt_cap_df = redis.get_df(MKT_DF_KEY)
kp_ohlcv = redis.get_df(KOSPI_OHLCV)
kd_ohlcv = redis.get_df(KOSDAQ_OHLCV)
etf_ohlcv = redis.get_df(ETF_OHLCV)
total_ohlcv = pd.concat([kp_ohlcv,kd_ohlcv, etf_ohlcv], axis=1)

mkt_cap = mkt_cap_df[['005930']]*total_ohlcv[['005930']]
test1 = pri_df[['005930']]/mkt_cap
test1['mkt_cap'] = mkt_cap
test1
rank_df = test1.rank(method='min')
rank_df.sort_values(by=['005930'])

ticker_list = pri_df.columns.tolist()
len(ticker_list)
print(pri_df.shape, frg_df.shape, ins_df.shape)

kp_tickers = [ticker.decode() for ticker in redis.redis_client.lrange(KOSPI_TICKERS, 0 ,-1)]
kd_tickers = [ticker.decode() for ticker in redis.redis_client.lrange(KOSDAQ_TICKERS, 0 ,-1)]
total_tickers = kd_tickers + kp_tickers
len(total_tickers)

def make_ticker_dict(total_tickers):
    total_tickers_dict = dict()
    for ticker in total_tickers:
        total_tickers_dict[ticker.split('|')[0]] = ticker.split('|')[1]
    return total_tickers_dict

ticker_dict = make_ticker_dict(total_tickers)
ticker_list = pri_df.columns.tolist()
len(ticker_list)
refined_tickers = [t for t in ticker_list if t not in except_list]
len(refined_tickers)
ticker_list

def mkt_cap_calc(mkt_cap_df, pri_df):
    start = time.time()
    global total_mkt_cap
    ticker_list = pri_df.columns.tolist()
    refined_tickers = [t[0] for t in ticker_list if t not in except_list]
    make_data_start = False
    i = 0
    for ticker in refined_tickers:
        i+=1
        if i % 100==0:
            print(i,':',ticker)
        mkt_df = mkt_cap_df[[ticker]]*total_ohlcv[[ticker]]
        if make_data_start == False:
            total_mkt_cap = mkt_df
            make_data_start = True
        else:
            total_mkt_cap = pd.concat([total_mkt_cap, mkt_df], axis=1)
    print("all process is done!")
    end = time.time()
    print(end-start)
    return total_mkt_cap

def mkt_cap_buysell(mode, total_mkt_cap):
    start = time.time()
    global total_rank_df
    if mode == 'pri':
        analysis_df = pri_df
    elif mode == 'frg':
        analysis_df = frg_df
    elif mode == 'ins':
        analysis_df = ins_df
    else:
        print("Error")
    ticker_list = analysis_df.columns.tolist()
    make_data_start = False
    i = 0
    for ticker in ticker_list:
        i+=1
        if i % 100==0:
            print(i,':',ticker)
        mkt_divide = analysis_df[[ticker]]/total_mkt_cap[[ticker]]
        temp_rank = mkt_divide.rank(method='min')
        if make_data_start == False:
            total_rank_df = temp_rank
            make_data_start = True
        else:
            total_rank_df = pd.concat([total_rank_df, temp_rank], axis=1)
    print("all process is done!")
    end = time.time()
    print(end-start)
    return total_rank_df

def make_cache_data(mode):
    total_mkt_cap = mkt_cap_calc(mkt_cap_df, pri_df)
    if mode == 'pri':
        rank_df = mkt_cap_buysell('pri', total_mkt_cap)
        col_name = "{} sell rank".format(mode)
    elif mode == 'frg'
        rank_df = mkt_cap_buysell('frg', total_mkt_cap)
        col_name = "{} net rank".format(mode)
    elif mode == 'ins':
        rank_df = mkt_cap_buysell('ins', total_mkt_cap)
        col_name = "{} net rank".format(mode)
    else:
        print("error")
    recent_date = pri_rank.tail(1).index.tolist()[0].strftime('%Y%m%d')
    print(recent_date)
    ticker_df = pd.DataFrame.from_dict(ticker_dict, orient='index')
    ticker_df.columns = ['name']
    ticker_df['code'] = ticker_df.index
    recent_mkt= total_mkt_cap.tail(1).T
    recent_mkt.columns = ['mkt_cap']
    recent_mkt['code'] = recent_mkt.index
    recent_rank = rank_df.tail(1).T
    recent_rank.columns = [col_name]
    recent_rank['code'] = recent_rank.index
    cache_df = pd.merge(pd.merge(recent_rank, ticker_df, on='code', how='inner'),recent_mkt, on='code', how='inner')
    cache_df.columns
    cache_df.sort_values(col_name).dropna(inplace=True)
    return cache_df



original_data = redis.get_list('to_update_factor_list')
len(original_data)
original_data
df1 = redis.get_df('KOSPI_OHLCV')

list1 = ['20190208']
list2 = [i.strftime('%Y%m%d') for i in list1]
list3 = ['to_update_factor_list'] + list1

redis.set_list(list3)
redis.del_key('to_update_factor_list')
redis.set_list()
