from module.cache import RedisClient
import numpy as np
import pandas as pd

redis = RedisClient()


PER_DF_KEY = "PER_DF"
PBR_DF_KEY = "PBR_DF"
PSR_DF_KEY = "PSR_DF"
PCR_DF_KEY = "PCR_DF"

KOSPI_OHLCV = 'KOSPI_OHLCV'
KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'

pbr_df = redis.get_df(PBR_DF_KEY)
per_df = redis.get_df(PER_DF_KEY)
psr_df = redis.get_df(PSR_DF_KEY)
pcr_df = redis.get_df(PCR_DF_KEY)
kp_ohlcv = redis.get_df(KOSPI_OHLCV)
kd_ohlcv = redis.get_df(KOSDAQ_OHLCV)

pbr_df.shape

pbr_Q = pbr_df.resample('Q').last()
per_Q = per_df.resample('Q').last()
psr_Q = psr_df.resample('Q').last()
pcr_Q = pcr_df.resample('Q').last()

pbr_Q
make_df_first = True
for i in range(pbr_Q.shape[0]):
    min_val = pbr_df.iloc[i].min()
    bins = np.linspace(5, min_val, 11)
    bins = sorted(list(bins), reverse=False)
    labels = list(range(1,11))
    if make_df_first == True:
        digitize_array = pd.cut(pbr_Q.T.iloc[:,i].astype('float'), bins, labels=labels)
        digitize_df = pd.DataFrame(digitize_array)
        make_df_first = False
    else:
        add_array = pd.cut(pbr_Q.T.iloc[:,i].astype('float'), bins, labels=labels)
        add_df = pd.DataFrame(add_array)
        digitize_df = pd.concat([digitize_df, add_df], axis=1)
digitize_df = digitize_df.T


digitize_df.iloc[0][digitize_df.iloc[0] == 1].index
index_list = list(digitize_df.iloc[0][digitize_df.iloc[0] == 2].index)
ohlcv_Q = kd_ohlcv.resample('Q').last()
ohlcv_Q_index = list(ohlcv_Q.columns)
intersec_list = list(set(index_list).intersection(ohlcv_Q_index))
len(intersec_list)

ohlcv_Q[intersec_list]

kp_ohlcv.shape
kd_ohlcv.shape

ohlcv = pd.concat([kp_ohlcv, kd_ohlcv], axis=1)
ohlcv_Q_return = ohlcv.resample('Q').last().pct_change()

port_yc = []
for date in range(len(ohlcv_Q_return) - 1):
    invest_end_date = date + 1
    Q1_data = digitize_df.iloc[date]
    top_index = list(Q1_data[Q1_data == 1].index)
    ohlcv_Q_index = list(ohlcv_Q.columns)
    intersec_list = list(set(index_list).intersection(ohlcv_Q_index))
    ohlcv_Q_return_row = ohlcv_Q_return.iloc[invest_end_date]
    low_pbr_invest = ohlcv_Q_return_row[intersec_list]
    invest_return = low_pbr_invest.mean()
    port_yc.append(invest_return)

return_df = pd.DataFrame({'low_pbr':port_yc})
(1+return_df).cumprod().plot()

# nd_list = []
# for i in range(0,25):
#     ten_list = pbr_Q.iloc[i][pbr_Q.iloc[i] > 5].tolist()
#     nd_list.append(len(ten_list))
# nd_list
#
# nda1 = pd.cut(pbr_Q.T.iloc[:,0].astype('float'), bins, labels=labels)
# nda2 = pd.cut(pbr_Q.T.iloc[:,1].astype('float'), bins, labels=labels)
#
# df1 = pd.DataFrame(nda1)
# df2 = pd.DataFrame(nda2)
#
# kp_ohlcv = kp_ohlcv.resample('Q').last().pct_change()
# kp_ohlcv
#
#
# pd.concat([df1, df2], axis=1).T
