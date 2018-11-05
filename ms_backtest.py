from module import market_signal
from module.market_signal import MarketSignal
import pandas as pd
import redis
%matplotlib inline

import matplotlib.pyplot as plt
ms = MarketSignal()
index_ohlcv, index_vol = ms.merge_index_data()

kp_vol_prc, kd_vol_prc, index_vol_prc, kp_ret, kd_ret, index_ret, ohlcv, volume, vol_prc, returns = ms.make_ohlcv_df(index_ohlcv, index_vol)
# ms_backtest(self, index_ohlcv, vol_prc, mode, invest_num=10, month_list="all", market = "코스피", period="M", rolling=200):
kp_m_ret, mode_index_dict = ms.ms_backtest(index_ohlcv, kp_vol_prc, ["mom","m_volt","m_volt_vol"], 10, [6])
(1 + kp_m_ret).cumprod().plot()
# df1 = pd.DataFrame.from_dict(mode_index_dict['m_volt'], orient='index')
# df2 = df1.T
# df2.to_csv('dd.csv', encoding='utf-8')
# pd.DataFrame(mode_index_dict['m_volt'][6])
kp_m_ret_10_acceler, mode_index_dict_acc = ms.ms_backtest(index_ohlcv, kp_vol_prc, invest_num=10, month_list="acceler", mode=["mom","m_volt","m_volt_vol"])
kp_m_ret_10_all, mode_index_dict_all= ms.ms_backtest(index_ohlcv, kp_vol_prc, invest_num=10, month_list="all", mode=["mom","m_volt"])
(1 + kp_m_ret_10_acceler).cumprod().plot()
(1 + kp_m_ret_10_all).cumprod().plot()

ohlcv.tail()
ohlcv['코스피'].plot()
ohlcv['코스닥'].plot()
ret = returns.dropna(how='all')
ret_pos = (ret>=0)
ret_lag3 = ret.rolling(20).mean().shift(-19)
ret_lag3_pos = (ret_lag3>=0)

ret_pos.head()
ret_lag3_pos.head()
ret_lag3.shape

(ret_pos*ret_lag3_pos).sum()/ret_lag3.shape[0]
