from module import market_signal
from module.market_signal import MarketSignal
%matplotlib inline

import matplotlib.pyplot as plt
ms = MarketSignal()
index_ohlcv, index_vol = ms.merge_index_data()

kp_vol_prc, kd_vol_prc, index_vol_prc, kp_ret, kd_ret, index_ret, ohlcv, volume, vol_prc, returns = ms.make_ohlcv_df(index_ohlcv, index_vol)
# ms_backtest(self, index_ohlcv, vol_prc, mode, invest_num=10, month_list="all", market = "코스피", period="M", rolling=200):
kp_m_ret, mode_index_dict = ms.ms_backtest(index_ohlcv, kp_vol_prc, ["mom","m_volt","m_volt_vol"], 10, [6])
kp_m_ret.columns
(1+kp_m_ret['mom + volt_6']).cumprod()[-1]
mode_index_dict
mode_index_dict.keys()

import pandas as pd

df1 = pd.DataFrame.from_dict(mode_index_dict['m_volt'], orient='index')
df2 = df1.T
df2.to_csv('dd.csv', encoding='utf-8')
kp_m_ret['코스피'plo
pd.DataFrame(mode_index_dict['m_volt'][6])
(1 + kp_m_ret).cumprod().plot()
kp_m_ret_10_mvv = ms.ms_backtest(index_ohlcv, kp_vol_prc, 10, [1, 3, 6, 12], ["mom","m_volt","m_volt_vol"])
kp_m_ret_10_mv = ms.ms_backtest(index_ohlcv, kp_vol_prc, 10, [1, 3, 6, 12], ["mom","m_volt"])
(1 + kp_m_ret_10_mvv).cumprod().plot()
(1 + kp_m_ret_10_mv).cumprod().plot()
