from module import market_signal
from module.market_signal import MarketSignal
%matplotlib inline

import matplotlib.pyplot as plt
ms = MarketSignal()
index_ohlcv, index_vol = ms.merge_index_data()

kp_vol_prc, kd_vol_prc, index_vol_prc, kp_ret, kd_ret, index_ret, ohlcv, volume, vol_prc, returns = ms.make_ohlcv_df(index_ohlcv, index_vol)
kp_m_ret = ms.ms_backtest(index_ohlcv, kp_vol_prc, 20, [6], ["mom","m_volt","m_volt_vol"])
kp_m_ret_10_mvv = ms.ms_backtest(index_ohlcv, kp_vol_prc, 10, [1, 3, 6, 12], ["mom","m_volt","m_volt_vol"])
kp_m_ret_10_mv = ms.ms_backtest(index_ohlcv, kp_vol_prc, 10, [1, 3, 6, 12], ["mom","m_volt"])
(1 + kp_m_ret).cumprod().plot()
(1 + kp_m_ret_10_mvv).cumprod().plot()
(1 + kp_m_ret_10_mv).cumprod().plot()
