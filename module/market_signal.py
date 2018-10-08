import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
plt.rcParams["figure.figsize"] = (12, 6)
# 그래프에서 마이너스 폰트 깨지는 문제에 대한 대처
mpl.rcParams['axes.unicode_minus'] = False
import redis

from module import keystQuant
from module.keystQuant import DATA_MAPPER, MARKET_CODES

class MarketSignal(keystQuant):

    def __init__(self):
        super().__init__()

    def dual_momentum(self, data):
        # data (pd.DataFrame) --> 한 달을 주기로 resample된 데이터프레임
        # resample 처리가 안 된 상태라면, set_periodic_close() 메소드 사용
        for i in range(1, 13):
            momentum = data.pct_change(i)  # 단순 수익률: (P(t) - P(t-i))/P(t-i), P = 종가
            if i == 1:
                temp = momentum
            else:
                temp += momentum
        mom = temp / 12  # 위에서 구한 모든 모멘텀값을 더한 후 12로 나눔 (12개월 평균 모멘텀이 된다)
        return mom.fillna(0)  # nan은 0으로 처리

    def volatility(self, returns_data, window=12):
        # 변동성 계산
        # 보통 변동성 계산은 일년을 주기로 계산한다
        # 그래서 returns_data 월별로 resample된거라면 window를 12로 잡는다
        # (데이터가 일일 데이터면 보통 window를 200으로 잡는다)
        # (데이터가 일주일로 resample되었다면, window는 48을 잡는다)
        # (3개월/분기별로 resample 되었다면 window는 4로 잡는다)
        return returns_data.rolling(window=window).std().fillna(0)

    def information_ratio(self, data, benchmark):
        ir = pd.DataFrame()
        ir['Excess Return'] = data - benchmark
        ir = ir['Excess Return'].mean() / ir['Excess Return'].std()
        return ir

    #### invest_num : 상위 종목 몇개를 조합할 것인것인지 정하는 변수
    #### month_list = [1, 3, 6, 12]
    #### market = ['코스피', '코스닥']
    ####         ['코스피 대형주', '코스피 중형주', '코스피 소형주', '코스닥 대형주', '코스닥 중형주', '코스닥 소형주']
    ####         ['성장주', '가치주', '배당주', '퀄리티주', '사회책임경영주']
    #### mode = ["mom","m_vol","m_volt_vol"]
    def ms_backtest(self, index_ohlcv, kp_vol_prc, invest_num, month_list, mode, market = "코스피", period="M", rolling=200):
        col_dict = {"mom": "mom", "m_volt": "mom + volt", "m_volt_vol":"mom + volt + volume"}
        kp = pd.DataFrame(index_ohlcv[market])
        kp.index = pd.to_datetime(kp.index)
        kp_m_ret = kp.resample(period).last().pct_change().fillna(0)
        kp_m_ret = kp_m_ret.iloc[:-1, :]
        invest_num = invest_num
        for m in mode:
            for i in month_list:
                kp_m = self.kp_ohlcv.resample(period).last()
                kp_m_mom = kp_m.pct_change(i).fillna(0)
                kp_m_mom_rank = kp_m_mom.T.rank(ascending=False).T
                kp_m_mom_score = 1 - (kp_m_mom_rank / kp_m_mom_rank.max())

                kp_m_volt = self.kp_ohlcv.pct_change().rolling(rolling).std().resample('M').last()
                kp_m_volt_rank = kp_m_volt.T.rank(ascending=True).T
                kp_m_volt_score = 1 - (kp_m_volt_rank / kp_m_volt_rank.max())

                kp_m_vol_prc = kp_vol_prc.resample(period).mean()
                kp_m_vol_prc_rank = kp_m_vol_prc.T.rank(ascending=False).T
                kp_m_vol_prc_score = 1 - (kp_m_vol_prc_rank / kp_m_vol_prc_rank.max())
                if m == "mom":
                    total_score = kp_m_mom_score
                elif m == "m_volt":
                    total_score = (kp_m_mom_score + kp_m_volt_score) / 2
                elif m == "m_volt_vol":
                    total_score = (kp_m_mom_score + kp_m_volt_score + kp_m_vol_prc_score) / 3
                else:
                    print("mom, m_volt, m_volt_vol 중에 선택하시오")
                total_score_rank = total_score.T.rank(ascending=False).T
                port_yc = []
                for date in range(len(kp_m) - 1):
                  invest_end_date = date + 1
                  top_index = total_score_rank.iloc[date]
                  top_index = top_index[top_index < invest_num + 1].index
                  invest_return = kp_m.pct_change().iloc[invest_end_date][top_index].mean()
                  port_yc.append(invest_return)
                colname = col_dict[m]
                kp_m_ret["{}_{}".format(colname, i)] = port_yc
                ir = self.information_ratio(kp_m_ret["{}_{}".format(colname, i)], kp_m_ret[market])
                print("{}_{}: {}".format(colname, i, ir))
        return kp_m_ret
