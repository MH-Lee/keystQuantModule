import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
plt.rcParams["figure.figsize"] = (12, 6)
# 그래프에서 마이너스 폰트 깨지는 문제에 대한 대처
mpl.rcParams['axes.unicode_minus'] = False
import redis
import os

from module.keystQuant import KeystQuant
from module.keystQuant import DATA_MAPPER, MARKET_CODES

directory = os.getcwd()

class MarketSignal(KeystQuant):

    def __init__(self):
        super().__init__()

    def dual_momentum(self, data, mode, month=None):
        # data (pd.DataFrame) --> 한 달을 주기로 resample된 데이터프레임
        # resample 처리가 안 된 상태라면, set_periodic_close() 메소드 사용
        if mode == 'all':
            for i in range(1, 13):
                momentum = data.pct_change(i)  # 단순 수익률: (P(t) - P(t-i))/P(t-i), P = 종가
                if i == 1:
                    temp = momentum
                else:
                    temp += momentum
            mom = temp / 12  # 위에서 구한 모든 모멘텀값을 더한 후 12로 나눔 (12개월 평균 모멘텀이 된다)
        # accelerating momentum 계산
        elif mode == 'acceler':
            for i in [1, 3, 6]:
                momentum = data.pct_change(i)  # 단순 수익률: (P(t) - P(t-i))/P(t-i), P = 종가
                if i == 1:
                    temp = momentum
                else:
                    temp += momentum
            mom = temp / 3  # 위에서 구한 모든 모멘텀값을 더한 후 12로 나눔 (12개월 평균 모멘텀이 된다)
        else:
            mom = data.pct_change(month)
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

    def make_rank_data(self, market_ohlcv, vol_prc, month_list, period, rolling, month=None):
        ohlcv_m = self.set_periodic_ret(market_ohlcv, period).last()
        ohlcv_m_mom = self.dual_momentum(ohlcv_m, month_list, month)
        ohlcv_m_mom_rank = self.calc_rank(ohlcv_m_mom, False)
        ohlcv_m_mom_score = self.calc_score(ohlcv_m_mom_rank)

        ohlcv_m_volt = self.volatility(market_ohlcv.pct_change(), rolling).resample('M').last()
        ohlcv_m_volt_rank = self.calc_rank(ohlcv_m_volt, True)
        ohlcv_m_volt_score = self.calc_score(ohlcv_m_volt_rank)

        m_vol_prc = self.set_periodic_ret(vol_prc, period).mean()
        m_vol_prc_rank = self.calc_rank(m_vol_prc, False)
        m_vol_prc_score = self.calc_score(m_vol_prc_rank)
        return ohlcv_m, ohlcv_m_mom_score, ohlcv_m_volt_score, m_vol_prc_score

    ##### momentum, volatility, volume_price를 가중치를 곱해서 total_score를 산출
    def sum_score(self, mode, mom, volt , vol_prc, m_w, v_w, vp_w):
        m = mode
        if m == "mom":
            total_score = m_w*mom
        elif m == "m_volt":
            total_score = (m_w*mom + v_w*volt) / 2
        elif m == "m_volt_vol":
            total_score = (m_w*mom + v_w*volt + vp_w*vol_prc) / 3
        else:
            print("mom, m_volt, m_volt_vol 중에 선택하시오")
        return total_score

    def filter_top_stock(self, ohlcv_m, total_score_rank, invest_num, ticker_dict, m_mom, m_volt, m_vol_prc):
        port_yc = []
        index_dict = dict()
        date_list = total_score_rank.index.tolist()
        for date in range(len(ohlcv_m) - 1):
            invest_end_date = date + 1
            top_index = total_score_rank.iloc[date]
            top_index = top_index[top_index < invest_num + 1].index
            invest_return = ohlcv_m.pct_change().iloc[invest_end_date][top_index].mean()
            stock_list = [i for i in top_index.tolist()]
            m_mom_rank =  m_mom.loc[date_list[date], stock_list]
            m_volt_rank = m_volt.loc[date_list[date], stock_list]
            m_vol_prc_rank = m_volt.loc[date_list[date], stock_list]
            if len(m_mom) == len(m_volt) == len(m_vol_prc):
                stock_data_dict = dict()
                for i in range(len(stock_list)):
                    score_list = [m_mom_rank.values[i], m_volt_rank.values[i], m_vol_prc_rank[i]]
                    stock_data_dict[ticker_dict[stock_list[i]]] = score_list
            index_dict[date_list[date]] = stock_data_dict
            port_yc.append(invest_return)
        return port_yc, index_dict
    
    # def filter_top_stock(self, ohlcv_m, total_score_rank, invest_num, ticker_dict):
    #     date_list = total_score_rank.index.tolist()
    #     port_yc = []
    #     index_dict = dict()
    #     for date in range(len(ohlcv_m) - 1):
    #         invest_end_date = date + 1
    #         top_index = total_score_rank.iloc[date]
    #         top_index = top_index[top_index < invest_num + 1].index
    #         invest_return = ohlcv_m.pct_change().iloc[invest_end_date][top_index].mean()
    #         stock_list = [ticker_dict[i] for i in top_index.tolist()]
    #         index_dict[date_list[date]] = stock_list
    #         port_yc.append(invest_return)
    #     return port_yc, index_dict

    #### invest_num : 상위 종목 몇개를 조합할 것인것인지 정하는 변수
    #### month_list = [1, 3, 6, 12]
    #### month_list = "all"은 12개월 평균 momentum
    #### market = ['코스피', '코스닥']
    ####         ['코스피 대형주', '코스피 중형주', '코스피 소형주', '코스닥 대형주', '코스닥 중형주', '코스닥 소형주']
    ####         ['성장주', '가치주', '배당주', '퀄리티주', '사회책임경영주']
    #### mode = ["mom","m_vol","m_volt_vol"]
    def ms_backtest(self, index_ohlcv, vol_prc, mode, invest_num=10, month_list="all", market = "코스피", period="M", rolling=200, m_w=1, v_w=1, vp_w=1):
        col_dict = {"mom": "mom", "m_volt": "mom + volt", "m_volt_vol":"mom + volt + volume"}
        _, _, kp_ticker_dict, kd_ticker_dict = self.make_ticker_data(self.kp_tickers, self.kd_tickers)
        if type(month_list) == str:
            month_list = month_list.lower()
        index_df = pd.DataFrame(index_ohlcv[market])
        index_df.index = pd.to_datetime(index_df.index)
        m_ret = self.set_periodic_ret(index_df, period="M").last().pct_change().fillna(0)
        m_ret = m_ret.iloc[:-1, :]
        if market == "코스피":
            market_ohlcv = self.kp_ohlcv
            ticker_dict = kp_ticker_dict
        elif market == "코스닥":
            market_ohlcv = self.kd_ohlcv
            ticker_dict = kd_ticker_dict
        invest_num = invest_num
        if month_list == "all" or month_list == "acceler":
            ohlcv_m, ohlcv_m_mom_score, ohlcv_m_volt_score, m_vol_prc_score = self.make_rank_data(market_ohlcv, vol_prc, month_list, period, rolling)
            # if not os.path.exists(directory + '\\data'):
            #     os.mkdir(directory + '\\data')
            # ohlcv_m_mom_score.to_csv('{}\\m_mom.csv'.format(directory + '\\data'), encoding='utf-8')
            # ohlcv_m_volt_score.to_csv('{}\\m_volt_score.csv'.format(directory + '\\data'), encoding='utf-8')
            # m_vol_prc_score.to_csv('{}\\m_vol_prc_score.csv'.format(directory + '\\data'), encoding='utf-8')
            mode_index_dict = dict()
            for m in mode:
                total_score = self.sum_score(m, ohlcv_m_mom_score, ohlcv_m_volt_score, m_vol_prc_score, m_w, v_w, vp_w)
                # total_score.to_csv('{}\\total_score_{}.csv'.format(directory + '\\data', m), encoding='utf-8')
                total_score_rank = total_score.T.rank(ascending=False).T
                port_yc, index_dict = self.filter_top_stock(ohlcv_m, total_score_rank, invest_num, ticker_dict, ohlcv_m_mom_score, ohlcv_m_volt_score, m_vol_prc_score)
                colname = col_dict[m]
                mode_index_dict[m] = index_dict
                m_ret["{}".format(colname)] = port_yc
                ir = self.information_ratio(m_ret["{}".format(colname)], m_ret[market])
                print("{}: {}".format(colname, ir))
        else:
            mode_index_dict = dict()
            for m in mode:
                month_list_dict = dict()
                for i in month_list:
                    ohlcv_m, ohlcv_m_mom_score, ohlcv_m_volt_score, m_vol_prc_score = self.make_rank_data(market_ohlcv, vol_prc, month_list, period, rolling, month=i)
                    total_score = self.sum_score(m, ohlcv_m_mom_score, ohlcv_m_volt_score, m_vol_prc_score, m_w, v_w, vp_w)
                    total_score_rank = total_score.T.rank(ascending=False).T
                    port_yc, index_dict = self.filter_top_stock(ohlcv_m, total_score_rank, invest_num, ticker_dict, ohlcv_m_mom_score, ohlcv_m_volt_score, m_vol_prc_score)
                    colname = col_dict[m]
                    month_list_dict[i] = index_dict
                    m_ret["{}_{}".format(colname, i)] = port_yc
                    ir = self.information_ratio(m_ret["{}_{}".format(colname, i)], m_ret[market])
                    print("{}_{}: {}".format(colname, i, ir))
                mode_index_dict[m] = month_list_dict
        return m_ret, mode_index_dict
