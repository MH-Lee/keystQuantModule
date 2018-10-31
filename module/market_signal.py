import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import redis

from module.keystQuant import KeystQuant
from module.keystQuant import DATA_MAPPER, MARKET_CODES

class MarketSignal(KeystQuant):

    def __init__(self):
        super().__init__()

    def dual_momentum(self, data, mode):
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
            mom = data.pct_change(mode)
        # 위에서 구한 모든 모멘텀값을 더한 후 12로 나눔 (12개월 평균 모멘텀이 된다)
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
    #### month_list = "all"은 12개월 평균 momentum, acceler = 1,3,6 평균
    #### market = ['코스피', '코스닥']
    ####         ['코스피 대형주', '코스피 중형주', '코스피 소형주', '코스닥 대형주', '코스닥 중형주', '코스닥 소형주']
    ####         ['성장주', '가치주', '배당주', '퀄리티주', '사회책임경영주']
    #### mode = ["mom","m_vol","m_volt_vol"]
    def ms_backtest(self, index_ohlcv, vol_prc, mode, invest_num=10, month_list="all", market = "코스피", period="M", rolling=200, mw=1, vw=1, vpw=1):
        col_dict = {"mom": "mom", "m_volt": "mom + volt", "m_volt_vol":"mom + volt + volume"}
        _, _, kp_ticker_dict, kd_ticker_dict = self.make_ticker_data(self.kp_tickers, self.kd_tickers)
        if type(month_list) == str:
            month_list = month_list.lower()
        index_df  = pd.DataFrame(index_ohlcv[market])
        index_df .index = pd.to_datetime(index_df.index)
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
            m_ohlcv = self.set_periodic_ret(market_ohlcv, period).last()
            m_mom = self.dual_momentum(m_ohlcv, month_list)
            m_mom_rank = m_mom.T.rank(ascending=False).T
            m_mom_score = 1 - (m_mom_rank / m_mom_rank.max())

            m_volt = self.volatility(market_ohlcv.pct_change(), rolling).resample('M').last()
            m_volt_rank = m_volt.T.rank(ascending=True).T
            m_volt_score = 1 - (m_volt_rank / m_volt_rank.max())

            m_vol_prc = self.set_periodic_ret(vol_prc, period).mean()
            m_vol_prc_rank = m_vol_prc.T.rank(ascending=False).T
            m_vol_prc_score = 1 - (m_vol_prc_rank / m_vol_prc_rank.max())
            mode_index_dict = dict()
            for m in mode:
                if m == "mom":
                    total_score = m_mom_score
                elif m == "m_volt":
                    total_score = (m_mom_score + m_volt_score) / 2
                elif m == "m_volt_vol":
                    total_score = (mw*m_mom_score + vw*m_volt_score + vw*m_vol_prc_score) / 3
                else:
                    print("mom, m_volt, m_volt_vol 중에 선택하시오")
                total_score_rank = total_score.T.rank(ascending=False).T
                data_list = total_score_rank.index.tolist()
                port_yc = []
                index_dict = dict()
                date_list = total_score_rank.index.tolist()
                for date in range(len(m_ohlcv) - 1):
                    invest_end_date = date + 1
                    top_index = total_score_rank.iloc[date]
                    top_index = top_index[top_index < invest_num + 1].index
                    invest_return = m_ohlcv.pct_change().iloc[invest_end_date][top_index].mean()
                    port_yc.append(invest_return)
                    stock_list = [ticker_dict[i] for i in top_index.tolist()]
                    if len(stock_list) == 0:
                        print("empty list")
                        continue
                    m_mom_check =  m_mom_rank.loc[date_list[date], stock_list]
                    m_volt_check = m_volt_rank.loc[date_list[date], stock_list]
                    m_vol_prc_check = m_vol_prc_rank.loc[date_list[date], stock_list]
                    stock_data_dict = dict()
                    for j in range(len(stock_list)):
                        print(m_mom_check.values[j], m_volt_check.values[j], m_vol_prc_check.values[j])
                        score_list = [m_mom_check.values[j], m_volt_check.values[j], m_vol_prc_check[j]]
                        stock_data_dict[ticker_dict[stock_list[j]]] = score_list
                    index_dict[date_list[date]] = stock_data_dict
                    month_list_dict[i] = index_dict
                mode_index_dict[m] = index_dict
                colname = col_dict[m]
                m_ret["{}".format(colname)] = port_yc
                ir = self.information_ratio(m_ret["{}".format(colname)], m_ret[market])
                print("{}: {}".format(colname, ir))
        else:
            mode_index_dict = dict()
            for m in mode:
                month_list_dict = dict()
                for i in month_list:
                    m_ohlcv = self.set_periodic_ret(market_ohlcv, period).last()
                    m_mom = self.dual_momentum(m_ohlcv, i)
                    m_mom_rank = m_mom.T.rank(ascending=False).T
                    m_mom_score = 1 - (m_mom_rank / m_mom_rank.max())

                    m_volt = self.volatility(market_ohlcv.pct_change(), rolling).resample('M').last()
                    m_volt_rank = m_volt.T.rank(ascending=True).T
                    m_volt_score = 1 - (m_volt_rank / m_volt_rank.max())

                    m_vol_prc = self.set_periodic_ret(vol_prc, period="M").mean()
                    m_vol_prc_rank = m_vol_prc.T.rank(ascending=False).T
                    m_vol_prc_score = 1 - (m_vol_prc_rank / m_vol_prc_rank.max())
                    if m == "mom":
                        total_score = m_mom_score
                    elif m == "m_volt":
                        total_score = (m_mom_score + m_volt_score) / 2
                    elif m == "m_volt_vol":
                        total_score = (mw*m_mom_score + vw*m_volt_score + vw*m_vol_prc_score) / 3
                    else:
                        print("mom, m_volt, m_volt_vol 중에 선택하시오")
                    total_score_rank = total_score.T.rank(ascending=False).T
                    port_yc = []
                    index_dict = dict()
                    date_list = total_score_rank.index.tolist()
                    for date in range(len(m_ohlcv) - 1):
                        invest_end_date = date + 1
                        top_index = total_score_rank.iloc[date]
                        top_index = top_index[top_index < invest_num + 1].index
                        invest_return = m_ohlcv.pct_change().iloc[invest_end_date][top_index].mean()
                        stock_list = [i for i in top_index.tolist()]
                        port_yc.append(invest_return)
                        if len(stock_list) == 0:
                            print("empty list")
                            continue
                        m_mom_check =  m_mom_rank.loc[date_list[date], stock_list]
                        m_volt_check = m_volt_rank.loc[date_list[date], stock_list]
                        m_vol_prc_check = m_vol_prc_rank.loc[date_list[date], stock_list]
                        stock_data_dict = dict()
                        for j in range(len(stock_list)):
                            print(m_mom_check.values[j], m_volt_check.values[j], m_vol_prc_check.values[j])
                            score_list = [m_mom_check.values[j], m_volt_check.values[j], m_vol_prc_check[j]]
                            stock_data_dict[ticker_dict[stock_list[j]]] = score_list
                        index_dict[date_list[date]] = stock_data_dict
                        month_list_dict[i] = index_dict
                    colname = col_dict[m]
                    m_ret["{}_{}".format(colname, i)] = port_yc
                    ir = self.information_ratio(m_ret["{}_{}".format(colname, i)], m_ret[market])
                    print("{}_{}: {}".format(colname, i, ir))
                mode_index_dict[m] = month_list_dict
        return m_ret, mode_index_dict
