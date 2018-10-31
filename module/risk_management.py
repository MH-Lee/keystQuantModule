import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import redis

from module import keystQuant
from module.keystQuant import DATA_MAPPER, MARKET_CODES

class RiskMangement(keystQuant):

    def __init__(self):
        super().__init__()

    def kelly_criterion(self, rt, kp_ret, window):
        '''
        rt: 종목의 종가로 계산한 리턴값
        kp_ret: 코스피 지수로 계산한 리턴값
        window: rolling시킬 일수 (보통 60 사용)
        '''
        # 배팅 비중 계산 (Kelly Criterion 사용)

        # STEP 1: excess return 계산하기
        # 종목 평균 수익률 - 코스피 평균 수익률 (rolling으로 처리하여 계산)
        ret_list = []

        rolling_rt = rt.rolling(window).mean()
        rolling_kp_ret = kp_ret.rolling(window).mean()

        for i in range(len(rt)):
            if i == 0:
                stock_return = rt[i] # nan
                kospi_return = kp_ret[i] # nan
            else:
                stock_return = rt[:i].mean() if i < window else rolling_rt[i]
                kospi_return = kp_ret[:i].mean() if i < window else rolling_kp_ret[i]
            excess_return = stock_return - kospi_return
            ret_list.append(excess_return)

        kelly = pd.DataFrame(ret_list)
        kelly.rename(columns={0: 'Excess Return'}, inplace=True)
        kelly['Excess Return MA'] = kelly['Excess Return'].rolling(window).mean() # excess return의 moving average
        kelly['Excess Return MV'] = kelly['Excess Return'].rolling(window).var() # excess return의 moving variance

        kelly_criterion = []

        for i in range(len(kelly)):
            if i == 0:
                exc_ret_mean = kelly['Excess Return'][i].mean()
                exc_ret_var = kelly['Excess Return'][i].var()
            else:
                exc_ret_mean = kelly['Excess Return'][:i].mean() if i < window else kelly['Excess Return MA'][i]
                exc_ret_var = kelly['Excess Return'][:i].var() if i < window else kelly['Excess Return MV'][i]
            kelly_ratio = exc_ret_mean / exc_ret_var
            kelly_criterion.append(kelly_ratio)

        kelly['Kelly Criterion'] = kelly_criterion
        # 캘리 숫자를 0 ~ 1로 스케일링하기 위해 max, min 구하기
        kelly['Kelly Criterion MAX'] = kelly['Kelly Criterion'].rolling(window).max()
        kelly['Kelly Criterion MIN'] = kelly['Kelly Criterion'].rolling(window).min()

        invest_ratio = []

        for i in range(len(kelly['Kelly Criterion'])):

            if i == 0:
                # 시작은 자본금 전체 투자한다
                invest_amt = 1

            if i < window:
                # window보다 작은 인덱스값은 위에서 계산한 max, min이 없기 때문에 따로 rolling으로 데이터를 묶어서 max, min을 계산
                kelly_nums = kelly['Kelly Criterion'][:i] # 0 부터 현재값까지 모두 묶기
                max_kelly = kelly_nums.max()
                min_kelly = kelly_nums.min()
                invest_amt = (kelly['Kelly Criterion'][i] - min_kelly) / (max_kelly - min_kelly) if max_kelly - min_kelly != 0 else 1

            if i >= window:
                max_kelly = kelly['Kelly Criterion MAX'][i]
                min_kelly = kelly['Kelly Criterion MIN'][i]
                invest_amt = (kelly['Kelly Criterion'][i] - min_kelly) / (max_kelly - min_kelly) if max_kelly - min_kelly != 0 else 1

            if pd.isnull(kelly['Kelly Criterion'][i]):
                # 캘리 숫자가 없으면 자본금 모두를 투자 (보통 초기 몇개 빼고는 모두 캘리 숫자가 있다)
                invest_amt = 1

            invest_ratio.append(invest_amt)

        # 초기 몇개의 데이터는 음수인 숫자도 있고 1이 훨씬 넘는 숫자도 있다
        # 모두 제거한다
        invest_ratio = [ratio if (ratio <= 1) and (ratio >= 0) else 1 for ratio in invest_ratio]

        kelly['Invest Ratio'] = invest_ratio
        kelly['Invest Ratio'].fillna(1, inplace=True)

        return pd.DataFrame(kelly['Invest Ratio'])

    def buy(self, capital, invest_ratio, buy_price, stock_num, portfolio_value):
        # 1: 살 수 있는 주식의 수를 계산한다
        stock_buy_num = (capital * invest_ratio) // buy_price
        # 2: 총 보유중인 주식수를 계산한다
        stock_num = stock_num + stock_buy_num
        # 3: 주식을 사는데 드는 비용을 계산한다
        cost = stock_buy_num * buy_price
        # 4: 보유중인 자본금과 포트폴리오의 가치를 계산한다
        capital = capital - cost
        portfolio_value = portfolio_value + cost
        return capital, stock_num, portfolio_value

    def sell(sefl, capital, sell_price, stock_num, portfolio_value):
        # 1: 수익금액을 계산한다
        profit = stock_num * sell_price
        # 2: 자본금을 다시 계산한다
        capital = capital + profit
        # 3: 보유 주식수와 포트폴리오 가치를 0으로 새팅한다
        stock_num = 0
        portfolio_value = 0
        return capital, stock_num, portfolio_value
