import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import redis
import requests

DATA_MAPPER = {
    'index_tickers': 'INDEX_TICKERS',
    'kospi_tickers': 'KOSPI_TICKERS',
    'kosdaq_tickers': 'KOSDAQ_TICKERS',
    'kospi_ohlcv': 'KOSPI_OHLCV',
    'kospi_vol': 'KOSPI_VOL',
    'kosdaq_ohlcv': 'KOSDAQ_OHLCV',
    'kosdaq_vol': 'KOSDAQ_VOL',
    'index': '_INDEX',
    'ohlcv': '_OHLCV'
}

MARKET_CODES = {
    # 시장 인덱스
    '코스피': 'I.001',
    '코스닥': 'I.201',

    # 사이즈 인덱스
    '코스피 대형주': 'I.002',
    '코스피 중형주': 'I.003',
    '코스피 소형주': 'I.004',
    '코스닥 대형주': 'I.202',
    '코스닥 중형주': 'I.203',
    '코스닥 소형주': 'I.204',

    # 스타일 인덱스
    '성장주': 'I.431',  # KRX 스마트 모멘텀
    '가치주': 'I.432',  # KRX 스마트 밸류
    '배당주': 'I.192',  # KRX 고배당 50
    '퀄리티주': 'I.433',  # KRX 스마트 퀄리티
    '사회책임경영주': 'I.426',  # KRX 사회책임경영

    # 산업 인덱스
    '코스피 음식료품': 'I.005',
    '코스피 섬유,의복': 'I.006',
    '코스피 종이,목재': 'I.007',
    '코스피 화학': 'I.008',
    '코스피 의약품': 'I.009',
    '코스피 비금속광물': 'I.010',
    '코스피 철강및금속': 'I.011',
    '코스피 기계': 'I.012',
    '코스피 전기,전자': 'I.013',
    '코스피 의료정밀': 'I.014',
    '코스피 운수장비': 'I.015',
    '코스피 유통업': 'I.016',
    '코스피 전기가스업': 'I.017',
    '코스피 건설업': 'I.018',
    '코스피 운수창고': 'I.019',
    '코스피 통신업': 'I.020',
    '코스피 금융업': 'I.021',
    '코스피 은행': 'I.022',
    '코스피 증권': 'I.024',
    '코스피 보험': 'I.025',
    '코스피 서비스업': 'I.026',
    '코스피 제조업': 'I.027',
    '코스닥 기타서비스': 'I.212',
    '코스닥 IT종합': 'I.215',
    '코스닥 제조': 'I.224',
    '코스닥 건설': 'I.226',
    '코스닥 유통': 'I.227',
    '코스닥 운송': 'I.229',
    '코스닥 금융': 'I.231',
    '코스닥 오락, 문화': 'I.237',
    '코스닥 통신방송서비스': 'I.241',
    '코스닥 IT S/W & SVC': 'I.242',
    '코스닥 IT H/W': 'I.243',
    '코스닥 음식료,담배': 'I.256',
    '코스닥 섬유,의류': 'I.258',
    '코스닥 종이,목재': 'I.262',
    '코스닥 출판,매체복제': 'I.263',
    '코스닥 화학': 'I.265',
    '코스닥 제약': 'I.266',
    '코스닥 비금속': 'I.267',
    '코스닥 금속': 'I.268',
    '코스닥 기계,장비': 'I.270',
    '코스닥 일반전기,전자': 'I.272',
    '코스닥 의료,정밀기기': 'I.274',
    '코스닥 운송장비,부품': 'I.275',
    '코스닥 기타 제조': 'I.277',
    '코스닥 통신서비스': 'I.351',
    '코스닥 방송서비스': 'I.352',
    '코스닥 인터넷': 'I.353',
    '코스닥 디지탈컨텐츠': 'I.354',
    '코스닥 소프트웨어': 'I.355',
    '코스닥 컴퓨터서비스': 'I.356',
    '코스닥 통신장비': 'I.357',
    '코스닥 정보기기': 'I.358',
    '코스닥 반도체': 'I.359',
    '코스닥 IT부품': 'I.360'
}

class KeystQuant(object):

    def __init__(self):
        self.cache_ip = '198.13.60.19'
        self.cache_pw = 'da56038fa453c22d2c46e83179126e97d4d272d02ece83eb83a97357e842d065'

        self.r = redis.StrictRedis(host=self.cache_ip, port=6379, password=self.cache_pw)
        self.mkcap_url = 'http://45.76.202.71:3000/api/v1/stocks/mktcap/?date=20181008&page={}'

        # redis keys
        self.KOSPI_INDEX = 'I.001_INDEX'

        self.KOSPI_TICKERS = 'KOSPI_TICKERS'
        self.KOSDAQ_TICKERS = 'KOSDAQ_TICKERS'

        self.KOSPI_OHLCV = 'KOSPI_OHLCV'
        self.KOSDAQ_OHLCV = 'KOSDAQ_OHLCV'

        self.KOSPI_VOL = 'KOSPI_VOL'
        self.KOSDAQ_VOL = 'KOSDAQ_VOL'

        self.kp_tickers = [ticker.decode() for ticker in self.r.lrange(self.KOSPI_TICKERS, 0 ,-1)]
        self.kd_tickers = [ticker.decode() for ticker in self.r.lrange(self.KOSDAQ_TICKERS, 0 ,-1)]
        self.tickers = self.kp_tickers + self.kd_tickers

        # 종가/거래량 데이터 불러오기
        self.kp_ohlcv = pd.read_msgpack(self.r.get(self.KOSPI_OHLCV))
        self.kd_ohlcv = pd.read_msgpack(self.r.get(self.KOSDAQ_OHLCV))

        self.kp_vol = pd.read_msgpack(self.r.get(self.KOSPI_VOL))
        self.kd_vol = pd.read_msgpack(self.r.get(self.KOSDAQ_VOL))
        print("ready!")

    def get_val(self, redis_client, key):
        ## mined에서 사용하게 될 모든 데이터는 TICKERS 데이터가 아니면 pandas df이다
        ## TICKERS 데이터는 리스트 형식이다
        if 'TICKERS' in key:
            data = redis_client.lrange(key, 0, -1)
            data = list(map(lambda x: x.decode('utf-8'), data))
        else:
            data = pd.read_msgpack(redis_client.get(key))  # 레디스에서 df 형식의 데이터를 가지고 오는 방법
            ### 참고: 레디스에 df를 저장하는 방법은: redis.set(key, df.to_msgpack(compress='zlib'))와 같은 형식이다
        return data

    def set_index_lists(self):
        index_list = MARKET_CODES.keys()  # 인덱스 종류를 담은 리스트

        # 모든 인덱스 종류를 담은 리스트 만들기
        # 산업별 분류는 너무 많아서 나머지 리스트에서 없는 인덱스를 가져오는 방식으로 리스트 정의
        bm = ['코스피', '코스닥']
        size = ['코스피 대형주', '코스피 중형주', '코스피 소형주', '코스닥 대형주', '코스닥 중형주', '코스닥 소형주']
        style = ['성장주', '가치주', '배당주', '퀄리티주', '사회책임경영주']
        industry = [index for index in index_list if index not in bm and \
                         index not in size and \
                         index not in style]

        print('BM: ' + ' '.join(str(i) for i in bm))
        print('SIZE: ' + ' '.join(str(i) for i in size))
        print('STYLE: ' + ' '.join(str(i) for i in style))
        print('INDUSTRY: ' + ' '.join(str(i) for i in industry))
        return bm, size, style, industry


    def make_index_data(self, redis_client, index_list):
        index_data_dict = {}  # 딕셔너리 형식으로 저장한다
        for index in index_list:
            # 레디스 키값은 I.002_INDEX와 같은 형식이다
            index_key = MARKET_CODES[index] + DATA_MAPPER['index']  # MARKET_CODES 딕셔너리에서 코드를 찾아온다
            index_df = self.get_val(redis_client, index_key)
            index_data_dict[index] = index_df
        return index_data_dict

    def make_series_data(self, data, key, index, columns):
        series_df = data[key][[index, columns]]
        series_df.set_index(index, inplace=True)
        series_df.rename(columns={columns: key}, inplace=True)
        return series_df

    def make_refined_data(self):
        refined_ticker = []
        status_code = 200
        i = 0
        while True:
            i += 1
            req = requests.get(self.mkcap_url.format(i))
            status_code = req.status_code
            if status_code == 404:
                break
            mkcap_ticker = [r['code'] for r in req.json()['results']]
            refined_ticker += mkcap_ticker
        return refined_ticker

    def make_ticker_data(self, kp_tickers, kd_tickers):
        refined_ticker = self.make_refined_data()
        kp_tickers_dict = dict()
        kd_tickers_dict = dict()
        kp_tickers_list = [ticker.split('|')[0] for ticker in kp_tickers if ticker.split('|')[0] in refined_ticker]
        kd_tickers_list = [ticker.split('|')[0] for ticker in kd_tickers if ticker.split('|')[0] in refined_ticker]
        for ticker in kp_tickers:
            kp_tickers_dict[ticker.split('|')[0]] = ticker.split('|')[1]
        for ticker in kd_tickers:
            kd_tickers_dict[ticker.split('|')[0]] = ticker.split('|')[1]
        return kp_tickers_list, kd_tickers_list, kp_tickers_dict, kd_tickers_dict

    def merge_index_data(self):
        bm, size, style, industry = self.set_index_lists()

        bm_data = self.make_index_data(self.r, bm)
        size_data = self.make_index_data(self.r, size)
        style_data = self.make_index_data(self.r, style)
        industry_data = self.make_index_data(self.r, industry)

        index_keys = bm + size + style + industry

        make_data_start = True

        for key in index_keys:

            if key in bm:
                data = bm_data
            if key in size:
                data = size_data
            if key in style:
                data = style_data
            if key in industry:
                data = industry_data

            ohlcv_df = self.make_series_data(data, key, 'date', 'cls_prc')
            vol_df= self.make_series_data(data, key, 'date', 'trd_qty')

            if make_data_start:
                index_ohlcv = ohlcv_df
                index_vol = vol_df
                make_data_start = False
            else:
                index_ohlcv = pd.concat([index_ohlcv, ohlcv_df], axis=1)
                index_vol = pd.concat([index_vol, vol_df], axis=1)
        return index_ohlcv, index_vol

    def make_ohlcv_df(self, index_ohlcv, index_vol):
        # 거래대금 데이터 만들기
        kp_vol_prc = self.kp_ohlcv * self.kp_vol
        kd_vol_prc = self.kd_ohlcv * self.kd_vol
        index_vol_prc = index_ohlcv * index_vol
        # 수익률 데이터 만들기
        kp_ret = self.kp_ohlcv.pct_change()
        kd_ret = self.kd_ohlcv.pct_change()
        index_ret = index_ohlcv.pct_change()
        # 모든 OHLCV, Volume, Return 데이터를 하나로 묶는다
        ohlcv = pd.concat([self.kp_ohlcv, self.kd_ohlcv, index_ohlcv], axis=1)
        volume = pd.concat([self.kp_vol, self.kd_vol, index_vol], axis=1)
        vol_prc = pd.concat([kp_vol_prc, kd_vol_prc, index_vol_prc], axis=1)
        returns = pd.concat([kp_ret, kd_ret, index_ret], axis=1)
        return kp_vol_prc, kd_vol_prc, index_vol_prc, kp_ret, kd_ret, index_ret, ohlcv, volume, vol_prc, returns

    def set_periodic_ret(self, data, period='M'):
        ### 인자 설명:
        ### 1. ohlcv_df (pd.DataFrame)
        ### 2. period (str) --> W, M, Q, 6M, A
        ###                     일주일, 한달, 세달, 여섯달, 일년 주기 종가
        ###
        # 인자로 받은 데이터프레임 ohlcv_df의 인덱스를 데이트타임으로 바꿔준다
        data.index = pd.to_datetime(data.index)
        periodic_close = data.resample(period)
        return periodic_close

    def make_redis_ohlcv_df(self, mode, kp_tickers_list, kd_tickers_list):
        make_data_start = True
        if mode == 'kp':
            tickers_list = kp_tickers_list
        elif mode == 'kd':
            tickers_list =  kd_tickers_list
        else:
            print('choose kp or kd')
        for ticker in tickers_list:
            # OHLCV 데이터 불러오기
            key = ticker + '_OHLCV'
            ohlcv = pd.read_msgpack(r.get(key))
            ohlcv.set_index('date', inplace=True)
            ohlcv.index = pd.to_datetime(ohlcv.index)
            ohlcv_df = ohlcv[['adj_prc']]
            vol_df = ohlcv[['trd_qty']]
            ohlcv_df.rename({'adj_prc':ticker}, axis='columns', inplace=True)
            vol_df.rename({'trd_qty':ticker}, axis='columns', inplace=True)

            if make_data_start:
                total_ohlcv = ohlcv_df
                total_vol = vol_df
                make_data_start = False
            else:
                total_ohlcv = pd.concat([total_ohlcv, ohlcv_df], axis=1)
                total_vol = pd.concat([total_vol, vol_df], axis=1)
        return total_ohlcv, total_vol
