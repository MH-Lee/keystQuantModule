from cache import RedisClient
from bs4 import BeautifulSoup
import pandas as pd


r = RedisClient()
ETF_CHECER_URL = 'https://comp.wisereport.co.kr/ETF/lookup.aspx'
ETF_ETN_DF = pd.read_html(ETF_CHECER_URL, encoding='utf-8')[0]
ETN_LIST = ETF_ETN_DF[ETF_ETN_DF['구분']=='ETN']['종목코드'].tolist()
ETN_LIST = ['ETN_LIST']+ETN_LIST
r.set_list(ETN_LIST)

r.get_list('ETN_LIST')
