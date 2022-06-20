#!/usr/bin/env python
# encoding: utf-8
'''
@time: 2021/9/1 下午4:01
@author: bill (billevent107421@gmail.com)
@file: crawl_daily_finance_data.py
@type: 需求
@project: 爬取每日外幣匯率、台股股價、三大法人每日買賣超資料
@desc:
'''

import sys
sys.path.append(r"/Users/bill/Desktop/pythonProject/finance")
from db_tools import db_tools
# pandas套件內read_html函數所需的套件：html5lib
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from io import StringIO
import numpy as np
# 爬蟲使用
import requests
# 拆解網頁格式
from bs4 import BeautifulSoup
# 正規表答式模組
import re
# 爬蟲休息函數
import time
# 讀取json檔使用
import json

# 以下為爬取yahoo所需用到的套件注意重點 ===========================
# 如在使用pandas_datareader時遇到ImportError:cannot import name 'is_list_like'錯誤
# 所以需要先加上這行再import
# pd.core.common.is_list_like = pd.api.types.is_list_like
import pandas_datareader.data as web


class crawl_daily_finance_data():
    def __init__(self):
        # 爬取上市股票公司基本資料所需字典資料
        self.payload_twse = {
            'encodeURIComponent': '1',
            'step': '1',
            'firstin': '1',
            'TYPEK': 'sii',
            'code': ''
        }
        # 爬取上櫃股票公司基本資料所需字典資料
        self.payload_otc = {
            'encodeURIComponent': '1',
            'step': '1',
            'firstin': '1',
            'TYPEK': 'otc',
            'code': ''
        }
        # 合併爬取上市櫃公司基本資料所需字典資料
        self.payload_data = [self.payload_twse, self.payload_otc]

    # 爬取當年度的國定假日日期
    def get_holiday(self, year):
        url = 'http://www.stockq.org/taiwan/holiday' + str(year) + '.php'
        res = requests.get(url)
        res.encode = "utf-8"
        soup = BeautifulSoup(res.text, "html.parser")

        holiday_name = []
        holiday_start = []
        holiday_end = []
        holiday_day = []
        holiday_remarks = []
        # 第一個欄位名稱、第二個當年度元旦假期跟最後一個隔年元旦假期不解析，當年度不解析主要是因為怕起始日期是去年，不好判斷
        for holiday_soup in soup.select(".InfoContantTable tr")[2:-1]:
            # 假期的名稱
            holiday_name.append(holiday_soup.select("td")[0].text)
            # 因資料內含有星期，全部取代掉
            holiday_range = re.sub(r'[(一)(二)(三)(四)(五)(六)(日)]', '', holiday_soup.select("td")[1].text)
            start = str(year) + "/" + holiday_range.split('-')[0]
            start = datetime.datetime.strptime(start.replace('/', '-'), "%Y-%m-%d")
            end = str(year) + "/" + holiday_range.split('-')[1]
            end = datetime.datetime.strptime(end.replace('/', '-'), "%Y-%m-%d")
            holiday_start.append(start.strftime("%Y-%m-%d"))
            holiday_end.append(end.strftime("%Y-%m-%d"))
            # 假期的天數
            holiday_day.append(holiday_soup.select("td")[2].text)
            # 假期的備註
            holiday_remarks.append(holiday_soup.select("td")[3].text)

        result = pd.DataFrame({"holiday_name": holiday_name,
                               "holiday_start": holiday_start,
                               "holiday_end": holiday_end,
                               "holiday_day": holiday_day,
                               "holiday_remarks": holiday_remarks})

        result["holiday_year"] = str(year)
        result = result[["holiday_name", "holiday_year", "holiday_start", "holiday_end", "holiday_day", "holiday_remarks"]]

        return result

    # 計算每週的起始日及結束日，作為後續計算每週技術指標的日期範圍
    def get_week_date_range(self, date):
        if type(date) != datetime.date:
            today = datetime.datetime.strptime(date, "%Y-%m-%d")
        else:
            today = date
        # month = today.month
        # year = today.year
        # day = today.day
        weekday = today.weekday()

        start = today + datetime.timedelta(0 - weekday)
        end = today + datetime.timedelta(4 - weekday)

        # start = datetime.datetime(start.year, start.month, start.day)
        # end = datetime.datetime(end.year, end.month, end.day)

        return start, end

    # 爬取目前台股上市櫃個股的基本資料 ==========================================
    def crawl_stock_information(self):
        try:
            stock_information = pd.DataFrame()

            for payload in self.payload_data:
                date_res = requests.post("https://mops.twse.com.tw/mops/web/ajax_t51sb01", data=payload)
                stock_data = pd.read_html(date_res.text, flavor='html5lib')[4]

                if payload['TYPEK'] == 'sii':
                    # 選取所需欄位即可
                    stock_data = stock_data[['公司代號', '公司名稱', '公司簡稱', '英文簡稱', '產業類別', '成立日期', '上市日期',
                                             '實收資本額(元)', '已發行普通股數或TDR原發行股數', '私募普通股(股)', '特別股(股)',
                                             '公司網址']]

                    stock_data.rename(
                        columns={'公司代號': 'stock_code', '公司名稱': 'company_name', '公司簡稱': 'company_name_ch',
                                 '英文簡稱': 'company_name_en',
                                 '產業類別': 'industry', '成立日期': 'establishment_date', '上市日期': 'listed_date',
                                 '實收資本額(元)': 'company_capital',
                                 '已發行普通股數或TDR原發行股數': 'outstanding_principal', '私募普通股(股)': 'private_equity',
                                 '特別股(股)': 'preferred_stock', '公司網址': 'company_url'}, inplace=True)

                    stock_data = stock_data[stock_data['stock_code'] != '公司代號']

                    stock_data['stock_class'] = 'TWSE'
                    stock_data['stock_class_ch'] = '上市'
                else:
                    # 選取所需欄位即可
                    stock_data = stock_data[['公司代號', '公司名稱', '公司簡稱', '英文簡稱', '產業類別', '成立日期', '上櫃日期',
                                             '實收資本額(元)', '已發行普通股數或TDR原發行股數', '私募普通股(股)', '特別股(股)',
                                             '公司網址']]

                    stock_data.rename(
                        columns={'公司代號': 'stock_code', '公司名稱': 'company_name', '公司簡稱': 'company_name_ch',
                                 '英文簡稱': 'company_name_en',
                                 '產業類別': 'industry',
                                 '成立日期': 'establishment_date', '上櫃日期': 'listed_date', '實收資本額(元)': 'company_capital',
                                 '已發行普通股數或TDR原發行股數': 'outstanding_principal', '私募普通股(股)': 'private_equity',
                                 '特別股(股)': 'preferred_stock', '公司網址': 'company_url'}, inplace=True)

                    stock_data = stock_data[stock_data['stock_code'] != '公司代號']

                    stock_data['stock_class'] = 'OTC'
                    stock_data['stock_class_ch'] = '上櫃'

                # 合併上市櫃公司基本資料
                stock_information = pd.concat([stock_information, stock_data], ignore_index=True)

                # 修改日期格式中的/符號
                stock_information['establishment_date'] = stock_information['establishment_date'].str.replace('/', '-')
                stock_information['listed_date'] = stock_information['listed_date'].str.replace('/', '-')

                # 修改數值中的小數點
                stock_information['company_capital'] = stock_information['company_capital'].astype(int)
                stock_information['outstanding_principal'] = stock_information['outstanding_principal'].astype(int)
                stock_information['private_equity'] = stock_information['private_equity'].astype(int)
                stock_information['preferred_stock'] = stock_information['preferred_stock'].astype(int)
        except:
            stock_information = pd.DataFrame()

        return stock_information

    # 爬取yahoo股票價格 =================================================
    # 但這邊有問題，有的時候爬取下來的資料會有些天是沒有成交量的，那個股價資訊資料就是錯的
    def crawl_stock_price(self, stock_id, start, end):
        stock_price = pd.DataFrame()
        stock_error_id = []
        stock_id['stock_code'] = stock_id['stock_code'].astype(str)

        for id in stock_id['stock_code']:
            print(id)
            try:
                if stock_id.loc[stock_id['stock_code'] == str(id), 'stock_class'].iloc[0] == 'TWSE':
                    # 上市公司股價
                    stock_dr = web.get_data_yahoo(str(id) + '.TW', start, end)
                    stock_dr['stock_code'] = id
                    stock_dr.columns = ['high_price', 'low_price', 'open_price', 'close_price',
                                        'volume', 'adj_close_price', 'stock_code']
                    stock_price = stock_price.append(stock_dr, ignore_index=False)
                else:
                    # 上櫃公司股價
                    stock_dr = web.get_data_yahoo(str(id) + '.TWO', start, end)
                    stock_dr['stock_code'] = id
                    stock_dr.columns = ['high_price', 'low_price', 'open_price', 'close_price',
                                        'volume', 'adj_close_price', 'stock_code']
                    stock_price = stock_price.append(stock_dr, ignore_index=False)
            except:
                stock_error_id.append(id)

        # 去除重複資料
        stock_price.drop_duplicates(inplace=True)
        # 因日期在index欄，這邊重設index，把日期獨立成一個欄位
        stock_price = stock_price.reset_index()
        stock_price = stock_price.rename(columns={'Date': 'data_date'})
        # 日期因格式問題包含到時分秒，所以取出日期部分
        stock_price['data_date'] = stock_price['data_date'].astype(str).str.slice(0, 10)
        # 更改欄位順序
        stock_price = stock_price.reindex(columns=['stock_code', 'data_date', 'high_price',
                                                   'low_price', 'open_price', 'close_price',
                                                   'volume', 'adj_close_price'])

        return stock_price, stock_error_id

    # 爬取國際指數
    def crawl_internetional_index(self, start, end):
        stock_price = pd.DataFrame()
        stock_error_id = []
        # S&P500(^GSPC) : 又名「標準普爾500指數」，由500家美國上市公司所組成，這500家公司都來自於美國股市的兩大股票交易市場，紐約證券交易所和納斯達克
        # 道瓊工業平均指數(^DJI) : 包括美國最大、最知名的三十家上市公司，由每個組成公司的股票價格總和
        # 費城半導體指數(^SOX) : 是費城證交所市值加權指數，主要從事設計，銷售，製造，銷售的30家大公司組成的半導體
        # NASDAQ(^IXIC) : 又名「那斯達克綜合指數」，1971年2月5日創立，基數點為100點，其成分股包括所有於美國那斯達克上市的股份
        # 紐約證券交易所綜合指數(^XAX) : 包括在紐約股票交易所上市所有普通股的指數，旗下設有四個分類指數：工業、交通、公共事業及金融
        # 羅素2000指數(^RUT) : 羅素指數是由Frank Russell公司創立，以美國市場為基準，成分股都是美國公司，羅素2000指數是以羅素3000指數中最小的2000家公司股票來編制的，因此這個指數主要是用來反映美國中小企業的狀況，是投資美國小型股的重要指數
        # 泛歐交易所(^N100) : 由荷蘭阿姆斯特丹證券交易所、法國巴黎證券交易所、比利時布魯塞爾證券交易所、葡萄牙里斯本證券交易所、英國倫敦國際金融期貨交易所合併成立，是歐洲最大的證券交易所
        # 日經225(^N225) : 全名為日本股市經濟平均指數，是由日本經濟新聞推出的東京證券交易所的225種類的股價指數
        # 恆生指數(^HSI) : 反映香港股市行情的重要指標，指數由五十隻恒指成份股的市值計算出來的，代表了香港交易所所有上市公司的十二個月平均市值涵蓋率的63%
        # 上證綜合指數(000001.SS) : 上海證券交易所主要的綜合股價指數，為一種市值加權指數，是反應掛牌股票總體走勢的統計指標
        # 韓國綜合股價指數(^ KS11) : 簡稱為KOSPI，是韓國交易所的股票指數。指數由所有在交易所內交易的股票價格來計算，並以1980年1月4日作為指數的基準起始日，當日股市的開市價作為100點的基準
        # 台灣加權指數(^ TWII) : 由臺灣證券交易所所編製的股價指數，是台灣最為人熟悉的股票指數，被視為是呈現台灣經濟走向的櫥窗
        stock_id = ['^GSPC', '^DJI', '^SOX', '^IXIC', '^XAX', '^RUT', '^N100', '^N225', '^HSI', '000001.SS', '^KS11',
                    '^TWII']

        for id in stock_id:
            try:
                # 國際指數
                stock_dr = web.get_data_yahoo(id, start, end)
                stock_dr['stock_code'] = id[1:]
                stock_dr.columns = ['high_price', 'low_price', 'open_price', 'close_price',
                                    'volume', 'adj_close_price', 'stock_code']
                stock_price = stock_price.append(stock_dr, ignore_index=False)
            except:
                stock_error_id.append(id)

        # 去除重複資料
        stock_price.drop_duplicates(inplace=True)
        # 因日期在index欄，這邊重設index，把日期獨立成一個欄位
        stock_price = stock_price.reset_index()
        stock_price = stock_price.rename(columns={'Date': 'data_date'})
        # 日期因格式問題包含到時分秒，所以取出日期部分
        stock_price['data_date'] = stock_price['data_date'].astype(str).str.slice(0, 10)
        # 更改欄位順序
        stock_price = stock_price.reindex(columns=['stock_code', 'data_date', 'high_price',
                                                   'low_price', 'open_price', 'close_price',
                                                   'volume', 'adj_close_price'])

        return stock_price, stock_error_id

    # 爬取上市公司三大法人每日買賣超股數 ==============================================
    def institutional_investors_twse_listed(self, date):
        date = datetime.datetime.strptime(date, "%Y-%m-%d")

        # 將時間物件變成字串：'20180102'
        datestr = date.strftime('%Y%m%d')

        # 下載三大法人資料
        try:
            # 取得資料
            r = requests.get('http://www.twse.com.tw/fund/T86?response=csv&date=' + datestr + '&selectType=ALLBUT0999')

            # 製作三大法人的DataFrame
            stock_buy_sell = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')

            # 刪除逗點
            stock_buy_sell = stock_buy_sell.astype(str).apply(lambda s: s.str.replace(',', ''))

            # 刪除「證券代號」中的「"」和「=」，並令成stock_id這個欄位
            stock_buy_sell['stock_id'] = stock_buy_sell['證券代號'].str.replace('=', '').str.replace('"', '')

            # 刪除原本「證券代號」這個欄位
            stock_buy_sell = stock_buy_sell.drop(['證券代號'], axis=1)

            stock_buy_sell = stock_buy_sell[['stock_id', '外陸資買進股數(不含外資自營商)', '外陸資賣出股數(不含外資自營商)',
                                             '外陸資買賣超股數(不含外資自營商)', '外資自營商買進股數', '外資自營商賣出股數',
                                             '外資自營商買賣超股數', '投信買進股數', '投信賣出股數', '投信買賣超股數',
                                             '自營商買進股數(自行買賣)', '自營商賣出股數(自行買賣)', '自營商買賣超股數(自行買賣)',
                                             '自營商買進股數(避險)', '自營商賣出股數(避險)', '自營商買賣超股數(避險)',
                                             '三大法人買賣超股數']]

            stock_buy_sell.columns = ['stock_code', 'foreign_investor_buy_amount', 'foreign_investor_sell_amount',
                                      'foreign_investor_dealer_vol_amount', 'foreign_investor_dealer_buy_amount',
                                      'foreign_investor_dealer_sell_amount', 'foreign_investor_vol_amount',
                                      'investment_trust_buy_amount', 'investment_trust_sell_amount',
                                      'investment_trust_vol_amount', 'dealer_buy_amount', 'dealer_sell_amount',
                                      'dealer_vol_amount', 'dealer_hedge_buy_amount', 'dealer_hedge_sell_amount',
                                      'dealer_hedge_vol_amount', 'institutional_investors_vol_amount']

            stock_buy_sell['data_date'] = date

            # 將dataframe的型態轉成數字
            # stock_buy_sell = stock_buy_sell.apply(lambda s: pd.to_numeric(s, errors='coerce')).dropna(how='all', axis=1)

        except:
            stock_buy_sell = pd.DataFrame()

        return stock_buy_sell

    # 爬取上櫃公司三大法人每日買賣超股數 =================================================
    def institutional_investors_otc_listed(self, date):
        date = datetime.datetime.strptime(date, "%Y-%m-%d")

        # 因網址資料是用民國年，所以要減掉1911
        date_y = str(date.year - 1911)
        date_m = '0' + str(date.month) if date.month < 10 else str(date.month)
        date_d = '0' + str(date.day) if date.day < 10 else str(date.day)

        link = f'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d={date_y}/{date_m}/{date_d}&s=0,asc'
        try:
            r = requests.get(link)
            lines = r.text.split('\r\n')
            lines = lines[1:]

            df_data = []
            for row in lines:
                if row == lines[0]:
                    df_colname = row.split(',')
                else:
                    if len(row) > 0:
                        row_data = row.split('","')
                        new_row_data = []
                        for ele in row_data:
                            ele = ele.replace('"', '')
                            ele = ele.replace(',', '')
                            new_row_data.append(ele)
                        df_data.append(new_row_data)

            stock_buy_sell = pd.DataFrame(df_data, columns=df_colname)
            stock_buy_sell = stock_buy_sell[['代號', '外資及陸資(不含外資自營商)-買進股數', '外資及陸資(不含外資自營商)-賣出股數',
                                             '外資及陸資(不含外資自營商)-買賣超股數', '外資自營商-買進股數', '外資自營商-賣出股數',
                                             '外資自營商-買賣超股數', '投信-買進股數', '投信-賣出股數', '投信-買賣超股數',
                                             '自營商(自行買賣)-買進股數', '自營商(自行買賣)-賣出股數', '自營商(自行買賣)-買賣超股數',
                                             '自營商(避險)-買進股數', '自營商(避險)-賣出股數', '自營商(避險)-買賣超股數', '三大法人買賣超股數合計']]

            stock_buy_sell.columns = ['stock_code', 'foreign_investor_buy_amount', 'foreign_investor_sell_amount',
                                      'foreign_investor_dealer_vol_amount', 'foreign_investor_dealer_buy_amount',
                                      'foreign_investor_dealer_sell_amount', 'foreign_investor_vol_amount',
                                      'investment_trust_buy_amount', 'investment_trust_sell_amount',
                                      'investment_trust_vol_amount',
                                      'dealer_buy_amount', 'dealer_sell_amount', 'dealer_vol_amount',
                                      'dealer_hedge_buy_amount',
                                      'dealer_hedge_sell_amount', 'dealer_hedge_vol_amount',
                                      'institutional_investors_vol_amount']

            stock_buy_sell['data_date'] = date.strftime("%Y-%m-%d")

        except:
            stock_buy_sell = pd.DataFrame()

        return stock_buy_sell

    # 爬取上市市場三大法人每日買賣超金額
    def institutional_investors_twse_listed_money(self, date):
        date = datetime.datetime.strptime(date, "%Y-%m-%d")

        # 將時間物件變成字串：'20180102'
        datestr = date.strftime('%Y%m%d')

        try:
            # 取得資料
            r = requests.get(f"https://www.twse.com.tw/fund/BFI82U?response=csv&dayDate={datestr}&type=day")

            # 製作三大法人的DataFrame
            stock_buy_sell_money = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')
            stock_buy_sell_money = stock_buy_sell_money.T.reset_index()
            stock_buy_sell_money.columns = ['buy_sell_type_ch', 'dealer', 'dealer_hedge', 'investment_trust',
                                            'foreign_investor', 'foreign_investor_dealer', 'buy_sell_diff_total']
            # 刪除第一列資料
            stock_buy_sell_money = stock_buy_sell_money.drop(stock_buy_sell_money.index[0])
            # 衍生另一個欄位將中文字種類轉成英文種類
            stock_buy_sell_money['buy_sell_type'] = stock_buy_sell_money['buy_sell_type_ch'].apply(
                lambda x: 'buy_money_amount' if x == '買進金額' else (
                    'sell_money_amount' if x == '賣出金額' else 'diff_money_amount'))

            # 刪除逗點
            stock_buy_sell_money = stock_buy_sell_money.astype(str).apply(lambda s: s.str.replace(',', ''))
            stock_buy_sell_money['data_date'] = date.strftime("%Y-%m-%d")
            stock_buy_sell_money['stock_class'] = 'TWSE'

            stock_buy_sell_money = stock_buy_sell_money[
                ['data_date', 'stock_class', 'buy_sell_type', 'buy_sell_type_ch',
                 'dealer', 'dealer_hedge', 'investment_trust', 'foreign_investor',
                 'foreign_investor_dealer', 'buy_sell_diff_total']]
        except:
            stock_buy_sell_money = pd.DataFrame()

        return stock_buy_sell_money

    # 爬取上櫃市場三大法人每日買賣超金額
    def institutional_investors_otc_listed_money(self, date):
        date = datetime.datetime.strptime(date, "%Y-%m-%d")

        # 因網址資料是用民國年，所以要減掉1911
        date_y = str(date.year - 1911)
        date_m = '0' + str(date.month) if date.month < 10 else str(date.month)
        date_d = '0' + str(date.day) if date.day < 10 else str(date.day)

        try:
            r = requests.get(
                f"https://www.tpex.org.tw/web/stock/3insti/3insti_summary/3itrdsum_download.php?l=zh-tw&t=D&p=1&d={date_y}/{date_m}/{date_d}")

            # 製作三大法人的DataFrame
            stock_buy_sell_money = pd.read_csv(StringIO(r.text), header=1).dropna(how='all', axis=1).dropna(how='any')
            stock_buy_sell_money = stock_buy_sell_money.T.reset_index()

            # 刪除第二、六個欄位
            stock_buy_sell_money = stock_buy_sell_money.drop([0, 4], axis=1)
            # 刪除第一列資料
            stock_buy_sell_money = stock_buy_sell_money.drop(stock_buy_sell_money.index[0])
            stock_buy_sell_money.columns = ['buy_sell_type_ch', 'foreign_investor', 'foreign_investor_dealer',
                                            'investment_trust', 'dealer', 'dealer_hedge', 'buy_sell_diff_total']

            # 刪除（元）這個單位
            stock_buy_sell_money['buy_sell_type_ch'] = stock_buy_sell_money['buy_sell_type_ch'].apply(
                lambda x: x.replace('(元)', ''))
            # 將上櫃種類取代與上市同一種類
            stock_buy_sell_money['buy_sell_type_ch'] = stock_buy_sell_money['buy_sell_type_ch'].apply(
                lambda x: x.replace('買賣超', '買賣差額'))
            # 衍生另一個欄位將中文字種類轉成英文種類
            stock_buy_sell_money['buy_sell_type'] = stock_buy_sell_money['buy_sell_type_ch'].apply(
                lambda x: 'buy_money_amount' if x == '買進金額' else (
                    'sell_money_amount' if x == '賣出金額' else 'diff_money_amount'))

            # 刪除逗點
            stock_buy_sell_money = stock_buy_sell_money.astype(str).apply(lambda s: s.str.replace(',', ''))
            stock_buy_sell_money['data_date'] = date.strftime("%Y-%m-%d")
            stock_buy_sell_money['stock_class'] = 'OTC'

            stock_buy_sell_money = stock_buy_sell_money[
                ['data_date', 'stock_class', 'buy_sell_type', 'buy_sell_type_ch',
                 'dealer', 'dealer_hedge', 'investment_trust', 'foreign_investor',
                 'foreign_investor_dealer', 'buy_sell_diff_total']]
        except:
            stock_buy_sell_money = pd.DataFrame()

        return stock_buy_sell_money

    # 從台銀下載每日最新匯率資料 ==============================================
    def daily_currency(self, date):
        #try:
        # 若是在執行時碰到ImportError: lxml not found, please install it此錯誤，但是明明已經有安裝且可正常import，那就在read_html內指定html5lib
        dfs = pd.read_html('https://rate.bot.com.tw/xrt?lang=zh-TW', flavor='html5lib')
        currency = dfs[0]
        currency = currency.iloc[:, 0:5]
        currency.columns = ['currency_type', 'bank_note_buying_rate', 'bank_note_selling_rate', 'spot_buying_rate',
                            'spot_selling_rate']
        currency['currency_type'] = currency['currency_type'].str.extract('\((\w+)\)')
        currency['data_date'] = date
        currency = currency.replace('-', np.nan)
        # except:
        #     currency = pd.DataFrame()

        return currency

    # 統計每週的股價資料
    def price_stat_week(self, start, end):
        sql = f"select * \
            from stock_price \
            where data_date between '{start}' and '{end}' \
            order by data_date"

        result = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)

        # 計算股價日期為當年的第幾週
        result['week_num'] = result['data_date'].apply(lambda x: x.isocalendar()[1])
        # 找出當週的起始日期
        result['week_start_date'] = result['data_date'].apply(lambda x: self.get_week_date_range(x)[0])
        # 找出當週的結束日期
        result['week_end_date'] = result['data_date'].apply(lambda x: self.get_week_date_range(x)[1])
        # 歸類年份，若week_num週數為1的話以週結束的日期做為歸類年份的基準，若week_num不為1就以周起始的日期做為歸類年份的基準
        result['data_year'] = result.apply(lambda x: x['week_end_date'].strftime("%Y") if x['week_num'] == 1 else x['week_start_date'].strftime("%Y"), axis=1)

        # 計算開盤價
        stat_open = result.groupby(
            ['stock_code', 'data_year', 'week_num', 'week_start_date', 'week_end_date']).first().reset_index()
        stat_open = stat_open[['stock_code', 'data_year', 'week_num', 'week_start_date', 'week_end_date', 'open_price']]

        # 計算收盤價
        stat_close = result.groupby(['stock_code', 'data_year', 'week_num', 'week_start_date', 'week_end_date']).last().reset_index()
        stat_close = stat_close[['stock_code', 'data_year', 'week_num', 'week_start_date', 'week_end_date', 'close_price']]

        # 計算最高價
        stat_high = result.groupby(['stock_code', 'data_year', 'week_num', 'week_start_date', 'week_end_date']).agg(
            high_price=('high_price', 'max')).reset_index()

        # 計算最低價
        stat_low = result.groupby(['stock_code', 'data_year', 'week_num', 'week_start_date', 'week_end_date']).agg(
            low_price=('low_price', 'min')).reset_index()

        # 計算成交量
        stat_vol = result.groupby(['stock_code', 'data_year', 'week_num', 'week_start_date', 'week_end_date']).agg(
            volume=('volume', 'sum')).reset_index()

        result = pd.merge(stat_high, stat_low, on=['stock_code', 'data_year', 'week_num', 'week_start_date',
                                                   'week_end_date'], how='left')

        result = pd.merge(result, stat_open, on=['stock_code', 'data_year', 'week_num', 'week_start_date',
                                                 'week_end_date'], how='left')

        result = pd.merge(result, stat_close, on=['stock_code', 'data_year', 'week_num', 'week_start_date',
                                                  'week_end_date'], how='left')

        result = pd.merge(result, stat_vol, on=['stock_code', 'data_year', 'week_num', 'week_start_date',
                                                'week_end_date'], how='left')

        return result

    def price_stat_month(self, data_year_month):
        # 從資料庫內取出原本的個股基本資料
        sql = f"select a.*, b.data_year_month, b.month_start_date, b.month_end_date \
                from ( \
                    select * \
                    from stock_price \
                    where substr(data_date, 1, 7) = '{data_year_month}' \
                ) as a \
                left join ( \
                    select stock_code \
                    , substr(data_date, 1, 7) as data_year_month \
                    , min(data_date) as month_start_date \
                    , max(data_date) as month_end_date \
                    from stock_price \
                    where substr(data_date, 1, 7) = '{data_year_month}' \
                    group by stock_code, substr(data_date, 1, 7) \
                ) as b \
                on a.stock_code = b.stock_code"

        result = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)

        # 計算開盤價
        stat_open = result.groupby(['stock_code', 'data_year_month', 'month_start_date', 'month_end_date']).first().reset_index()
        stat_open = stat_open[['stock_code', 'data_year_month', 'month_start_date', 'month_end_date', 'open_price']]

        # 計算收盤價
        stat_close = result.groupby(['stock_code', 'data_year_month', 'month_start_date', 'month_end_date']).last().reset_index()
        stat_close = stat_close[['stock_code', 'data_year_month', 'month_start_date', 'month_end_date', 'close_price']]

        # 計算最高價
        stat_high = result.groupby(['stock_code', 'data_year_month', 'month_start_date', 'month_end_date']).agg(
            high_price=('high_price', 'max')).reset_index()

        # 計算最低價
        stat_low = result.groupby(['stock_code', 'data_year_month', 'month_start_date', 'month_end_date']).agg(
            low_price=('low_price', 'min')).reset_index()

        # 計算成交量
        stat_vol = result.groupby(['stock_code', 'data_year_month', 'month_start_date', 'month_end_date']).agg(
            volume=('volume', 'sum')).reset_index()

        result = pd.merge(stat_high, stat_low, on=['stock_code', 'data_year_month', 'month_start_date', 'month_end_date'], how='left')

        result = pd.merge(result, stat_open, on=['stock_code', 'data_year_month', 'month_start_date', 'month_end_date'], how='left')

        result = pd.merge(result, stat_close, on=['stock_code', 'data_year_month', 'month_start_date', 'month_end_date'], how='left')

        result = pd.merge(result, stat_vol, on=['stock_code', 'data_year_month', 'month_start_date', 'month_end_date'], how='left')

        return result


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('crawl_daily_finance_data.py')
    print('程式開始執行時間：' + start_time.strftime("%Y-%m-%d %H:%M:%S"))

    # 將函數令成變數做後續使用
    crawl_daily_finance_data = crawl_daily_finance_data()

    # 爬取每日最新外幣匯率資料 ==========================================================
    date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    date = date.strftime("%Y-%m-%d")

    currency = crawl_daily_finance_data.daily_currency(date=date)

    # 將爬取的資料寫入資料庫
    insert_currency = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
                                                            sql_insert_data=currency,
                                                            db_table='foreign_exchange_rates')

    print('爬取每日最新外幣匯率於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
    print('匯入資料庫總筆數：', str(len(currency)) + '筆')

    # 每年1/5爬取當年的國定假日，後續可供其他程式判斷是否日期為假日 ============================
    if date[5:10] == '01-05':
        holiday_data = crawl_daily_finance_data.get_holiday(year=date[0:4])

        # 將爬取的資料寫入資料庫
        insert_currency = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
                                                                sql_insert_data=holiday_data,
                                                                db_table='holiday_data')

        print('爬取每年度國定假期於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
        print('匯入資料庫總筆數：', str(len(holiday_data)) + '筆')

    # 爬取個股基本資料（每週五更新一次）、個股股價（每週一～五更新）、 爬取上市(櫃)公司三大法人每日買賣超=======================================================
    if datetime.datetime.now().weekday() in (0, 1, 2, 3, 4):
        # 爬取上市(櫃)公司三大法人每日買賣超股數 ===============================================
        # 設定資料日期
        date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        date = date.strftime("%Y-%m-%d")

        # 爬取上市公司資料
        stock_twse_buy_sell = crawl_daily_finance_data.institutional_investors_twse_listed(date=date)

        # 爬取上櫃公司資料
        stock_otc_buy_sell = crawl_daily_finance_data.institutional_investors_otc_listed(date=date)

        # 合併爬取的上市櫃公司資料
        stock_buy_sell = pd.concat([stock_twse_buy_sell, stock_otc_buy_sell], axis=0)

        # 將爬取的資料寫入資料庫
        insert_stock_buy_sell = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
                                                                      sql_insert_data=stock_buy_sell,
                                                                      db_table='stock_institutional_investors')

        print('爬取上市（櫃）公司三大法人每日買賣超於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
        print('上市公司匯入資料庫總筆數：', str(len(stock_twse_buy_sell)) + '筆')
        print('上櫃公司匯入資料庫總筆數：', str(len(stock_otc_buy_sell)) + '筆')

        # 爬取上市買賣超金額資料
        stock_twse_buy_sell_money = crawl_daily_finance_data.institutional_investors_twse_listed_money(date=date)

        # 爬取上櫃買賣超金額資料
        stock_otc_buy_sell_money = crawl_daily_finance_data.institutional_investors_otc_listed_money(date=date)

        # 合併爬取的上市櫃買賣超金額資料
        stock_buy_sell_money = pd.concat([stock_twse_buy_sell_money, stock_otc_buy_sell_money], axis=0)

        # 將爬取的資料寫入資料庫
        insert_stock_buy_sell_money = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
                                                                            sql_insert_data=stock_buy_sell_money,
                                                                            db_table='stock_institutional_investors_money')

        print('爬取上市（櫃）三大法人每日買賣超金額於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
        print('匯入資料庫總筆數：', str(len(stock_buy_sell_money)) + '筆')

    # if datetime.datetime.now().weekday() == 5:
    #     # 爬取股價資料（每週一～五更新）=================================================
    #     # 讀檔案方法
    #     # os.getcwd()  # 取得目前讀取路徑
    #     # os.chdir("C:/Users/bill/Desktop/")  # 切換讀取路徑
    #     # stock_id = pd.read_csv('股票代碼1.csv', engine='python')
    #
    #     # 從資料庫內取出股票ID
    #     sql = "select distinct stock_code, stock_class from stock_information where stock_status = '1'"
    #     stock_id = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)
    #
    #     # 設定爬蟲時間（雖然股價資料禮拜一～五才有，但這邊設定時間可以長一些，設定為上週日～本週六）
    #     start_date = datetime.datetime.now() - datetime.timedelta(6)
    #     # start_date = datetime.datetime.now()
    #     end_date = datetime.datetime.now()
    #     start_date = start_date.strftime("%Y-%m-%d")
    #     end_date = end_date.strftime("%Y-%m-%d")
    #
    #     # 開始爬取股價資料
    #     stock_crawl_price_data = crawl_daily_finance_data.crawl_stock_price(stock_id, start=start_date, end=end_date)
    #     stock_price = stock_crawl_price_data[0]
    #     stock_error_code = stock_crawl_price_data[1]
    #
    #     # 將爬取的股價資料寫入資料庫
    #     insert_stock_price = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
    #                                                                sql_insert_data=stock_price,
    #                                                                db_table='stock_price')
    #
    #     print('爬取個股股價於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
    #     print('爬個股股價時錯誤股價代號：' + str(stock_error_code))
    #     print('匯入資料庫總筆數：', str(len(stock_price)) + '筆')
        #
        # if datetime.datetime.now().weekday() == 4:
        #     # 爬取個股基本資料 =============================================
        #     # 從資料庫內取出原本的個股基本資料
        #     sql = "select * from stock_information"
        #     stock_information_old = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)
        #
        #     # 爬取最新的個股基本資料
        #     stock_information_new = crawl_daily_finance_data.crawl_stock_information()
        #
        #     # 合併新舊基本資料，去除重複後計算個股出現的次數
        #     stock_disitnct = stock_information_old.drop(['update_date'], axis=1)
        #     stock_disitnct = pd.concat([stock_disitnct, stock_information_new], axis=0)
        #     stock_disitnct.drop_duplicates(inplace=True)
        #     stock_disitnct_num = stock_disitnct.groupby(['stock_code']).agg(
        #         stock_code_num=('stock_code', 'count')).reset_index()
        #
        #     # 接著找出個股ID > 1的情況，這些個股代表新舊資料有變動，所以將這些個股舊的基本資料併到新的，再寫入資料庫
        #     stock_diff_code = stock_disitnct_num[stock_disitnct_num['stock_code_num'] > 1][['stock_code']]
        #     stock_information_old = stock_information_old[
        #         stock_information_old['stock_code'].isin(stock_diff_code['stock_code'])]
        #     stock_information_new['update_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        #     stock_information_new['stock_status'] = '1'
        #     stock_information_new = pd.concat([stock_information_old, stock_information_new], axis=0)
        #     stock_information_new.loc[
        #         stock_information_new['stock_status'].isin(stock_error_code), 'stock_status'] = '0'
        #
        #     # 先將原本的基本資料刪除
        #     stock_information_truncate = db_tools('db_invest').sql_delete_data(dbs_name='Investment',
        #                                                                        delete_type='truncate',
        #                                                                        db_table='stock_information',
        #                                                                        date='')
        #     print(stock_information_truncate)
        #
        #     insert_stock_information = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
        #                                                                      sql_insert_data=stock_information_new,
        #                                                                      db_table='stock_information')
        #
        #     print('爬取個股基本資料於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
        #     print('匯入資料庫總筆數：', str(len(stock_information_new)) + '筆')

    if datetime.datetime.now().weekday() == 6:
        # 爬取國際指數（因為時差問題，所以每週日更新）====================================================
        # 設定爬蟲時間（雖然股價資料禮拜一～五才有，但這邊設定時間可以長一些，設定為上週日～本週日避免時差問題）
        start_date = datetime.datetime.now() - datetime.timedelta(7)
        end_date = datetime.datetime.now()
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")

        # 開始爬取指數資料
        stock_crawl_index_data = crawl_daily_finance_data.crawl_internetional_index(start=start_date, end=end_date)
        stock_index = stock_crawl_index_data[0]
        stock_error_code = stock_crawl_index_data[1]

        # 將爬取的股價資料寫入資料庫
        insert_stock_price = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
                                                                   sql_insert_data=stock_index,
                                                                   db_table='stock_price')

        print('爬取國際指數於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
        print('爬時國際指數錯誤代號：' + str(stock_error_code))
        print('匯入資料庫總筆數：', str(len(stock_index)) + '筆')

        # # 計算每週個股各個統計資訊（週開盤、週收盤...等）====================================
        # # 設定資料日期
        # date = datetime.datetime.now().strftime("%Y-%m-%d")
        # start_date = crawl_daily_finance_data.get_week_date_range(date)[0]
        # end_date = crawl_daily_finance_data.get_week_date_range(date)[1]
        #
        # price_stat_week_data = crawl_daily_finance_data.price_stat_week(start=start_date, end=end_date)
        # # 再將資料寫入
        # insert_real_time_price = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
        #                                                                sql_insert_data=price_stat_week_data,
        #                                                                db_table='stock_price_week')
        #
        # print(date + '週均價統計於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
        # print('匯入資料庫總筆數：', str(len(price_stat_week_data)) + '筆')

    # if datetime.datetime.now().strftime("%d") == '01':
    #     # 設定資料日期
    #     date_month = datetime.datetime.now() - relativedelta(months=1)
    #     date_month = date_month.strftime("%Y-%m")
    #
    #     price_stat_month_data = crawl_daily_finance_data.price_stat_month(data_year_month=date_month)
    #
    #     # 再將資料寫入
    #     insert_real_time_price = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
    #                                                                    sql_insert_data=price_stat_month_data,
    #                                                                    db_table='stock_price_month')
    #
    #     print(date + '月均價統計於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
    #     print('匯入資料庫總筆數：', str(len(price_stat_month_data)) + '筆')

    end_time = datetime.datetime.now()
    print('程式執行完成時間' + end_time.strftime("%Y-%m-%d %H:%M:%S"))
    print('程式總執行時間：' + str((end_time - start_time).seconds) + '秒')
    print('===========================================================')



