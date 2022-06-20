#!/usr/bin/env python
# encoding: utf-8
'''
@time: 2021/10/16 下午4:01
@author: bill (billevent107421@gmail.com)
@file: crawl_monthly_revenue.py
@type: 需求
@project: 爬取個股每月公布營收資訊
@desc:
'''

import sys, requests, datetime, tempfile, zipfile, time
sys.path.append(r"/Users/bill/Desktop/pythonProject/finance")
from db_tools import db_tools
import pandas as pd
import numpy as np
from io import StringIO

# 爬取營收網址從以下網址來的：https://mops.twse.com.tw/mops/web/t21sc04_ifrs
def get_monthly_revenue(year, month):
    revenue_year_month = str(year) + '-' + '0' + str(month)

    # 假如是西元，轉成民國
    if year > 1990:
        year -= 1911

    url_twse = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_' + str(year) + '_' + str(month) + '_0.html'
    url_otc = 'https://mops.twse.com.tw/nas/t21/otc/t21sc03_' + str(year) + '_' + str(month) + '_0.html'
    if year <= 98:
        url_twse = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_' + str(year) + '_' + str(month) + '.html'
        url_otc = 'https://mops.twse.com.tw/nas/t21/otc/t21sc03_' + str(year) + '_' + str(month) + '.html'

    url_list = [url_twse, url_otc]
    # 偽裝瀏覽器資訊
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    result = pd.DataFrame()
    for url in url_list:
        # 下載該年月的網站，並用pandas轉換成 dataframe
        r = requests.get(url, headers=headers)
        r.encoding = 'big5'

        dfs = pd.read_html(StringIO(r.text), encoding='big-5')

        # 因dfs是聚集了各個產業的營收dataframe的list，所以我們利用for迴圈將其一個一個取出並合併在一起
        df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])

        # dir是
        if 'levels' in dir(df.columns):
            df.columns = df.columns.get_level_values(1)
        else:
            df = df[list(range(0, 10))]
            column_index = df.index[(df[0] == '公司代號')][0]
            df.columns = df.iloc[column_index]

        # coerce是
        df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
        df = df[~df['當月營收'].isnull()]
        df = df[df['公司代號'] != '合計']

        result = pd.concat([result, df])

        # 偽停頓
        time.sleep(5)

    # 因合併資料後產生了多層欄位的index，此部分我們先將其清除
    result.columns = result.columns.droplevel()
    # 將欄位重新命名
    result.rename(columns={'公司代號': 'stock_code', '公司名稱': 'company_name_ch', '備註': 'remark',
                           '上月比較增減(%)': 'last_month_revenue_change_ratio', '上月營收': 'last_month_gross_revenue',
                           '去年同月增減(%)': 'last_year_revenue_change_ratio', '去年當月營收': 'last_year_gross_revenue',
                           '當月營收': 'gross_revenue', '前期比較增減(%)': 'prophase_revenue_change_ratio',
                           '去年累計營收': 'last_year_accumulation_revenue', '當月累計營收': 'accumulation_revenue'}, inplace=True)
    result['revenue_year_month'] = revenue_year_month
    # 調整欄位順序
    result = result[['stock_code', 'company_name_ch', 'revenue_year_month', 'last_month_revenue_change_ratio',
                     'last_month_gross_revenue', 'last_year_revenue_change_ratio', 'last_year_gross_revenue',
                     'gross_revenue', 'prophase_revenue_change_ratio', 'last_year_accumulation_revenue',
                     'accumulation_revenue']]

    return result
	
if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('crawl_stock_fundamental_data.py')
    print('程式開始執行時間：' + start_time.strftime("%Y-%m-%d %H:%M:%S"))

    # 設定爬蟲時間
    data_date = datetime.datetime.now()
    data_date = data_date.strftime("%Y-%m-%d")
	
	# 要想一下爬取每月營收的程式執行時間點