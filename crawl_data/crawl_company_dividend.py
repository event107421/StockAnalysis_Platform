#!/usr/bin/env python
# encoding: utf-8
'''
@time: 2022/4/16 下午4:01
@author: bill (billevent107421@gmail.com)
@file: crawl_company_dividend.py
@type: 需求
@project: 爬取個股每年度股利分派資料、每季財報相關資料，如營收、每季綜合損益表、每季資產負債表、每季現金流量表等資料
@desc:
'''

import sys, requests, datetime, tempfile, zipfile, time
sys.path.append(r"/Users/bill/Desktop/pythonProject/finance")
from db_tools import db_tools
import pandas as pd
import numpy as np
from io import StringIO

# 爬取公司每年度股利分派資料：https://mops.twse.com.tw/mops/web/t05st09_new
def get_company_dividend(year):
    url = 'https://mops.twse.com.tw/server-java/t05st09sub'
    # 預設網頁post傳送的內容
    payload_twse = {
        'step': '1',
        'TYPEK': 'sii',
        'YEAR': str(year),
        'first': '',
        'qryType': '2'
    }
    payload_otc = {
        'step': '1',
        'TYPEK': 'otc',
        'YEAR': str(year),
        'first': '',
        'qryType': '2'
    }

    # 合併爬取上市櫃公司基本資料所需字典資料
    payload_data = [payload_twse, payload_otc]

    company_dividend = pd.DataFrame()
    for payload in payload_data:
        res = requests.post(url, data=payload)
        # 因網頁原始碼設定為big5，所以要進行轉碼，不然顯示會亂碼
        res.encoding = 'big5'
        df = pd.read_html(res.text, flavor='html5lib')

        type_dividend = pd.DataFrame()
        # 合併股利的資料
        for data in df:
            if '公司代號' in data.columns:
                # 因爬下來的資料有多層欄位名稱，刪除第一層的欄位名稱
                data.columns = data.columns.droplevel()
                data = data.drop(['決議（擬議）進度', '期別', '股利所屬 期間', '股東會 日期', '期初未分配盈餘/待彌補虧損(元)',
                                  '可分配 盈餘(元)', '本期淨利(淨損)(元)', '分配後期末未分配盈餘(元)', '摘錄公司章程-股利分派部分'], axis=1)
                data.columns = ['stock_code', 'dividend_year', 'dividend_date', 'cash_dividend',
                                'capital_reserve_cash_dividend_1', 'capital_reserve_cash_dividend_2',
                                'cash_dividend_total', 'stock_dividend', 'capital_reserve_stock_dividend_1',
                                'capital_reserve_stock_dividend_2', 'stock_dividend_total', 'remarks']
                # 只保留代碼即可，中文部分直接串個股基本資料表
                data['stock_code'] = data['stock_code'].apply(lambda s: s.split('-')[0].replace(' ', ''))
                # 股利公積的部分因是分為是否法定，所以這邊我們再把他進行加總
                data['capital_reserve_cash_dividend'] = data['capital_reserve_cash_dividend_1'] + data['capital_reserve_cash_dividend_2']
                data['capital_reserve_stock_dividend'] = data['capital_reserve_stock_dividend_1'] + data['capital_reserve_stock_dividend_2']

                data = data[['stock_code', 'dividend_year', 'dividend_date', 'cash_dividend',
                             'capital_reserve_cash_dividend', 'cash_dividend_total', 'stock_dividend',
                             'capital_reserve_stock_dividend', 'stock_dividend_total', 'remarks']]

                type_dividend = pd.concat([type_dividend, data], axis=0)

        company_dividend = pd.concat([company_dividend, type_dividend])

    return company_dividend
	
if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('crawl_company_dividend.py')
    print('程式開始執行時間：' + start_time.strftime("%Y-%m-%d %H:%M:%S"))
	
	# 設定爬蟲時間
    data_date = datetime.datetime.now()
    data_date = data_date.strftime("%Y-%m-%d")
	
	# 這邊要想一下股利政策執行程式的時間