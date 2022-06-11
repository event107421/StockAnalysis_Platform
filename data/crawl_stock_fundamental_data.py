#!/usr/bin/env python
# encoding: utf-8
'''
@time: 2021/10/16 下午4:01
@author: bill (billevent107421@gmail.com)
@file: crawl_stock_fundamental_data.py
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

# 爬取營收網址從以下網址來的：https://mops.twse.com.tw/mops/web/t21sc04_ifrs
def monthly_report(year, month):
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
    # 偽瀏覽器
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

# 預設網頁post傳送的內容
payload = {
        'encodeURIComponent': '1',
        'step': '1',
        'firstin': '1',
        'off': '1',
        'isQuery': 'Y'
    }

# 爬取公司每季綜合損益表：https://mops.twse.com.tw/mops/web/t163sb04
def get_company_performance(payload_dict):
    url = 'https://mops.twse.com.tw/mops/web/ajax_t163sb04'
    res = requests.post(url, data=payload_dict)
    df = pd.read_html(res.text, flavor='html5lib')

    return df

# 爬取公司每季資產負債表：https://mops.twse.com.tw/mops/web/t163sb05
def get_company_assets(payload_dict):
    url = 'https://mops.twse.com.tw/mops/web/t163sb05'
    res = requests.post(url, data=payload_dict)
    df = pd.read_html(res.text, flavor='html5lib')

    return df

# 爬取公司每季現金流量表：https://mops.twse.com.tw/mops/web/t163sb20
def get_company_cash_flows(payload_dict):
    url = 'https://mops.twse.com.tw/mops/web/t163sb20'
    res = requests.post(url, data=payload_dict)
    df = pd.read_html(res.text, flavor='html5lib')

    return df

# 一次爬取公司的各項季報資料
def crawl_company_quarterly_report(year_str, season_str, is_financial):
    year_season_str = str(year_str) + str(season_str)
    # 爬取的公司種類：上市(sii)、上櫃(otc)
    type_list = ['sii', 'otc']

    company_performance = pd.DataFrame()
    company_assets = pd.DataFrame()
    company_cash_flows = pd.DataFrame()

    for type in type_list:
        payload['TYPEK'] = type

        # 假如是西元，轉成民國
        if int(year_str) > 1990:
            year_str = int(year_str) - 1911
            year_str = str(year_str)
        payload['year'] = year_str

        # 假設第幾季只有打1、2、3、4，那就在前面補上0
        if len(season_str) == 1:
            season_str = '0' + str(season_str)
        payload['season'] = season_str

        performance_data = get_company_performance(payload_dict=payload)

        for data in performance_data:
            if '公司名稱' in data.columns.to_list():
                data['performance_year'] = year_season_str
                if '台中銀' in data['公司名稱']:
                    data['營業收入'] = data['利息淨收益'] + data['利息以外淨損益']
                    data['營業成本'] = np.nan
                    data['營業毛利（毛損）淨額'] = np.nan
                    data['營業利益（損失）'] = np.nan
                    data['營業外收入及支出'] = np.nan
                    bank_industry_perf = data[['公司代號', '營業收入', '營業成本', '營業毛利（毛損）淨額', '營業費用', '呆帳費用、承諾及保證責任準備提存', '營業利益（損失）', '營業外收入及支出',
                                               '繼續營業單位稅前淨利（淨損）', '所得稅費用（利益）', '淨利（損）歸屬於母公司業主', '基本每股盈餘（元）', 'performance_year']]
                elif '群益證' or '元大期貨' in data['公司名稱']:
                    data['營業成本'] = np.nan
                    data['營業毛利（毛損）淨額'] = np.nan
                    data['呆帳費用、承諾及保證責任準備提存'] = np.nan
                    securities_industry_perf = data[['公司代號', '收益', '營業成本', '營業毛利（毛損）淨額', '支出及費用', '呆帳費用、承諾及保證責任準備提存', '營業利益', '營業外損益',
                                                     '稅前淨利（淨損）', '所得稅費用（利益）', '淨利（損）歸屬於母公司業主', '基本每股盈餘（元）', 'performance_year']]
                elif '中信金' or '日盛金' in data['公司名稱']:
                    data['營業收入'] = data['利息淨收益'] + data['利息以外淨損益']
                    data['營業成本'] = np.nan
                    data['營業毛利（毛損）淨額'] = np.nan
                    data['營業利益（損失）'] = np.nan
                    data['營業外收入及支出'] = np.nan
                    financial_industry_perf = data[['公司代號', '營業收入', '營業成本', '營業毛利（毛損）淨額', '營業費用', '呆帳費用、承諾及保證責任準備提存', '營業利益（損失）', '營業外收入及支出',
                                                    '繼續營業單位稅前淨利（淨損）', '所得稅費用（利益）', '淨利（損）歸屬於母公司業主', '基本每股盈餘（元）', 'performance_year']]
                elif '中壽' in data['公司名稱']:
                    data['營業毛利（毛損）淨額'] = np.nan
                    data['呆帳費用、承諾及保證責任準備提存'] = np.nan
                    insurance_industry_perf = data[['公司代號', '營業收入', '營業成本', '營業毛利（毛損）淨額', '營業費用', '呆帳費用、承諾及保證責任準備提存', '營業利益（損失）', '營業外收入及支出',
                                                    '繼續營業單位稅前純益（純損）', '所得稅費用（利益）', '本期淨利（淨損）', '基本每股盈餘（元）', 'performance_year']]
                elif '和泰車' in data['公司名稱']:
                    data['呆帳費用、承諾及保證責任準備提存'] = np.nan
                    other_industry_perf = data[['公司代號', '收入', '支出', '呆帳費用、承諾及保證責任準備提存', '繼續營業單位稅前淨利（淨損）', '所得稅費用（利益）',
                                                '淨利（淨損）歸屬於母公司業主', '基本每股盈餘（元）', 'performance_year']]
                else:
                    data['呆帳費用、承諾及保證責任準備提存'] = np.nan
                    general_industry_perf = data[['公司代號', '營業收入', '營業成本', '營業毛利（毛損）淨額', '營業費用', '呆帳費用、承諾及保證責任準備提存', '營業利益（損失）', '營業外收入及支出',
                                                  '稅前淨利（淨損）', '所得稅費用（利益）', '淨利（淨損）歸屬於母公司業主', '基本每股盈餘（元）', 'performance_year']]
        # 避免被封IP，每爬完一個季報部分就休息5秒
        time.sleep(5)

        assets_data = get_company_assets(payload_dict=payload)
        for data in assets_data:
            if '公司名稱' in data.columns.to_list():
                data['assets_year'] = year_season_str
                if '台中銀' in data['stock_name']:
                    data['流動資產'] = data['現金及約當現金'] + data['存放央行及拆借銀行同業'] + data['透過損益按公允價值衡量之金融資產'] + data['透過其他綜合損益按公允價值衡量之金融資產'] + data['按攤銷後成本衡量之債務工具投資'] + data['避險之衍生金融資產淨額'] + data['附賣回票券及債券投資淨額'] + data['應收款項－淨額'] + data['當期所得稅資產'] + data['待出售資產－淨額'] + data['待分配予業主之資產－淨額'] + data['貼現及放款－淨額']
                    data['非流動資產'] = data['採用權益法之投資－淨額'] + data['受限制資產－淨額'] + data['其他金融資產－淨額'] + data['不動產及設備－淨額'] + data['使用權資產－淨額'] + data['投資性不動產投資－淨額'] + data['無形資產－淨額'] + data['遞延所得稅資產'] + data['其他資產－淨額']
                    data['流動負債'] = data['央行及銀行同業存款'] + data['央行及同業融資'] + data['透過損益按公允價值衡量之金融負債'] + data['避險之衍生金融負債－淨額'] + data['附買回票券及債券負債'] + data['應付款項'] + data['當期所得稅負債'] + data['與待出售資產直接相關之負債'] + data['存款及匯款']
                    data['非流動負債'] = data['應付金融債券'] + data['應付公司債'] + data['特別股負債'] + data['其他金融負債'] + data['負債準備'] + data['租賃負債'] + data['遞延所得稅負債'] + data['其他負債']
                    bank_industry_assets = data[['流動資產', '非流動資產', '資產總計', '流動負債', '非流動負債', '負債總計', '股本',
                                                 '庫藏股票', '非控制權益', '權益總計', '每股參考淨值', 'assets_year']]
                    bank_industry_assets.columns = ['current_assets', 'non_current_assets', 'total_assets',
                                                    'current_liabilities', 'non_current_liabilities',
                                                    'total_liabilities', 'capital', 'treasury_stock',
                                                    'non_controlling_interest', 'stockholders_equity', 'PBR',
                                                    'assets_year']
                elif '群益證' or '元大期貨' in data['stock_name']:
                    securities_industry_assets = data[['流動資產', '非流動資產', '資產總計', '流動負債', '非流動負債', '負債總計', '股本',
                                                       '庫藏股票', '非控制權益', '權益總計', '每股參考淨值', 'assets_year']]
                    securities_industry_assets.columns = ['current_assets', 'non_current_assets', 'total_assets',
                                                          'current_liabilities', 'non_current_liabilities',
                                                          'total_liabilities', 'capital', 'treasury_stock',
                                                          'non_controlling_interest', 'stockholders_equity', 'PBR',
                                                          'assets_year']
                elif '中信金' or '日盛金' in data['stock_name']:
                    data['流動資產'] = data['現金及約當現金'] + data['存放央行及拆借銀行同業'] + data['透過損益按公允價值衡量之金融資產'] + data['透過其他綜合損益按公允價值衡量之金融資產'] + data['按攤銷後成本衡量之債務工具投資'] + data['避險之衍生金融資產淨額'] + data['附賣回票券及債券投資淨額'] + data['應收款項－淨額'] + data['當期所得稅資產'] + data['待出售資產－淨額'] + data['待分配予業主之資產－淨額'] + data['貼現及放款－淨額']
                    data['非流動資產'] = data['採用權益法之投資－淨額'] + data['受限制資產－淨額'] + data['其他金融資產－淨額'] + data['不動產及設備－淨額'] + data['使用權資產－淨額'] + data['投資性不動產投資－淨額'] + data['無形資產－淨額'] + data['遞延所得稅資產'] + data['其他資產－淨額']
                    data['流動負債'] = data['央行及銀行同業存款'] + data['央行及同業融資'] + data['透過損益按公允價值衡量之金融負債'] + data['避險之衍生金融負債－淨額'] + data['附買回票券及債券負債'] + data['應付款項'] + data['當期所得稅負債'] + data['與待出售資產直接相關之負債'] + data['存款及匯款']
                    data['非流動負債'] = data['應付金融債券'] + data['應付公司債'] + data['特別股負債'] + data['其他金融負債'] + data['負債準備'] + data['租賃負債'] + data['遞延所得稅負債'] + data['其他負債']
                    financial_industry_assets = data[['流動資產', '非流動資產', '資產總計', '流動負債', '非流動負債', '負債總計', '股本',
                                                      '庫藏股票', '非控制權益', '權益總計', '每股參考淨值', 'assets_year']]
                    financial_industry_assets.columns = ['current_assets', 'non_current_assets', 'total_assets',
                                                         'current_liabilities', 'non_current_liabilities',
                                                         'total_liabilities', 'capital', 'treasury_stock',
                                                         'non_controlling_interest', 'stockholders_equity', 'PBR',
                                                         'assets_year']
                elif '中壽' in data['stock_name']:
                    data['流動資產'] = data['現金及約當現金'] + data['應收款項'] + data['本期所得稅資產'] + data['待出售資產'] + data['待分配予業主之資產（或處分群組）']
                    data['非流動資產'] = data['投資'] + data['再保險合約資產'] + data['不動產及設備'] + data['使用權資產'] + data['無形資產'] + data['遞延所得稅資產'] + data['其他資產'] + data['分離帳戶保險商品資產']
                    data['流動負債'] = data['短期債務'] + data['應付款項'] + data['本期所得稅負債'] + data['與待出售資產直接相關之負債'] + data['透過損益按公允價值衡量之金融負債'] + data['避險之衍生金融負債'] + data['應付債券'] + data['特別股負債'] + data['其他金融負債'] + data['租賃負債'] + data['保險負債']
                    data['非流動負債'] = data['具金融商品性質之保險契約準備'] + data['外匯價格變動準備'] + data['負債準備'] + data['遞延所得稅負債'] + data['其他負債'] + data['分離帳戶保險商品負債']
                    insurance_industry_assets = data[['流動資產', '非流動資產', '資產總計', '流動負債', '非流動負債', '負債總計', '股本',
                                                      '庫藏股票', '非控制權益', '權益總計', '每股參考淨值', 'assets_year']]
                    insurance_industry_assets.columns = ['current_assets', 'non_current_assets', 'total_assets',
                                                         'current_liabilities', 'non_current_liabilities',
                                                         'total_liabilities', 'capital', 'treasury_stock',
                                                         'non_controlling_interest', 'stockholders_equity', 'PBR',
                                                         'assets_year']
                elif '和泰車' in data['stock_name']:
                    other_industry_assets = data[['流動資產', '非流動資產', '資產總計', '流動負債', '非流動負債', '負債總計', '股本',
                                                  '庫藏股票', '非控制權益', '權益總計', '每股參考淨值', 'assets_year']]
                    other_industry_assets.columns = ['current_assets', 'non_current_assets', 'total_assets',
                                                     'current_liabilities', 'non_current_liabilities',
                                                     'total_liabilities', 'capital', 'treasury_stock',
                                                     'non_controlling_interest', 'stockholders_equity', 'PBR',
                                                     'assets_year']
                else:
                    general_industry_assets = data[['流動資產', '非流動資產', '資產總計', '流動負債', '非流動負債', '負債總計', '股本',
                                                    '庫藏股票', '非控制權益', '權益總計', '每股參考淨值', 'assets_year']]
                    general_industry_assets.columns = ['current_assets', 'non_current_assets', 'total_assets',
                                                       'current_liabilities', 'non_current_liabilities',
                                                       'total_liabilities', 'capital', 'treasury_stock',
                                                       'non_controlling_interest', 'stockholders_equity', 'PBR',
                                                       'assets_year']
        # 避免被封IP，每爬完一個季報部分就休息5秒
        time.sleep(5)

        cash_flows_data = get_company_cash_flows(payload_dict=payload)
        for data in cash_flows_data:
            if '公司名稱' in data.columns.to_list():
                data.columns = ['stock_code', 'stock_name', 'operating_cash_flows', 'investing_cash_flows',
                                'financing_cash_flows', 'others_cash_flows', 'net_cash_flow',
                                'free_cash_flow', 'beginning_balance', 'ending_balance']
                data['cash_flows_year'] = year_season_str
                data['free_cash_flow'] = data['operating_cash_flows'] + data['investing_cash_flows']

                if '台中銀' in data['stock_name']:
                    bank_industry_cash = data.drop(['stock_name'], axis=1)
                elif '群益證' or '元大期貨' in data['stock_name']:
                    securities_industry_cash = data.drop(['stock_name'], axis=1)
                elif '中信金' or '日盛金' in data['stock_name']:
                    financial_industry_cash = data.drop(['stock_name'], axis=1)
                elif '中壽' in data['stock_name']:
                    insurance_industry_cash = data.drop(['stock_name'], axis=1)
                elif '和泰車' in data['stock_name']:
                    other_industry_cash = data.drop(['stock_name'], axis=1)
                else:
                    general_industry_cash = data.drop(['stock_name'], axis=1)

        if is_financial == '0':
            # 整理非金融行業的季報資料
            company_performance = pd.concat([company_performance, general_industry_perf], axis=0)

            company_assets = pd.concat([company_assets, general_industry_assets])

            company_cash_flows = pd.concat([company_cash_flows, general_industry_cash], axis=0)
        elif is_financial == '1':
            # 整理金融行業的季報資料
            performance_data = pd.concat([bank_industry_perf, securities_industry_perf, financial_industry_perf, insurance_industry_perf], axis=0)
            company_performance = pd.concat([company_performance, performance_data], axis=0)

            assets_data = pd.concat([bank_industry_assets, securities_industry_assets, financial_industry_assets, insurance_industry_assets], axis=0)
            company_assets = pd.concat([company_assets, assets_data])

            cash_flows_data = pd.concat([bank_industry_cash, securities_industry_cash, financial_industry_cash, insurance_industry_cash], axis=0)
            company_cash_flows = pd.concat([company_cash_flows, cash_flows_data], axis=0)
        elif is_financial == 'all':
            # 整理所有行業的季報資料
            performance_data = pd.concat([bank_industry_perf, securities_industry_perf, financial_industry_perf,
                                          insurance_industry_perf, other_industry_perf, general_industry_perf], axis=0)
            company_performance = pd.concat([company_performance, performance_data], axis=0)

            assets_data = pd.concat([bank_industry_assets, securities_industry_assets, financial_industry_assets,
                                     insurance_industry_assets, other_industry_assets, general_industry_cash], axis=0)
            company_assets = pd.concat([company_assets, assets_data])

            cash_flows_data = pd.concat([bank_industry_cash, securities_industry_cash, financial_industry_cash,
                                         insurance_industry_cash, other_industry_cash, general_industry_cash], axis=0)
            company_cash_flows = pd.concat([company_cash_flows, cash_flows_data], axis=0)

    company_performance.columns = ['stock_code', 'is_finance', 'performance_year', 'capital_stock',
                                   'closing_price', 'average_price', 'price_change_value', 'price_change_ratio',
                                   'gross_revenue', 'operating_costs', 'operating_expenses', 'bad_debt_expense',
                                   'income_tax_expense', 'gross_profit', 'operating_income', 'non_operating_income',
                                   'profit_before_tax', 'profit_after_tax', 'EPS']

    company_assets.columns = ['current_assets', 'non_current_assets', 'total_assets', 'current_liabilities',
                              'non_current_liabilities', 'total_liabilities', 'capital', 'treasury_stock',
                              'non_controlling_interest', 'stockholders_equity', 'PBR', 'assets_year']

    company_cash_flows.columns = ['stock_code', 'cash_flows_year', 'operating_cash_flows', 'investing_cash_flows',
                                  'financing_cash_flows', 'others_cash_flows', 'net_cash_flow', 'free_cash_flow',
                                  'beginning_balance', 'ending_balance']

# 建立一個函數，判斷揭露財報日期是否為假日，如果為假日就先將其改為工作日
def get_work_date(data_date):
    # 從資料庫抓出當年度國定假日資料
    sql = f"select * \
            , (case when '{data_date}' between holiday_start and holiday_end then 1 else 0 end) as holiday_flag \
            from holiday_data \
            where holiday_start like '{data_date[0:4]}%' \
            order by holiday_start"

    holiday_data = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)

    # # 計算當年度假期資料及總共幾列
    # row_range = range(0, len(holiday_data))
    # # 放所有國定假日日期的list
    # holiday_list = []
    # # 對每個國定假日的日期衍生出範圍內的所有日期
    # for i in row_range:
    #     holiday_range = pd.date_range(start=holiday_data['holiday_start'][i],
    #                                   end=holiday_data['holiday_end'][i]).to_pydatetime().tolist()
    #     # 因為用date_range這個函數產出的範圍內所有日期都為日期格式，所以要再把每個轉成字串格式
    #     for holiday_date in holiday_range:
    #         holiday_list.append(holiday_date.strftime("%Y-%m-%d"))

    # 假如財報揭露日期為週末而且又是國定假日，那就以國定假日最後一天的隔天來當作財報揭露日期
    # 假如財報揭露日期為週末但不是國定假日，那就以週末的隔天來當作財報揭露日期
    if datetime.datetime.strptime(data_date, "%Y-%m-%d").weekday() in [5, 6] and sum(holiday_data['holiday_flag']) == 0:
        data_date = datetime.datetime.strptime(data_date, "%Y-%m-%d")
        if data_date.weekday() == 5:
            data_date = data_date + datetime.timedelta(2)
        elif data_date.weekday() == 6:
            data_date = data_date + datetime.timedelta(1)
    elif sum(holiday_data['holiday_flag']) > 0:
        data_date = holiday_data[holiday_data['holiday_flag'] == 1].iloc[0]['holiday_end']
        data_date = datetime.datetime.strftime(data_date + datetime.timedelta(1), "%Y-%m-%d")

    # 如果轉換後的日期就將其日期格式轉成字串格式
    if type(data_date) == datetime.datetime:
        data_date = data_date.strftime("%Y-%m-%d")

    return data_date

# 爬取上市各類產業本益比（這邊要注意並不包含上櫃公司）
def get_industry_per(year_month):
    url = f"https://www.twse.com.tw/statistics/count?url=%2FstaticFiles%2Finspection%2Finspection%2F04%2F001%2F{year_month}_C04001.zip&l1=%E4%B8%8A%E5%B8%82%E5%85%AC%E5%8F%B8%E6%9C%88%E5%A0%B1&l2=%E3%80%90%E5%A4%A7%E7%9B%A4%E3%80%81%E5%90%84%E7%94%A2%E6%A5%AD%E9%A1%9E%E8%82%A1%E5%8F%8A%E4%B8%8A%E5%B8%82%E8%82%A1%E7%A5%A8%E6%9C%AC%E7%9B%8A%E6%AF%94%E3%80%81%E6%AE%96%E5%88%A9%E7%8E%87%E5%8F%8A%E8%82%A1%E5%83%B9%E6%B7%A8%E5%80%BC%E6%AF%94%E3%80%91%E6%9C%88%E5%A0%B1"
    response = requests.get(url)
    # 創建臨存檔案
    tmp_file = tempfile.TemporaryFile()
    # 將爬蟲下來的內容寫入臨存檔案
    tmp_file.write(response.content)
    my_zip = zipfile.ZipFile(tmp_file, mode='r')
    file_name_list = []
    for file_name in my_zip.namelist():
        file_name_list.append(file_name)
    # 打開zip壓縮檔內的excel檔案
    result = pd.read_excel(my_zip.open(file_name_list[0]))
    # 只取各大類產業的資料
    industry_list = ['大 盤 ', '水泥工業類', '食品工業類', '塑膠工業類', '紡織纖維類', '電機機械類', '電器電纜類',
                     '玻璃陶瓷類', '造紙工業類', '鋼鐵工業類', '橡膠工業類', '汽車工業類', '建材營造類', '航運業類',
                     '觀光事業類', '金融保險類', '貿易百貨類', '其他類', '化學工業類', '生技醫療類', '油電燃氣類',
                     '半導體類', '電腦及周邊設備類', '光電類', '通信網路類', '電子零組件類', '電子通路類', '資訊服務類',
                     '其他電子類', '未含金融保險類', '未含電子類', '未含金融電子類', '水泥窯製類', '塑膠化工類', '機電類',
                     '化學生技醫療類', '電子工業類']
    result = result[result['P/E RATIO AND YIELD OF LISTED STOCKS'].isin(industry_list)][['P/E RATIO AND YIELD OF LISTED STOCKS', 'Unnamed: 5', 'Unnamed: 7', 'Unnamed: 9']]
    result.columns = ['stock_industry', 'PER', 'yield', 'PBR']
    result = result.replace('大 盤 ', '大盤')

    return result

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('crawl_stock_fundamental_data.py')
    print('程式開始執行時間：' + start_time.strftime("%Y-%m-%d %H:%M:%S"))

    # 設定爬蟲時間
    data_date = datetime.datetime.now()
    data_date = data_date.strftime("%Y-%m-%d")

    # 每月爬取大盤及各大類產業的本益比等資料的日期
    industry_date = data_date[0:7] + '-07'
    industry_date = get_work_date(data_date=industry_date)

    # 第一季財報，相關資料申報期限為每年的5/15，金控業期限為5/30
    # 第二季財報，相關資料申報期限為每年的8/14，金控業期限為8/31
    # 第三季財報，相關資料申報期限為每年的11/14，金控業期限為11/29
    # 無第四季財報，直接揭露年報，相關資料申報期限為隔年的3/31，所以要注意此部分當年度3/31的財報資料，為去年的年報資料
    financial_date = ['-05-30', '-08-31', '-11-29', '-03-31']
    other_date = ['-05-15', '-08-14', '-11-14', '-03-31']
    # 因為有可能揭露日期碰上假日，所以將碰到假日的日期改為後面最近的工作日
    for i in range(0, 4):
        financial_date[i] = get_work_date(data_date=(data_date[0:4] + financial_date[i]))
        other_date[i] = get_work_date(data_date=(data_date[0:4] + other_date[i]))

    if data_date[5:10] in other_date:
        # 爬取一般產業財報相關資料
    elif data_date[5:10] in financial_date:
        # 爬取金控業財報相關資料
    elif data_date == industry_date:
        industry_year_month = datetime.datetime.now() - datetime.timedelta(30)
        industry_year_month = industry_year_month.strftime("%Y-%m-%d")
        # 每月爬取大盤及各大類產業的本益比等資料
        industry_per = get_industry_per(year_month=industry_year_month[0:4] + industry_year_month[5:7])
        industry_per['data_year_month'] = industry_year_month[0:7]
        