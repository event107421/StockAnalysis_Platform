#!/usr/bin/env python
# encoding: utf-8
'''
@time: 2021/10/16 下午4:01
@author: bill (billevent107421@gmail.com)
@file: crawl_stock_quarterly_report.py
@type: 需求
@project: 爬取個股每季財報相關資料，如每季綜合損益表、每季資產負債表、每季現金流量表等資料
@desc:
'''

import sys, requests, datetime, tempfile, zipfile, time
sys.path.append(r"/Users/bill/Desktop/pythonProject/finance")
from db_tools import db_tools
import pandas as pd
import numpy as np
from io import StringIO

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

if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('crawl_stock_fundamental_data.py')
    print('程式開始執行時間：' + start_time.strftime("%Y-%m-%d %H:%M:%S"))

    # 設定爬蟲時間
    data_date = datetime.datetime.now()
    data_date = data_date.strftime("%Y-%m-%d")

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

        