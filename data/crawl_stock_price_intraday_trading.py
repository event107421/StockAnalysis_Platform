#!/usr/bin/env python
# encoding: utf-8
'''
@time: 2021/9/1 下午4:01
@author: bill (billevent107421@gmail.com)
@file: crawl_daily_finance_data.py
@type: 需求
@project: 個股每五秒的撮合成交價格，盤中即時資訊（但還有效能問題待解決）
@desc:
'''


import sys
# sys.path.append(r"./")
sys.path.append(r"/Users/bill/Desktop/pythonProject/finance")
from db_tools import db_tools
import requests
import json
import numpy as np
import pandas as pd
import datetime
import time
import sched


# 事件調度器，需先輸入下面語法初始化排程設定
s = sched.scheduler(time.time, time.sleep)

def real_time_stock_price_crawl(inc, stock_code):
    # 目前測試查詢最大值大概160個個股代碼左右，但是如果在盤中一次取160支股票會很慢，所以每次取50支股票，然後需要將所有股票代碼進行資料切割
    # 先創一個數列，做為每次取160個個股代碼的索引
    num_range = list(range(0, len(stock_code) + 50, 50))
    # 創一個數列，做為取上面數列的順序
    len_range = list(range(1, len(num_range), 1))
    # 裝錯誤的股票代碼索引
    stock_error_sequence = []
    # 裝即時盤中股價的資料框架
    stock_data = pd.DataFrame()
    for i in len_range:
        start_num = num_range[i - 1]
        end_num = num_range[i]
        stock_code_str = '|'.join('{}'.format(code_str) for code_str in stock_code[start_num:end_num])

        # 從下面這個網站爬取盤中即時資訊：https://mis.twse.com.tw/stock/fibest.jsp
        time.sleep(3)
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={stock_code_str}&json=1&delay=0&_=1552123547443"
        try:
            reqs = requests.get(url)
            reqsjson = json.loads(reqs.text)
            columns = ['c', 'n', 'z', 'tv', 'v', 'o', 'h', 'l', 'y', 'b']
            df = pd.DataFrame(reqsjson['msgArray'], columns=columns)
            # 若沒有最新的成交價格，就取最佳買價五檔中的第一檔做為目前最新的價格
            df['z'] = df.apply(lambda x: x['z'] if x['z'] != '-' else x['b'].split('_')[0], axis=1)
            df = df.drop('b', axis=1)
            df.columns = ['stock_code', 'company_name', 'newest_deal_price', 'newest_volume', 'accumulation_volume', 'open_price', 'high_price', 'low_price', 'yesterday_close_price']
            df.insert(9, "price_change_ratio", 0.0)

            stock_data = pd.concat([stock_data, df])
        except:
            error_sequence = (start_num, end_num)
            stock_error_sequence.append(error_sequence)

        # 資料中的'-'符號取代為na，後續用其他值來做填補
        stock_data = stock_data.replace('-', np.nan)
        stock_data['newest_volume'].fillna(value=0, inplace=True)
        stock_data['accumulation_volume'].fillna(value=0, inplace=True)
        # 新增漲跌百分比
        stock_data['price_change_ratio'] = round((stock_data['newest_deal_price'].astype(float) - stock_data['yesterday_close_price'].astype(float)) / stock_data['yesterday_close_price'].astype(float) * 100, 2)

    # 新增寫入的時間欄位
    stock_data['insert_time'] = datetime.datetime.now()

    # 先將原本的基本資料刪除
    stock_information_truncate = db_tools('db_invest').sql_delete_data(dbs_name='Investment',
                                                                       delete_type='truncate',
                                                                       db_table='daily_real_time_stock_price',
                                                                       date='')

    # 再將資料寫入
    insert_real_time_price = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
                                                                   sql_insert_data=stock_data,
                                                                   db_table='daily_real_time_stock_price')

    print('爬取個股基本資料於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
    print('匯入資料庫總筆數：', str(len(stock_data)) + '筆')

    # 讓程式在時間段內一直執行
    date_time = datetime.datetime.now()
    start_time = datetime.datetime.strptime(str(date_time.date()) + '9:00', '%Y-%m-%d%H:%M')
    end_time = datetime.datetime.strptime(str(date_time.date()) + '13:40', '%Y-%m-%d%H:%M')

    # 如果時間到收盤時間的話，就將最後的資料寫入收集日股價資料表中，留存資料
    if date_time >= end_time:
        # 從即時股價資料表拉出資料備份到另外一張存日股價資料的資料表
        sql = "select stock_code \
                            , open_price \
                            , high_price \
                            , low_price \
                            , newest_deal_price as close_price \
                            , accumulation_volume as volume \
                            from daily_real_time_stock_price"

        stock_price_daily = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)

        # 因校正股價是用爬取yahoo股價資料套件才會有的欄位，所以這部分先用na代替
        stock_price_daily['adj_close_price'] = np.nan
        stock_price_daily['data_date'] = datetime.datetime.now().strftime("%Y-%m-%d")

        # 修改欄位順序
        stock_price_daily = stock_price_daily[
            ['stock_code', 'data_date', 'high_price', 'low_price', 'open_price', 'close_price', 'volume',
             'adj_close_price']]

        # 將爬取的股價資料寫入資料庫
        insert_stock_price = db_tools('db_invest').sql_insert_data(dbs_name='Investment',
                                                                   sql_insert_data=stock_price_daily,
                                                                   db_table='stock_price')

        print('寫入當天個股股價於' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '已完成')
        print('匯入資料庫總筆數：', str(len(stock_price_daily)) + '筆')

    # 判斷爬蟲終止條件
    if date_time >= start_time and date_time <= end_time:
        s.enter(inc, 0, real_time_stock_price_crawl, argument=(inc, stock_code))


def main_stock_price(inc, stock_code):
    # enter四個引數分別為：間隔事件、優先順序（用於同時間到達的兩個事件同時執行時定序）、被呼叫觸發的函式，
    # 給該觸發函式的引數（tuple形式）
    s.enter(0, 0, real_time_stock_price_crawl, (inc, stock_code))
    s.run()


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    print('crawl_stock_price_intraday_trading.py')
    print('程式開始執行時間：' + start_time.strftime("%Y-%m-%d %H:%M:%S"))

    # 從資料庫內取出原本的個股基本資料
    sql = "select (case when stock_information.stock_class = 'TWSE' then concat('tse_', stock_information.stock_code, '.tw') \
    				    when stock_information.stock_class = 'OTC' then concat('otc_', stock_information.stock_code, '.tw') \
                        else '' end) as stock_code \
            from stock_information, \
            ( \
                select stock_code, max(update_date) as newest_update_date \
                from stock_information \
                group by stock_code \
            ) as a \
            where stock_information.stock_code = a.stock_code \
            and stock_information.update_date = a.newest_update_date"

    stock_information = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)

    # 這邊在設定間隔秒數時，要記得把程式執行時間加入，因為sched套件規則是，當前一個任務執行完之後，才會執行下一個任務
    main_stock_price(inc=300, stock_code=stock_information['stock_code'])

    end_time = datetime.datetime.now()
    print('程式執行完成時間' + end_time.strftime("%Y-%m-%d %H:%M:%S"))
    print('程式總執行時間：' + str((end_time - start_time).seconds) + '秒')
    print('===========================================================')