#!/usr/bin/env python
# encoding: utf-8
'''
@time: 2021/10/16 下午4:01
@author: bill (billevent107421@gmail.com)
@file: crawl_industry_per.py
@type: 需求
@project: 爬取上市、上櫃各類產業本益比資料
@desc:
'''

import sys, requests, datetime, tempfile, zipfile, time
sys.path.append(r"/Users/bill/Desktop/pythonProject/finance")
from db_tools import db_tools
import pandas as pd
import numpy as np
from io import StringIO

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
	
# 找一下看有沒有上櫃的本益比資料

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
	
	if data_date == industry_date:
		industry_year_month = datetime.datetime.now() - datetime.timedelta(30)
		industry_year_month = industry_year_month.strftime("%Y-%m-%d")
		# 每月爬取大盤及各大類產業的本益比等資料
		industry_per = get_industry_per(year_month=industry_year_month[0:4] + industry_year_month[5:7])
		industry_per['data_year_month'] = industry_year_month[0:7]