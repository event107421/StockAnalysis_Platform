#!/usr/bin/env python
# encoding: utf-8
'''
@time: 2021/9/1 下午4:01
@author: bill (billevent107421@gmail.com)
@file: crawl_daily_finance_data.py
@type: 需求
@project: 統計個股的各項技術指標，並且將符合交易策略的個股選出來進行推薦
@desc:
'''

import sys
sys.path.append(r"/Users/bill/Desktop/pythonProject/finance")
from db_tools import db_tools
import pandas as pd
import talib

class technical_analysis():
    def __init__(self):
        # 個股代碼
        sql = f"select distinct stock_code \
                from stock_price_month"

        stock_code = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)
        self.stock_code_data = stock_code['stock_code'].tolist()

    def technical_analysis_stat(self, data_set):
        # 因套件有規定資料框架格式，所以將欄位修改為套件所規定的名稱
        data_set.rename(columns={'open_price': 'open', 'high_price': 'high', 'low_price': 'low', 'close_price': 'close'}, inplace=True)

        result = pd.DataFrame()
        for code in self.stock_code_data:
            df = data_set[data_set['stock_code'] == code].reset_index()
            df = df.drop(['index'], axis=1)

            if len(df) > 0:
                # 計算均線
                df['ma5'] = talib.SMA(df['close'], timeperiod=5)
                df['ma10'] = talib.SMA(df['close'], timeperiod=10)
                df['ma20'] = talib.SMA(df['close'], timeperiod=20)
                df['ma40'] = talib.SMA(df['close'], timeperiod=40)
                df['ma60'] = talib.SMA(df['close'], timeperiod=60)
                df['ma120'] = talib.SMA(df['close'], timeperiod=120)
                df['ma240'] = talib.SMA(df['close'], timeperiod=240)

                # 一般KD指標的參數設定為9日，計算K和D時，所取的平滑值就用3，因此指標的參數上可以看到(9,3,3)這樣的參數，所以fastk_period設定為9
                # slowk_matype跟slowd_matype是平滑的種類，通常我們設為1，這就是一般人使用的週期，有自己的週期看法都能自行更改
                df['k'], df['d'] = talib.STOCH(df['high'], df['low'], df['close'],
                                               fastk_period=9, slowk_period=3, slowk_matype=1,
                                               slowd_period=3, slowd_matype=1)

                # MACD：
                # fastperiod：短期EMA平滑天數
                # slowperiod：長期EMA平滑天數
                # signalperiod：DEA線平滑天數
                df['MACD'], df['MACDsignal'], df['MACDhist'] = talib.MACD(df["close"],
                                                                          fastperiod=12,
                                                                          slowperiod=26,
                                                                          signalperiod=9)

                # 布林通道：
                # timeperiod : 為均線週期，通常我會使用20
                # nbdevup、nbdevdn : 為上下的標準差，這裡我會用我習慣的2.1倍標準差
                # matype : 一樣是平滑的種類，這裡我們就不變動
                df["upper"], df["middle"], df["lower"] = talib.BBANDS(df["close"],
                                                                      timeperiod=20,
                                                                      nbdevup=2.1,
                                                                      nbdevdn=2.1,
                                                                      matype=0)
                # 合併至最終資料集
                result = pd.concat([result, df], axis=0)

        return result

    # 近三天三大法人買賣超比例
    def institutional_investors_ratio(self):
        sql = f"select a.stock_code \
                , company_name_ch \
                , (foreign_investor_vol_amount / company_capital) * 1000 \
                , (foreign_investor_dealer_vol_amount / company_capital) * 1000 \
                , (investment_trust_vol_amount / company_capital) * 1000 \
                , (dealer_vol_amount / company_capital) * 1000 \
                , (dealer_hedge_vol_amount / company_capital) * 1000 \
                , (institutional_investors_vol_amount / company_capital) * 1000 \
                from ( \
                    select stock_code \
                    , company_name_ch \
                    , company_capital / 1000 as company_capital \
                    from stock_information \
                ) as a \
                left join ( \
                    select stock_code \
                    , sum(foreign_investor_vol_amount) / 1000 as foreign_investor_vol_amount \
                    , sum(foreign_investor_dealer_vol_amount) / 1000 as foreign_investor_dealer_vol_amount \
                    , sum(investment_trust_vol_amount) / 1000 as investment_trust_vol_amount \
                    , sum(dealer_vol_amount) / 1000 as dealer_vol_amount \
                    , sum(dealer_hedge_vol_amount) / 1000 as dealer_hedge_vol_amount \
                    , sum(institutional_investors_vol_amount) / 1000 as institutional_investors_vol_amount \
                    from stock_institutional_investors \
                    where DATEDIFF(NOW(), data_date) <= 3 \
                    group by stock_code \
                ) as b \
                on a.stock_code = b.stock_code"

        result = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)

        return result

    # 三大法人近三天連續買賣超之個股
    def institutional_investors_count(self):
        sql = f"select stock_code \
                , sum(case when foreign_investor_vol_amount > 0 then 1 else 0 end) as foreign_investor_buy_count \
                , sum(case when investment_trust_vol_amount > 0 then 1 else 0 end) as investment_trust_buy_count \
                , sum(case when dealer_vol_amount > 0 then 1 else 0 end) as dealer_buy_count \
                , sum(case when institutional_investors_vol_amount > 0 then 1 else 0 end) as institutional_investors_buy_count \
                from ( \
                    select stock_code \
                    , data_date \
                    , (foreign_investor_dealer_vol_amount + foreign_investor_vol_amount) as foreign_investor_vol_amount \
                    , investment_trust_vol_amount \
                    , (dealer_vol_amount + dealer_hedge_vol_amount) as dealer_vol_amount \
                    , institutional_investors_vol_amount \
                    from stock_institutional_investors \
                    where DATEDIFF(NOW(), data_date) <= 3 \
                ) as a \
                group by stock_code \
                having sum(case when foreign_investor_vol_amount > 0 then 1 else 0 end) = 3 \
                or sum(case when investment_trust_vol_amount > 0 then 1 else 0 end) = 3 \
                or sum(case when dealer_vol_amount > 0 then 1 else 0 end) = 3 \
                or sum(case when institutional_investors_vol_amount > 0 then 1 else 0 end) = 3"

        result = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)

        return result

    def stock_selection_strategy(self, data_set, select_operation):
        technical_analysis_stat = self.technical_analysis_stat(data_set)
        institutional_investors_ratio = self.institutional_investors_ratio()
        institutional_investors_count = self.institutional_investors_count()

        # 這邊把三大法人買賣超相關訊息串進計算技術指標的資料集內
        filter_data = pd.merge(technical_analysis_stat, institutional_investors_ratio)
        filter_data = pd.merge(filter_data, institutional_investors_count)

        # 放空是種短期（Short）的炒作，做多則相對是長期（Long）的投資，Short和Long代表的多空意義，因此而來
        # 所以我們在這邊把long當作買進的選股策略，short當作賣出(融券放空)的選股策略
        if select_operation == 'long':
            # K、D在20以下黃金交叉（也就是K值 > D值）或是RSI在20以下黃金交叉
            technical_analysis_stat['buy_flag'] = technical_analysis_stat.apply(lambda x: 1 if x['k'] > x['d'] and x['k'] <= 20 and x['d'] <= 20 else 0, axis=1)
            # MACD、DIF在負值的時候黃金交叉，且至少兩個差值要在0.1以上

            # MA:短期移動平均線由下往上突破長期移動平均線時，稱為黃金交叉

            # 以上三個指標規則選出來的個股再來計算後續上漲的幅度，再來看看三大法人買超、各個均線、成交量表現的情況，再來制定一套選股策略
        elif select_operation == 'short':
            # K、D在80以上死亡交叉或是RSI在80以下死亡交叉

            # MACD、DIF在正值的時候死亡交叉，且至少兩個差值要在0.1以上

            # MA:短期移動平均線由上往下突破長期移動平均線時，稱為死亡交叉

            # 以上三個指標規則選出來的個股再來計算後續上漲的幅度，再來看看三大法人買超、各個均線、成交量表現的情況，再來制定一套選股策略

if __name__ == '__main__':
    # 從即時股價資料表拉出資料備份到另外一張存日股價資料的資料表
    sql = "select stock_code \
         , open_price \
         , high_price \
         , low_price \
         , newest_deal_price as close_price \
         , accumulation_volume as volume \
         from daily_real_time_stock_price"

    stock_price_daily = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)
    stock_price_daily['data_date'] = '2021-09-23'

    sql = f"select stock_code \
            , data_date \
            , open_price \
            , high_price \
            , low_price \
            , close_price \
            , volume \
            from stock_price \
            where data_date between '2021-01-01' and '2021-09-23'"

    stock_price = db_tools('db_invest').sql_query_data(dbs_name='Investment', sql_statment=sql)

    stock_price = pd.concat([stock_price, stock_price_daily])