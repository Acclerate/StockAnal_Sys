# !/usr/bin/env python
# -*-coding:utf-8 -*-

"""
# File       : local_test
# Time       ：2023/3/3 20:40
# Author     ：qunzhong
# version    ：python 3.8
# Description：
"""
import datetime
import akshare as ak

import logging

logging.basicConfig(level=logging.INFO)


def get_stock_list():
    """获取所有 A 股股票代码"""
    stock_df = ak.stock_info_a_code_name()
    print(stock_df.head())  # 打印前几行看看列名
    return stock_df
def get_financial_data(stock_code):
    """获取个股财务数据"""
    try:
        # 尝试使用东方财富数据
        finance_df = ak.stock_financial_analysis_indicator_em(symbol=stock_code)

        if finance_df is None or finance_df.empty:
            logging.warning(f"财务数据为空 {stock_code}")
            return None

        return finance_df

    except Exception as e:
        logging.error(f"财务数据获取失败 {stock_code}: {e}", exc_info=True)
        return None


if __name__ == '__main__':
    # just for local test
    # current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # log_filename = 'sequoia-{}.log'.format(current_time)
    # print(log_filename)
    # import akshare as ak
    #
    # # 打印akshare的股票实时行情数据结构
    # df = ak.stock_zh_a_spot_em()
    #
    # print("\n当前akshare股票接口字段列表：")
    # print(df.columns.tolist())
    #
    # print("\n当前akshare债券接口字段列表：")
    # cb_df = ak.bond_zh_cov()
    # print(cb_df.columns.tolist())
    #
    # print("\n股票数据样例：")
    # print(df.head(3).to_markdown())
    # get_stock_list()

    stock_code = "300196"
    finance_df = ak.stock_financial_abstract(symbol=stock_code)

    print(finance_df)  # 查看是否能获取数据