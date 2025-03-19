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

if __name__ == '__main__':
    # just for local test
    # current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    # log_filename = 'sequoia-{}.log'.format(current_time)
    # print(log_filename)
    import akshare as ak

    # 打印akshare的股票实时行情数据结构
    df = ak.stock_zh_a_spot_em()

    print("\n当前akshare股票接口字段列表：")
    print(df.columns.tolist())

    print("\n当前akshare债券接口字段列表：")
    cb_df = ak.bond_zh_cov()
    print(cb_df.columns.tolist())

    print("\n股票数据样例：")
    print(df.head(3).to_markdown())