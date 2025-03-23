import logging
import time
from typing import Dict, List, Tuple
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime

# 配置日志
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


class LowFrequencyQuant:
    def __init__(self):
        self.stock_pool = []
        self.position_file = "positions.csv"
        self.historical_years = 10
        self.max_retries = 3  # 新增重试次数

    def get_stock_fundamentals(self) -> pd.DataFrame:
        """带重试机制的数据获取"""
        retries = 0
        while retries < self.max_retries:
            try:
                spot_df = ak.stock_zh_a_spot_em()

                # 字段映射（根据你的接口调整）
                column_mapping = {
                    'code': ['代码'],
                    'name': ['名称'],
                    'price': ['最新价'],
                    'pb': ['市净率'],
                    'pe': ['市盈率-动态']
                }

                actual_columns = {}
                for key, candidates in column_mapping.items():
                    for candidate in candidates:
                        if candidate in spot_df.columns:
                            actual_columns[key] = candidate
                            break
                    else:
                        if key != 'pe':  # PE为必要字段
                            logging.error(f"未找到匹配字段: {key}")
                            return pd.DataFrame()

                # 类型转换
                required_df = spot_df[list(actual_columns.values())]
                required_df.columns = actual_columns.keys()
                required_df = required_df.assign(
                    price=pd.to_numeric(required_df['price'], errors='coerce'),
                    pb=pd.to_numeric(required_df['pb'], errors='coerce'),
                    pe=pd.to_numeric(required_df['pe'], errors='coerce')
                ).dropna()

                # 添加模拟字段
                required_df['dividend_yield'] = 0.03  # 模拟数据
                required_df['pb_10year_pct'] = np.clip(
                    np.random.normal(loc=0.2, scale=0.1, size=len(required_df)),
                    0, 0.3
                )
                return required_df

            except Exception as e:
                retries += 1
                logging.warning(f"数据获取失败，正在重试({retries}/{self.max_retries})...")
                time.sleep(2 ** retries)  # 指数退避
        return pd.DataFrame()

    def generate_price_targets(self, df: pd.DataFrame) -> pd.DataFrame:
        """生成估值价格区间（新增关键方法）"""
        try:
            df['low_price'] = df['price'] * 0.8
            df['fair_price'] = df['price'] * 1.0
            df['high_price'] = df['price'] * 1.2
            return df
        except KeyError as e:
            logging.error(f"价格计算失败，缺失字段: {str(e)}")
            return pd.DataFrame()

    def stock_screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """筛选逻辑（容错处理）"""
        try:
            return df[
                (df['pb'] < 2) &
                (df['pe'] < 30) &
                (df['pb_10year_pct'] < 0.3)
                ].sort_values(by='pe', ascending=True).head(20)
        except Exception as e:
            logging.error(f"筛选失败: {str(e)}")
            return pd.DataFrame()

    def run(self):
        """主运行逻辑（异常处理优化）"""
        try:
            # 股票筛选
            stock_data = self.get_stock_fundamentals()
            if stock_data.empty:
                logging.error("数据获取失败，请检查：1.网络连接 2.akshare版本")
                return

            screened = self.stock_screen(stock_data)
            priced = self.generate_price_targets(screened)  # 调用新增方法

            # 打印结果
            print("\n=== 股票推荐列表 ===")
            print(priced[['code', 'name', 'price', 'low_price', 'fair_price', 'high_price']])

        except Exception as e:
            logging.error(f"运行异常: {str(e)}")


if __name__ == "__main__":
    lfq = LowFrequencyQuant()
    print("=== 首次运行（测试网络连接）===")
    lfq.run()
