#!/usr/bin/env python
# -*- coding: utf-8 -*-

import concurrent.futures
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Dict, Optional

import akshare as ak
import numpy as np
import pandas as pd
import requests
import talib
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('stock_filter.log'), logging.StreamHandler()]
)

# 全局配置参数 (可根据需要调整)
CONFIG = {
    'min_days': 250,  # 技术分析所需最小数据天数
    'roe_threshold': 0.08,  # ROE均值阈值
    'profit_growth_threshold': 0.15,  # 净利润增长率均值阈值
    'cash_flow_threshold': 0.5,  # 自由现金流比率阈值
    'debt_ratio_threshold': 0.6,  # 资产负债率阈值
    'dividend_yield_threshold': 0.05,  # 股息率阈值
    'volume_increase_ratio': 1.2,  # 成交量放大比例
    'max_workers': 4,  # 并发线程数
}

# 配置
CACHE_PATH = Path("stock_list_cache.pkl")
REQUEST_TIMEOUT = 10  # 请求超时时间


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_stock_list() -> pd.DataFrame:
    """多源获取+缓存+重试的股票列表获取（修正列名问题）"""
    # 尝试读取缓存
    if CACHE_PATH.exists():
        mtime = CACHE_PATH.stat().st_mtime
        if time.time() - mtime < 86400:  # 24小时有效缓存
            cached_df = pd.read_pickle(CACHE_PATH)
            # 检查缓存列名是否正确
            if {'code', 'name'}.issubset(cached_df.columns):
                return cached_df

    # 定义数据源及列名处理
    data_sources = [
        {
            "func": ak.stock_info_a_code_name,
            "name": "AKShare主源",
            "col_map": {}  # 该数据源本身返回'code'和'name'
        },
        {
            "func": lambda: ak.stock_info_sz_name_code(symbol="A股列表"),
            "name": "深交所",
            "col_map": {'证券代码': 'code', '证券简称': 'name'}
        },
        {
            "func": ak.stock_zh_a_spot_em,
            "name": "腾讯财经",
            "col_map": {'代码': 'code', '名称': 'name'}
        }
    ]

    for source in data_sources:
        try:
            stock_df = source["func"]()
            if stock_df is None or stock_df.empty:
                continue

            # 统一列名
            stock_df = stock_df.rename(columns=source["col_map"])

            # 确保包含所需列
            if not {'code', 'name'}.issubset(stock_df.columns):
                continue

            # 保留必要列并去重
            stock_df = stock_df[['code', 'name']].drop_duplicates('code')

            # 缓存处理
            stock_df.to_pickle(CACHE_PATH)
            logging.info(f"使用数据源: {source['name']}, 股票数量: {len(stock_df)}")
            return stock_df

        except Exception as e:
            logging.warning(f"数据源 {source['name']} 失败: {str(e)}")
            continue

    raise ConnectionError("所有数据源均不可用")

# 预编译正则表达式和常量
_STOCK_CODE_PATTERN = re.compile(r"^(sh|sz|bj)\d{6}$")
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"
]
_REQUIRED_COLUMNS = ["指标名称", "指标值"]
_REQUIRED_INDICATORS = [
    'ROE(净资产收益率·摊薄)',
    '净利润增长率',
    '经营活动产生的现金流量净额/EBIT',
    '资产负债率',
    '股息率'
]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20))
def get_financial_data(stock_code: str) -> Optional[pd.DataFrame]:
    """获取个股财务指标数据（增强稳定性版）"""
    try:
        # 股票代码格式校验与修正
        original_code = stock_code
        if not _STOCK_CODE_PATTERN.match(stock_code):
            prefix_mapping = {
                ("6", "9"): "sh",
                ("0", "3"): "sz",
                ("4", "8"): "bj"
            }
            for prefixes, prefix in prefix_mapping.items():
                if stock_code.startswith(prefixes):
                    stock_code = f"{prefix}{stock_code}"
                    break
            else:
                logging.warning(f"非法股票代码格式: {original_code}")
                return None
            
            if not _STOCK_CODE_PATTERN.match(stock_code):
                logging.warning(f"修正后仍为非法代码: {original_code}->{stock_code}")
                return None

        # 网络请求配置
        headers = {"User-Agent": random.choice(_USER_AGENTS)}
        proxies = {
            "http": os.getenv("HTTP_PROXY"),
            "https": os.getenv("HTTPS_PROXY")
        }

        # 移除内层重试机制，仅保留外层统一重试
        df = ak.stock_financial_analysis_indicator(
            stock=stock_code,
            headers=headers,
            proxies=proxies
        )

        # 合并空数据校验
        if df is None or df.empty:
            logging.debug(f"空数据: {stock_code}")
            return None

        # 增强列校验
        if not all(col in df.columns for col in _REQUIRED_COLUMNS):
            logging.warning(f"缺失关键列: {stock_code}")
            return None

        # 数据清洗优化
        df = df.copy()
        df['指标名称'] = df['指标名称'].astype(str).str.strip()
        
        # 增强数值转换健壮性
        df['指标值'] = (
            df['指标值']
            .astype(str)
            .str.replace('[％%]', '', regex=True)
            .str.replace(',', '', regex=False)  # 处理千分位逗号
            .replace({'--': np.nan, '-': np.nan, 'nan': np.nan})
            .apply(pd.to_numeric, errors='coerce')  # 安全类型转换
            / 100
        )

        # 指标校验优化
        existing_indicators = set(df['指标名称'])
        if not all(ind in existing_indicators for ind in _REQUIRED_INDICATORS):
            logging.debug(f"缺失关键指标: {stock_code}")
            return None

        return df.set_index('指标名称')

    except requests.exceptions.RequestException as e:
        logging.warning(f"网络请求失败 {stock_code}: {e}")
    except ValueError as e:
        logging.error(f"数据类型转换异常 {stock_code}: {e}")

def get_stock_kline(stock_code: str) -> Optional[pd.DataFrame]:
    """获取个股K线数据(自动延长获取时间直到数据足够)"""
    try:
        # 尝试获取足够长的历史数据
        df = ak.stock_zh_a_hist(
            symbol=stock_code, period="daily",
            adjust="qfq", start_date="20100101"
        )
        if len(df) < CONFIG['min_days']:
            logging.debug(f"数据不足{CONFIG['min_days']}天 {stock_code}")
            return None
        # 标准化列名
        df = df[['日期', '收盘', '成交量']].rename(columns={
            '日期': 'date', '收盘': 'close', '成交量': 'volume'
        })
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').tail(CONFIG['min_days'] * 2)  # 保留足够数据
    except Exception as e:
        logging.warning(f"K线数据获取失败 {stock_code}: {e}")
        return None


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算技术指标(向量化操作提升效率)"""
    df['ma_250'] = df['close'].rolling(250, min_periods=1).mean()
    # MACD计算
    df['macd'], df['signal'], _ = talib.MACD(
        df['close'], fastperiod=12, slowperiod=26, signalperiod=9
    )
    # 成交量均线
    df['volume_ma_5'] = df['volume'].rolling(5).mean()
    return df.dropna()


def analyze_stock(stock_code: str) -> Optional[Dict]:
    """分析单个股票是否符合条件"""
    try:
        # 获取财务数据
        finance_df = get_financial_data(stock_code)
        if finance_df is None:
            return None

        # 基本面分析
        try:
            roe = finance_df.loc['ROE(净资产收益率·摊薄)'].iloc[:3].mean()
            profit_growth = finance_df.loc['净利润增长率'].iloc[:3].mean()
            cash_flow_ratio = finance_df.loc['经营活动产生的现金流量净额/EBIT'].iloc[:5].mean()
            debt_ratio = finance_df.loc['资产负债率'].iloc[0]
            dividend_yield = finance_df.loc['股息率'].iloc[0]
        except KeyError as e:
            logging.debug(f"财务指标缺失 {stock_code}: {e}")
            return None

        # 检查基本面条件
        if not (
                roe > CONFIG['roe_threshold'] and
                profit_growth > CONFIG['profit_growth_threshold'] and
                cash_flow_ratio > CONFIG['cash_flow_threshold'] and
                debt_ratio < CONFIG['debt_ratio_threshold'] and
                dividend_yield > CONFIG['dividend_yield_threshold']
        ):
            return None

        # 技术面分析
        kline_df = get_stock_kline(stock_code)
        if kline_df is None or len(kline_df) < CONFIG['min_days']:
            return None

        kline_df = calculate_technical_indicators(kline_df)
        if kline_df.empty:
            return None

        # 检查技术面条件
        latest = kline_df.iloc[-1]
        prev = kline_df.iloc[-2] if len(kline_df) >= 2 else None

        # MACD金叉条件
        macd_cross = (latest['macd'] > latest['signal']) and (
                (prev is None) or (prev['macd'] <= prev['signal'])
        )

        if (
                latest['close'] > latest['ma_250'] and
                macd_cross and
                latest['volume'] > (latest['volume_ma_5'] * CONFIG['volume_increase_ratio'])
        ):
            return {
                'code': stock_code,
                'name': get_stock_list().set_index('code').loc[stock_code, 'name'],
                'close': latest['close'],
                'ma_250': latest['ma_250'],
                'macd': latest['macd'],
                'signal': latest['signal'],
                'volume_ratio': latest['volume'] / latest['volume_ma_5']
            }
    except Exception as e:
        logging.error(f"分析股票时发生错误 {stock_code}: {e}", exc_info=True)
    return None


def main():
    try:
        stock_list = get_stock_list()
        logging.info(f"开始筛选 {len(stock_list)} 只股票")

        selected_stocks = []
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=CONFIG['max_workers']
        ) as executor:
            futures = {
                executor.submit(analyze_stock, row['code']): row
                for _, row in stock_list.iterrows()
            }
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    selected_stocks.append(result)
                    logging.info(
                        f"筛选到股票: {result['code']} {result['name']} "
                        f"收盘价:{result['close']:.2f} MACD:{result['macd']:.2f}"
                    )

        logging.info(f"筛选完成，共找到 {len(selected_stocks)} 只符合条件股票")
        if selected_stocks:
            result_df = pd.DataFrame(selected_stocks)
            print("\n筛选结果:")
            print(result_df[['code', 'name', 'close', 'ma_250']])
            result_df.to_csv('selected_stocks.csv', index=False)
        return selected_stocks
    except Exception as e:
        logging.error(f"主程序错误: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

# 预编译正则表达式和常量
_STOCK_CODE_PATTERN = re.compile(r"^(sh|sz|bj)\d{6}$")
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"
]
_REQUIRED_COLUMNS = ["指标名称", "指标值"]
_REQUIRED_INDICATORS = [
    'ROE(净资产收益率·摊薄)',
    '净利润增长率',
    '经营活动产生的现金流量净额/EBIT',
    '资产负债率',
    '股息率'
]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20))
def get_financial_data(stock_code: str) -> Optional[pd.DataFrame]:
    """获取个股财务指标数据（增强稳定性版）"""
    try:
        # 股票代码格式校验与修正
        original_code = stock_code
        if not _STOCK_CODE_PATTERN.match(stock_code):
            prefix_mapping = {
                ("6", "9"): "sh",
                ("0", "3"): "sz",
                ("4", "8"): "bj"
            }
            for prefixes, prefix in prefix_mapping.items():
                if stock_code.startswith(prefixes):
                    stock_code = f"{prefix}{stock_code}"
                    break
            else:
                logging.warning(f"非法股票代码格式: {original_code}")
                return None
            
            if not _STOCK_CODE_PATTERN.match(stock_code):
                logging.warning(f"修正后仍为非法代码: {original_code}->{stock_code}")
                return None

        # 网络请求配置
        headers = {"User-Agent": random.choice(_USER_AGENTS)}
        proxies = {
            "http": os.getenv("HTTP_PROXY"),
            "https": os.getenv("HTTPS_PROXY")
        }

        # 移除内层重试机制，仅保留外层统一重试
        df = ak.stock_financial_analysis_indicator(
            stock=stock_code,
            headers=headers,
            proxies=proxies
        )

        # 合并空数据校验
        if df is None or df.empty:
            logging.debug(f"空数据: {stock_code}")
            return None

        # 增强列校验
        if not all(col in df.columns for col in _REQUIRED_COLUMNS):
            logging.warning(f"缺失关键列: {stock_code}")
            return None

        # 数据清洗优化
        df = df.copy()
        df['指标名称'] = df['指标名称'].astype(str).str.strip()
        
        # 增强数值转换健壮性
        df['指标值'] = (
            df['指标值']
            .astype(str)
            .str.replace('[％%]', '', regex=True)
            .str.replace(',', '', regex=False)  # 处理千分位逗号
            .replace({'--': np.nan, '-': np.nan, 'nan': np.nan})
            .apply(pd.to_numeric, errors='coerce')  # 安全类型转换
            / 100
        )

        # 指标校验优化
        existing_indicators = set(df['指标名称'])
        if not all(ind in existing_indicators for ind in _REQUIRED_INDICATORS):
            logging.debug(f"缺失关键指标: {stock_code}")
            return None

        return df.set_index('指标名称')

    except requests.exceptions.RequestException as e:
        logging.warning(f"网络请求失败 {stock_code}: {e}")
    except ValueError as e:
        logging.error(f"数据类型转换异常 {stock_code}: {e}")


# 预编译正则表达式和常量
_STOCK_CODE_PATTERN = re.compile(r"^(sh|sz|bj)\d{6}$")
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15"
]
_REQUIRED_COLUMNS = ["指标名称", "指标值"]
_REQUIRED_INDICATORS = [
    'ROE(净资产收益率·摊薄)',
    '净利润增长率',
    '经营活动产生的现金流量净额/EBIT',
    '资产负债率',
    '股息率'
]

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=20))
def get_financial_data(stock_code: str) -> Optional[pd.DataFrame]:
    """获取个股财务指标数据（增强稳定性版）"""
    try:
        # 股票代码格式校验与修正
        original_code = stock_code
        if not _STOCK_CODE_PATTERN.match(stock_code):
            prefix_mapping = {
                ("6", "9"): "sh",
                ("0", "3"): "sz",
                ("4", "8"): "bj"
            }
            for prefixes, prefix in prefix_mapping.items():
                if stock_code.startswith(prefixes):
                    stock_code = f"{prefix}{stock_code}"
                    break
            else:
                logging.warning(f"非法股票代码格式: {original_code}")
                return None
            
            if not _STOCK_CODE_PATTERN.match(stock_code):
                logging.warning(f"修正后仍为非法代码: {original_code}->{stock_code}")
                return None

        # 网络请求配置
        headers = {"User-Agent": random.choice(_USER_AGENTS)}
        proxies = {
            "http": os.getenv("HTTP_PROXY"),
            "https": os.getenv("HTTPS_PROXY")
        }

        # 移除内层重试机制，仅保留外层统一重试
        df = ak.stock_financial_analysis_indicator(
            stock=stock_code,
            headers=headers,
            proxies=proxies
        )

        # 合并空数据校验
        if df is None or df.empty:
            logging.debug(f"空数据: {stock_code}")
            return None

        # 增强列校验
        if not all(col in df.columns for col in _REQUIRED_COLUMNS):
            logging.warning(f"缺失关键列: {stock_code}")
            return None

        # 数据清洗优化
        df = df.copy()
        df['指标名称'] = df['指标名称'].astype(str).str.strip()
        
        # 增强数值转换健壮性
        df['指标值'] = (
            df['指标值']
            .astype(str)
            .str.replace('[％%]', '', regex=True)
            .str.replace(',', '', regex=False)  # 处理千分位逗号
            .replace({'--': np.nan, '-': np.nan, 'nan': np.nan})
            .apply(pd.to_numeric, errors='coerce')  # 安全类型转换
            / 100
        )

        # 指标校验优化
        existing_indicators = set(df['指标名称'])
        if not all(ind in existing_indicators for ind in _REQUIRED_INDICATORS):
            logging.debug(f"缺失关键指标: {stock_code}")
            return None

        return df.set_index('指标名称')

    except requests.exceptions.RequestException as e:
        logging.warning(f"网络请求失败 {stock_code}: {e}")
    except ValueError as e:
        logging.error(f"数据类型转换异常 {stock_code}: {e}")