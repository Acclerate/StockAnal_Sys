import akshare as ak
import pandas as pd
import numpy as np
import talib
import time


def get_stock_list():
    """获取所有 A 股股票代码"""
    stock_df = ak.stock_info_a_code_name()
    return stock_df


def get_financial_data(stock_code):
    """获取个股财务数据"""
    try:
        finance_df = ak.stock_financial_analysis_indicator(symbol=stock_code)
        return finance_df
    except Exception as e:
        print(f"财务数据获取失败 {stock_code}: {e}")
        return None


def get_stock_kline(stock_code):
    """获取个股日 K 线数据"""
    try:
        df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", adjust="qfq")
        df = df[['日期', '收盘', '成交量']].rename(columns={'日期': 'date', '收盘': 'close', '成交量': 'volume'})
        df['date'] = pd.to_datetime(df['date'])
        df.sort_values('date', inplace=True)
        return df
    except Exception as e:
        print(f"K 线数据获取失败 {stock_code}: {e}")
        return None


def calculate_technical_indicators(df):
    """计算技术指标"""
    df['ma_250'] = df['close'].rolling(window=250).mean()
    df['macd'], df['signal'], _ = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    df['volume_ma_5'] = df['volume'].rolling(window=5).mean()
    return df


def stock_filter(stock_code):
    """筛选股票"""
    finance_df = get_financial_data(stock_code)
    if finance_df is None or finance_df.empty:
        return False

    try:
        roe = finance_df.loc[finance_df['指标名称'] == 'ROE(净资产收益率·摊薄)', '指标值'].astype(float).values[:3]
        profit_growth = finance_df.loc[finance_df['指标名称'] == '净利润增长率', '指标值'].astype(float).values[:3]
        free_cash_flow_ratio = finance_df.loc[
                                   finance_df['指标名称'] == '经营活动产生的现金流量净额/EBIT', '指标值'].astype(
            float).values[:5]
        debt_ratio = finance_df.loc[finance_df['指标名称'] == '资产负债率', '指标值'].astype(float).values[0]
        dividend_yield = finance_df.loc[finance_df['指标名称'] == '股息率', '指标值'].astype(float).values[0]

        if (roe.mean() > 10 and
                profit_growth.mean() > 15 and
                free_cash_flow_ratio.mean() > 50 and
                debt_ratio < 60 and
                dividend_yield > 5):

            df = get_stock_kline(stock_code)
            if df is None or df.empty:
                return False

            df = calculate_technical_indicators(df)
            latest = df.iloc[-1]

            if (latest['close'] > latest['ma_250'] and
                    latest['macd'] > latest['signal'] and
                    latest['volume'] > latest['volume_ma_5']):
                return True
    except Exception as e:
        print(f"数据计算失败 {stock_code}: {e}")
    return False


def main():
    stock_list = get_stock_list()
    selected_stocks = []

    for _, row in stock_list.iterrows():
        stock_code = row['code']
        print(f"正在分析 {stock_code} ...")
        if stock_filter(stock_code):
            selected_stocks.append(stock_code)
        time.sleep(1)

    print("筛选出的股票:", selected_stocks)
    return selected_stocks


if __name__ == "__main__":
    main()
