import akshare as ak
import pandas as pd


def get_stock_history(stock_code: str, start_date: str, end_date: str):
    """新版数据获取函数"""
    try:
        # 使用通用接口替代旧方法
        df = ak.stock_zh_a_hist(
            symbol=stock_code,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq"
        )

        # 重命名字段
        df = df.rename(columns={
            "日期": "trade_date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume"
        })

        # 转换日期格式
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
        return df

    except Exception as e:
        print(f"数据获取失败: {str(e)}")
        return pd.DataFrame()


# 测试获取数据（使用有效日期）
data = get_stock_history("600133", "20240901", "20250318")
if not data.empty:
    print(f"最新数据日期: {data['trade_date'].iloc[-1]}")
    print(data.tail())
else:
    print("请检查：1.股票代码有效性 2.网络连接 3.日期范围")
