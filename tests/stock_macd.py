import akshare as ak
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import matplotlib.dates as mdates  # 需要添加这个导入
# 参数设置
initial_capital = 1000000  # 初始资金100万
short_window = 5           # 短期均线周期
long_window = 20           # 长期均线周期
transaction_cost = 0.001   # 交易手续费千分之一

# 获取股票数据
def get_stock_data():
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=1*365)).strftime('%Y%m%d')
    # start_date = (datetime.now() - timedelta(days=1*180)).strftime('%Y%m%d')
    # start_date = (datetime.now() - timedelta(days=1*90)).strftime('%Y%m%d')
    
    df = ak.stock_zh_a_hist(
        symbol="600133",  # 东湖高新
        # symbol="002261",  # 拓维信息
        # symbol="000977",  # 浪潮信息
        # symbol="600588",  # 埃斯顿
        # symbol="002747",  # 用友网络
        # symbol="601012",  # 隆基绿能
        # symbol="600597",  # 光明乳业

        period="daily",
        start_date=start_date,
        end_date=end_date,
        # adjust="hfq"      # 使用后复权数据
         adjust="qfq"  # 改为前复权
    )
    
    # 转换日期格式并设置为索引
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.set_index('日期').sort_index()
    return df[['开盘', '最高', '最低', '收盘', '成交量']]

# 计算技术指标
def calculate_technical(df):
    df['short_ma'] = df['收盘'].rolling(window=short_window).mean()
    df['long_ma'] = df['收盘'].rolling(window=long_window).mean()
    df['signal'] = np.where(df['short_ma'] > df['long_ma'], 1, 0)
    df['position'] = df['signal'].diff()
    return df.dropna()
def backtest(df):
    capital = initial_capital
    position = 0
    portfolio = pd.DataFrame(index=df.index)
    portfolio['close'] = df['收盘']
    portfolio['cash'] = capital
    portfolio['shares'] = 0
    portfolio['total'] = capital
    portfolio['signal'] = 0

    for i in range(len(df)):  # 遍历所有数据点
        # 当前信号（使用当日信号）
        current_signal = df['position'].iloc[i]
        current_price = df['收盘'].iloc[i]
        
        # 执行交易
        if current_signal == 1:  # 金叉当日买入
            if position == 0:
                # 计算可买数量（向下取整）
                shares_bought = capital // (current_price * (1 + transaction_cost))
                if shares_bought > 0:
                    cost = shares_bought * current_price * (1 + transaction_cost)
                    capital -= cost
                    position = shares_bought
        elif current_signal == -1:  # 死叉当日卖出
            if position > 0:
                proceeds = position * current_price * (1 - transaction_cost)
                capital += proceeds
                position = 0
        
        # 更新组合价值
        portfolio.iloc[i, 1] = capital
        portfolio.iloc[i, 2] = position
        portfolio.iloc[i, 3] = capital + position * current_price
        portfolio.iloc[i, 4] = current_signal
    
    return portfolio

# 性能分析
def analyze_performance(portfolio):
    # 计算收益率
    portfolio['returns'] = portfolio['total'].pct_change()
    
    # 总收益率
    total_return = (portfolio['total'][-1] / initial_capital - 1) * 100
    
    # 年化收益率
    years = len(portfolio) / 252
    annualized_return = (portfolio['total'][-1] / initial_capital) ** (1/years) - 1
    
    # 最大回撤
    portfolio['peak'] = portfolio['total'].cummax()
    portfolio['drawdown'] = (portfolio['total'] - portfolio['peak']) / portfolio['peak']
    max_drawdown = portfolio['drawdown'].min() * 100

    # 胜率
    winning_trades = len(portfolio[portfolio['returns'] > 0])
    total_trades = len(portfolio['returns'].dropna())
    win_rate = winning_trades / total_trades * 100

    print(f"最终资产: {portfolio['total'][-1]:.2f}")
    print(f"总收益率: {total_return:.2f}%")
    print(f"年化收益率: {annualized_return*100:.2f}%")
    print(f"最大回撤: {max_drawdown:.2f}%")
    print(f"胜率: {win_rate:.2f}%")


def visualize_results(portfolio):
    plt.figure(figsize=(12, 10))
    
    # 资金曲线
    ax1 = plt.subplot(211)
    ax1.plot(portfolio['total'], label='Portfolio Value')
    ax1.set_title('Portfolio Performance')
    ax1.set_ylabel('Value')
    ax1.legend()
    
    # 设置日期格式
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')  # 旋转45度
    
    # 买卖信号
    ax2 = plt.subplot(212)
    ax2.plot(portfolio['close'], label='Price')
    buy_signals = portfolio[portfolio['signal'] == 1]
    sell_signals = portfolio[portfolio['signal'] == -1]
    ax2.plot(buy_signals.index, buy_signals['close'], '^', markersize=10, color='g', label='Buy')
    ax2.plot(sell_signals.index, sell_signals['close'], 'v', markersize=10, color='r', label='Sell')
    ax2.set_title('Trading Signals')
    ax2.set_ylabel('Price')
    ax2.legend()
    
    # 设置日期格式
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax2.get_xticklabels(), rotation=45, ha='right')  # 旋转45度
    
    plt.tight_layout()
    plt.show()

if __name__ == '__main__':
    # 获取并处理数据
    data = get_stock_data()
    data = calculate_technical(data)
    
    # 运行回测
    portfolio = backtest(data)
    
    # 分析结果
    analyze_performance(portfolio)
    
    # 可视化
    visualize_results(portfolio)