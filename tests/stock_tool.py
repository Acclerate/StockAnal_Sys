import ctypes
import logging
import time
import os
from enum import Enum
from typing import Dict, Optional, Tuple

import akshare as ak
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from cachetools import cached, TTLCache
# 配置日志系统
def setup_logging():
    """初始化日志配置"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    # 创建每日日志文件
    log_filename = os.path.join(log_dir, f"{time.strftime('%Y-%m-%d')}.log")

    formatter = logging.Formatter(
        fmt='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 文件处理器（按日生成）
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers = [file_handler, console_handler]

    return log_filename

# 初始化日志并获取日志路径
log_file_path = setup_logging()

# 其他常量配置
CACHE_TTL = 120
stock_cache = TTLCache(maxsize=256, ttl=CACHE_TTL)

# 在常量配置区域添加
WMICON_NOTIFY = 0x00000010  # 通知图标标志
NIIF_INFO = 0x00000001      # 信息类型图标
# 在常量配置区域添加交易时间配置（约第30行后）
TRADING_HOURS = {
    "morning_open": (9, 30),
    "morning_close": (11, 30),
    "afternoon_open": (13, 0),
    "afternoon_close": (15, 0)
}

def is_trading_time() -> bool:
    """判断当前是否处于A股交易时间"""
    now = time.localtime()
    weekday = now.tm_wday  # 周一(0) 到 周五(4)，周六(5)，周日(6)

    # 周末不交易
    if weekday >= 5:
        return False

    current_hour = now.tm_hour
    current_min = now.tm_min

    # 上午交易时段 9:30-11:30
    if (current_hour == 9 and current_min >= 30) or \
            (10 <= current_hour < 11) or \
            (current_hour == 11 and current_min <= 30):
        return True

    # 下午交易时段 13:00-15:00
    if (current_hour == 13 and current_min >= 0) or \
            (14 <= current_hour < 15) or \
            (current_hour == 15 and current_min == 0):
        return True

    return False


def get_next_check_interval() -> float:
    """计算到下一个交易时段需要等待的秒数"""
    now = time.localtime()
    next_check = 300  # 默认5分钟

    current_time = (now.tm_hour, now.tm_min)
    morning_open = TRADING_HOURS["morning_open"]
    afternoon_open = TRADING_HOURS["afternoon_open"]

    if current_time < morning_open:
        # 早于9:30，等到9:30
        next_check = (morning_open[0] - now.tm_hour) * 3600 + (morning_open[1] - now.tm_min) * 60
    elif current_time >= TRADING_HOURS["afternoon_close"]:
        # 下午收盘后，等到次日9:30
        next_day = 86400 - (now.tm_hour * 3600 + now.tm_min * 60 + now.tm_sec)
        next_check = next_day + morning_open[0] * 3600 + morning_open[1] * 60
    elif TRADING_HOURS["morning_close"] < current_time < afternoon_open:
        # 午间休市，等到13:00
        next_check = (afternoon_open[0] - now.tm_hour) * 3600 + (afternoon_open[1] - now.tm_min) * 60

    return max(60, next_check)  # 至少等待1分钟
# 添加通知函数
def show_windows_notification(title: str, msg: str):
    """显示Windows通知"""
    try:
        ctypes.windll.user32.MessageBoxW(0, msg, title, 0x40)  # 0x40是信息图标
    except Exception as e:
        logging.error(f"通知发送失败: {str(e)}")

# 修改日志格式配置（增加毫秒显示）
logging.basicConfig(
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
# 常量配置
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://data.eastmoney.com/",
}

RETRY_STRATEGY = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS"]
)


class MarketType(Enum):
    A_SHANGHAI = "sh"
    A_SHENZHEN = "sz"
    US = "us"
    HK = "hk"


STOCK_PREFIX_MAP = {
    '6': MarketType.A_SHANGHAI,
    '5': MarketType.A_SHANGHAI,
    '0': MarketType.A_SHENZHEN,
    '3': MarketType.A_SHENZHEN,
    '9': MarketType.A_SHANGHAI,  # 科创板
    '00700': MarketType.HK,  # 港股示例
    'AAPL': MarketType.US  # 美股示例
}

# 缓存配置
CACHE_TTL = 10  # 秒


def create_retry_session() -> requests.Session:
    """创建带重试机制的会话"""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=RETRY_STRATEGY)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session


def format_stock_code(stock_code: str) -> Tuple[str, MarketType]:
    """标准化股票代码格式"""
    code = stock_code.upper().lstrip('.SHZ')
    for prefix, market in STOCK_PREFIX_MAP.items():
        if code.startswith(prefix):
            return code, market
    # 默认处理A股
    if len(code) == 6:
        prefix = code[0]
        return code, STOCK_PREFIX_MAP.get(prefix, MarketType.A_SHANGHAI)
    return code, MarketType.A_SHANGHAI


@cached(stock_cache)
def get_cached_stock_data(market: MarketType) -> pd.DataFrame:
    """带市场区分的缓存数据获取"""
    try:
        # 获取全市场A股数据
        df = ak.stock_zh_a_spot_em()
        # 根据市场筛选数据
        if market == MarketType.A_SHANGHAI:
            return df[df['代码'].str.startswith(('6', '5', '9'))]
        elif market == MarketType.A_SHENZHEN:
            return df[df['代码'].str.startswith(('0', '3'))]
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"Akshare数据获取失败: {str(e)}", exc_info=True)
        return pd.DataFrame()


def get_stock_realtime_price(stock_code: str) -> Optional[Dict]:
    """主API获取实时股价"""
    try:
        code, market = format_stock_code(stock_code)
        df = get_cached_stock_data(market)

        if not df.empty:
            target = df[df['代码'].str.contains(code, case=False)]
            if not target.empty:
                return format_a_share_data(target.iloc[0])
        return None
    except Exception as e:
        logging.error(f"主API查询失败: {str(e)}", exc_info=True)
        return None


def get_stock_price_backup(stock_code: str) -> Optional[Dict]:
    """备用API获取股价数据"""
    try:
        code, market = format_stock_code(stock_code)
        params = build_eastmoney_params(code, market)

        with create_retry_session() as session:
            response = session.get(
                url="https://push2.eastmoney.com/api/qt/stock/get",
                params=params,
                headers=HEADERS,
                timeout=10
            )
            response.raise_for_status()
            return parse_eastmoney_data(response.json(), code, market)
    except Exception as e:
        logging.error(f"备用API查询失败: {str(e)}", exc_info=True)
        return None


def build_eastmoney_params(code: str, market: MarketType) -> Dict:
    """构建东方财富接口参数"""
    market_map = {
        MarketType.A_SHANGHAI: ("1", code),
        MarketType.A_SHENZHEN: ("0", code),
        MarketType.US: ("105", code.split('.')[-1]),  # 示例：美股AAPL
        MarketType.HK: ("116", code)  # 示例：港股00700
    }
    market_id, sec_code = market_map.get(market, ("1", code))

    return {
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "invt": 2,
        "fltt": 2,
        "fields": "f43,f57,f58,f169,f170,f46,f44,f51,f168,f47,f164,f116,f60,f45,f52,f50,f48,f167,f117,f71,f161,f49,f530,f135,f136,f137,f138,f139,f141,f142,f144,f145,f147,f148,f140,f143,f146,f149,f55,f62,f162,f92,f173,f104,f105,f84,f85,f183,f184,f185,f186,f187,f188,f189,f190,f191,f192,f107,f111,f86,f177,f78,f110,f262,f263,f264,f267,f268,f255,f256,f257,f258,f127,f199,f128,f198,f259,f260,f261,f171,f277,f278,f279,f288,f152,f250,f251,f252,f253,f254,f269,f270,f271,f272,f273,f274,f275,f276,f265,f266,f289,f290,f286,f285,f292,f293,f294,f295",
        "secid": f"{market_id}.{sec_code}",
        "_": int(time.time() * 1000)
    }


def format_a_share_data(row: pd.Series) -> Dict:
    """格式化A股数据"""
    return {
        "代码": row['代码'],
        "名称": row['名称'],
        "当前价": round(float(row['最新价']), 2),
        "涨跌幅(%)": round(float(row['涨跌幅']), 2),
        "成交量(手)": int(row['成交量']),
        "市场类型": "A股",
        "数据源": "主API"
    }


def parse_eastmoney_data(data: Dict, code: str, market: MarketType) -> Optional[Dict]:
    """解析东方财富返回数据"""
    if data.get("rc") == 0 and data.get("data"):
        item = data["data"]
        return {
            "代码": code,
            "名称": item.get("f58", code),
            "当前价": round(item.get("f43", 0), 2),
            "涨跌幅(%)": round(item.get("f170", 0), 2),
            "成交量(手)": int(item.get("f47", 0)),
            "市场类型": market.value,
            "数据源": "备用API"
        }
    return None


def get_stock_price(stock_code: str) -> Optional[Dict]:
    """统一获取股票价格入口"""
    strategies = [get_stock_realtime_price, get_stock_price_backup]

    for strategy in strategies:
        for retry in range(2):
            if result := strategy(stock_code):
                return result
            time.sleep(0.5 * (retry + 1))
    logging.warning(f"所有数据源获取失败: {stock_code}")
    return None


if __name__ == "__main__":
    logging.info(f"程序启动，日志文件路径：{os.path.abspath(log_file_path)}")
    test_cases = [
        "002261",  # 拓维信息
        "000977",  # 浪潮信息
        "600133",  # 东湖高新
        "600588",  # 用友网络
        "002747",  # 用友网络
        "601012"  # 隆基绿能
        # "600597"  # 光明乳业
    ]

    while True:
        if not is_trading_time():
            wait_seconds = get_next_check_interval()
            logging.info(f"非交易时间段，{wait_seconds // 60}分钟后重试...")
            time.sleep(wait_seconds)
            continue

        cycle_start = time.time()
        # 在每次循环开始时清除缓存
        stock_cache.clear()  # 强制刷新数据

        logging.info(
            f"------------------------------------ {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())} ---------------------------------------------- \n")
        for code in test_cases:
            start_time = time.time()
            result = get_stock_price(code)
            elapsed = time.time() - start_time
            if result:
                current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                logging.info(f"[{current_time}] [{elapsed:.2f}s] {result['名称']}({result['代码']}) "
                             f"当前价: {result['当前价']} | 涨跌幅: {result['涨跌幅(%)']}% "
                             f"| 市场: {result['市场类型']}")


            else:
                logging.info(f"[{elapsed:.2f}s] 股票 {code} 数据获取失败")

            # 添加特化价格提醒
            if result['代码'] == '600133' and float(result['当前价']) >= 10.9:
                show_windows_notification(
                    "价格提醒",
                    f"{result['名称']}({result['代码']}) 已达目标价\n当前价: {result['当前价']}\n" +
                    f"预设阈值: 10.9 | 涨跌幅: {result['涨跌幅(%)']}%"
                )
            time.sleep(0.5)  # 保持原有防刷间隔

        # 精确3分钟间隔控制
        elapsed = time.time() - cycle_start
        if elapsed < 180:
            time.sleep(180 - elapsed)
