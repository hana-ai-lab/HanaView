import json
import logging
import logging.handlers
import os
import re
import sys
from datetime import datetime, timedelta, timezone
import pytz
import time
import math
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from curl_cffi.requests import Session
import openai
import httpx
from io import StringIO
from urllib.parse import urlparse
from .image_generator import generate_fear_greed_chart
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Constants ---
DATA_DIR = 'data'
RAW_DATA_PATH = os.path.join(DATA_DIR, 'data_raw.json')
FINAL_DATA_PATH_PREFIX = os.path.join(DATA_DIR, 'data_')

# URLs
CNN_FEAR_GREED_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata/"
YAHOO_FINANCE_NEWS_URL = "https://finance.yahoo.com/topic/stock-market-news/"
YAHOO_EARNINGS_CALENDAR_URL = "https://finance.yahoo.com/calendar/earnings"
SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ100_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"

# Monex URLs
MONEX_ECONOMIC_CALENDAR_URL = "https://mst.monex.co.jp/pc/servlet/ITS/report/EconomyIndexCalendar"
MONEX_US_EARNINGS_URL = "https://mst.monex.co.jp/mst/servlet/ITS/fi/FIClosingCalendarUSGuest"
MONEX_JP_EARNINGS_URL = "https://mst.monex.co.jp/mst/servlet/ITS/fi/FIClosingCalendarJPGuest"

# Tickers
VIX_TICKER = "^VIX"
T_NOTE_TICKER = "^TNX"

WORLD_INDICES = {
    "日本": [
        {"name": "日経平均", "ticker": "^N225", "country_code": "JP", "description": "東証 日経225平均"},
        {"name": "日経時間外", "ticker": "NIY=F", "country_code": "JP", "description": "Nikkei/Yen Futures"},
    ],
    "米国": [
        {"name": "ダウ", "ticker": "^DJI", "country_code": "US", "description": "ダウ30"},
        {"name": "ナスダック", "ticker": "^IXIC", "country_code": "US", "description": "ナスダック総合"},
        {"name": "S&P500", "ticker": "^GSPC", "country_code": "US", "description": "S&P500"},
        {"name": "SOX指数", "ticker": "^SOX", "country_code": "US", "description": "フィラデルフィア半導体指数"},
        {"name": "VIX", "ticker": "^VIX", "country_code": "US", "description": "VIX恐怖指数"},
        {"name": "ラッセル2000", "ticker": "^RUT", "country_code": "US", "description": "ラッセル2000"},
    ],
    "債券": [
        {"name": "米国債10年", "ticker": "^TNX", "country_code": "US", "description": "10年債利回り"},
    ],
    "為替": [
        {"name": "ドル円", "ticker": "JPY=X", "country_code": "FX", "description": "USD/JPY"},
        {"name": "ユーロ円", "ticker": "EURJPY=X", "country_code": "FX", "description": "EUR/JPY"},
        {"name": "ポンド円", "ticker": "GBPJPY=X", "country_code": "FX", "description": "GBP/JPY"},
        {"name": "豪ドル円", "ticker": "AUDJPY=X", "country_code": "FX", "description": "AUD/JPY"},
        {"name": "カナダドル円", "ticker": "CADJPY=X", "country_code": "FX", "description": "CAD/JPY"},
        {"name": "スイスフラン円", "ticker": "CHFJPY=X", "country_code": "FX", "description": "CHF/JPY"},
    ],
    "コモディティ": [
        {"name": "ゴールド", "ticker": "GC=F", "country_code": "CM", "description": "金先物 (USD)"},
        {"name": "ゴールド(円)", "ticker": "GOLD-JPY", "country_code": "CM", "description": "金 (円換算)", "calculated": True},
        {"name": "WTI原油", "ticker": "CL=F", "country_code": "CM", "description": "原油先物"},
        {"name": "北海ブレント", "ticker": "BZ=F", "country_code": "CM", "description": "ブレント原油先物"},
        {"name": "天然ガス", "ticker": "NG=F", "country_code": "CM", "description": "天然ガス先物"},
        {"name": "銅", "ticker": "HG=F", "country_code": "CM", "description": "銅先物"},
        {"name": "ビットコイン", "ticker": "BTC-USD", "country_code": "CM", "description": "Bitcoin"},
        {"name": "イーサリアム", "ticker": "ETH-USD", "country_code": "CM", "description": "Ethereum"},
    ],
    "北東アジア": [
        {"name": "上海総合", "ticker": "000001.SS", "country_code": "CN", "description": "上海総合指数"},
        {"name": "CSI300", "ticker": "000300.SS", "country_code": "CN", "description": "CSI300指数"},
        {"name": "韓国 KOSPI", "ticker": "^KS11", "country_code": "KR", "description": "韓国総合株価指数"},
        {"name": "香港 ハンセン", "ticker": "^HSI", "country_code": "HK", "description": "香港ハンセン指数"},
        {"name": "台湾 加権", "ticker": "^TWII", "country_code": "TW", "description": "台湾加権指数"},
    ],
    "欧州": [
        {"name": "イギリス FTSE", "ticker": "^FTSE", "country_code": "GB", "description": "FTSE100指数"},
        {"name": "ドイツ DAX", "ticker": "^GDAXI", "country_code": "DE", "description": "DAX指数"},
        {"name": "フランス CAC40", "ticker": "^FCHI", "country_code": "FR", "description": "CAC40指数"},
        {"name": "イタリア MIB", "ticker": "FTSEMIB.MI", "country_code": "IT", "description": "FTSE MIB指数"},
        {"name": "スイス SMI", "ticker": "^SSMI", "country_code": "CH", "description": "SMI指数"},
    ],
    "ピックアップ": [
        {"name": "FANG+", "ticker": "^NYFANG", "country_code": "US", "description": "NYSE FANG+ Index"},
        {"name": "全世界株式 オルカン", "ticker": "ACWI", "country_code": "US", "description": "MSCI ACWI"},
    ],
    "新興アジア": [
        {"name": "インド Nifty", "ticker": "^NSEI", "country_code": "IN", "description": "NIFTY 50"},
        {"name": "マレーシア KLCI", "ticker": "^KLSE", "country_code": "MY", "description": "FTSE Bursa Malaysia KLCI"},
        {"name": "タイ SET指数", "ticker": "^SET.BK", "country_code": "TH", "description": "SET Index"},
        {"name": "ベトナム", "ticker": "^VNINDEX.VN", "country_code": "VN", "description": "VN-Index"},
        {"name": "シンガポールSTI", "ticker": "^STI", "country_code": "SG", "description": "Straits Times Index"},
        {"name": "インドネシアJKSE", "ticker": "^JKSE", "country_code": "ID", "description": "Jakarta Composite Index"},
    ],
    "オセアニア": [
        {"name": "オーストラリアASX", "ticker": "^AXJO", "country_code": "AU", "description": "S&P/ASX 200"},
        {"name": "ニュージーランド", "ticker": "^NZ50", "country_code": "NZ", "description": "S&P/NZX 50"},
    ],
    "アメリカ大陸": [
        {"name": "カナダS&Pトロント総合", "ticker": "^GSPTSE", "country_code": "CA", "description": "S&P/TSX Composite"},
        {"name": "メキシコIPC", "ticker": "^MXX", "country_code": "MX", "description": "S&P/BMV IPC"},
        {"name": "アルゼンチン メンバル", "ticker": "^MERV", "country_code": "AR", "description": "S&P Merval"},
        {"name": "ブラジル ボベスパ", "ticker": "^BVSP", "country_code": "BR", "description": "IBOVESPA"},
    ],
    "中東･アフリカ": [
        {"name": "トルコ Borsa100", "ticker": "XU100.IS", "country_code": "TR", "description": "BIST 100"},
        {"name": "ドバイ UAE", "ticker": "DFMGI.AE", "country_code": "AE", "description": "DFM General Index"},
        {"name": "サウジアラビア", "ticker": "^TASI.SR", "country_code": "SA", "description": "Tadawul All Share Index"},
    ],
}

# --- Error Handling ---
class MarketDataError(Exception):
    """Custom exception for data fetching and processing errors."""
    def __init__(self, code, message=None):
        self.code = code
        self.message = message or ERROR_CODES.get(code, "An unknown error occurred.")
        super().__init__(f"[{self.code}] {self.message}")

ERROR_CODES = {
    "E001": "OpenAI API key is not configured.",
    "E002": "Data file could not be read.",
    "E003": "Failed to connect to an external API.",
    "E004": "Failed to fetch Fear & Greed Index data.",
    "E005": "AI content generation failed.",
    "E006": "Failed to fetch heatmap data.",
    "E007": "Failed to fetch calendar data via Selenium.",
}

# --- Logging Configuration ---
LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'app.log')

# Create a stream handler for console output
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)

# Create a formatter and set it for both handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

# Get the root logger and add handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Avoid adding handlers multiple times if this module is reloaded
if not logger.handlers:
    logger.addHandler(stream_handler)


# --- Main Data Fetching Class ---
class MarketDataFetcher:
    def __init__(self):
        # curl_cffiのSessionを使用してブラウザを偽装
        self.http_session = Session(impersonate="chrome110", headers={'Accept-Language': 'en-US,en;q=0.9'})
        # yfinance用のセッションも別途作成
        self.yf_session = Session(impersonate="safari15_5")
        self.data = {"market": {}, "news": [], "indicators": {"economic": [], "us_earnings": [], "jp_earnings": []}}
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.warning(f"[E001] {ERROR_CODES['E001']} AI functions will be skipped.")
            self.openai_client = None
            self.openai_model = None
        else:
            http_client = httpx.Client(trust_env=False)
            self.openai_client = openai.OpenAI(api_key=api_key, http_client=http_client)
            self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4-turbo") # Fallback for safety

    def _clean_non_compliant_floats(self, obj):
        if isinstance(obj, dict):
            return {k: self._clean_non_compliant_floats(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._clean_non_compliant_floats(elem) for elem in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    def _generate_svg_chart_data(self, history_df):
        if history_df.empty or 'Close' not in history_df.columns:
            return "", 0, 0

        prices = history_df['Close'].dropna().tolist()
        if not prices:
            return "", 0, 0

        min_price = min(prices)
        max_price = max(prices)
        price_range = max_price - min_price

        num_points = len(prices)
        svg_points = []

        for i, price in enumerate(prices):
            x = (i / (num_points - 1)) * 85 if num_points > 1 else 42.5

            if price_range == 0:
                y = 50
            else:
                normalized_y = (price - min_price) / price_range
                y = 97 - (normalized_y * 94)

            svg_points.append(f"{x:.2f},{y:.2f}")

        return " ".join(svg_points), min_price, max_price

    def fetch_world_indices_data(self):
        """Fetches data for world indices for the new 'World' tab, organized by category."""
        logger.info("Fetching world indices data...")
        world_data = {}
        gold_usd_price = None
        usdjpy_price = None
        gold_svg_chart_data = ""
        gold_min_val = 0
        gold_max_val = 0

        for category, indices in WORLD_INDICES.items():
            category_data = []
            for index_info in indices:
                name = index_info["name"]
                ticker_symbol = index_info["ticker"]

                if index_info.get("calculated"):
                    continue

                try:
                    ticker = yf.Ticker(ticker_symbol, session=self.yf_session)
                    # Fetch more data points for a smoother chart
                    hist_data = ticker.history(period="5d", interval="1h")

                    if hist_data.empty or len(hist_data['Close'].dropna()) < 2:
                        logger.warning(f"No 5d history for {name} ({ticker_symbol}), using daily.")
                        hist_data = ticker.history(period="3mo", interval="1d")
                        if hist_data.empty or len(hist_data['Close'].dropna()) < 2:
                             logger.error(f"Not enough data for {name} ({ticker_symbol}), skipping.")
                             continue

                    # Data processing
                    close_prices = hist_data['Close'].dropna()
                    current_price = close_prices.iloc[-1]
                    prev_close = close_prices.iloc[-2]
                    change = current_price - prev_close
                    percent_change = (change / prev_close) * 100 if prev_close != 0 else 0

                    # Generate SVG chart data
                    svg_chart_data, min_val, max_val = self._generate_svg_chart_data(hist_data)

                    if ticker_symbol == "GC=F":
                        gold_usd_price = current_price
                        gold_svg_chart_data = svg_chart_data
                        gold_min_val = min_val
                        gold_max_val = max_val

                    if ticker_symbol == "JPY=X":
                        usdjpy_price = current_price

                    category_data.append({
                        "name": name,
                        "ticker": ticker_symbol,
                        "country_code": index_info["country_code"],
                        "volatilityIndex": index_info.get("description", ""),
                        "currentValue": f"{current_price:,.2f}",
                        "changeValue": f"{abs(change):,.2f}",
                        "percentage": f"{abs(percent_change):.2f}",
                        "isPositive": bool(change >= 0),
                        "chartData": svg_chart_data,
                        "maxValue": f"{max_val:,.0f}",
                        "minValue": f"{min_val:,.0f}"
                    })
                    time.sleep(0.5)

                except Exception as e:
                    logger.error(f"Failed to fetch data for {name} ({ticker_symbol}): {e}")
                    category_data.append({
                        "name": name,
                        "ticker": ticker_symbol,
                        "country_code": index_info["country_code"],
                        "error": f"Failed to fetch data: {e}"
                    })
                    continue
            world_data[category] = category_data

        if gold_usd_price and usdjpy_price:
            logger.info("Calculating Gold (JPY) price...")
            try:
                gold_jpy_per_gram = (gold_usd_price * usdjpy_price) / 31.1035

                # Use historical data of components to estimate previous price
                gold_hist = yf.Ticker("GC=F", session=self.yf_session).history(period="2d", interval="1d")['Close']
                jpy_hist = yf.Ticker("JPY=X", session=self.yf_session).history(period="2d", interval="1d")['Close']

                if len(gold_hist) > 1 and len(jpy_hist) > 1:
                    prev_gold_usd = gold_hist.iloc[-2]
                    prev_jpy_usd = jpy_hist.iloc[-2]
                    prev_gold_jpy_per_gram = (prev_gold_usd * prev_jpy_usd) / 31.1035

                    change = gold_jpy_per_gram - prev_gold_jpy_per_gram
                    percent_change = (change / prev_gold_jpy_per_gram) * 100 if prev_gold_jpy_per_gram != 0 else 0

                    world_data["コモディティ"].insert(1, {
                        "name": "ゴールド(円)",
                        "ticker": "GOLD-JPY",
                        "country_code": "CM",
                        "volatilityIndex": "金 (円換算)",
                        "currentValue": f"{gold_jpy_per_gram:,.0f}",
                        "changeValue": f"{abs(change):,.0f}",
                        "percentage": f"{abs(percent_change):.2f}",
                        "isPositive": bool(change >= 0),
                        "chartData": gold_svg_chart_data,
                        "maxValue": f"{gold_max_val * usdjpy_price / 31.1035:,.0f}",
                        "minValue": f"{gold_min_val * usdjpy_price / 31.1035:,.0f}"
                    })
                else:
                    logger.warning("Not enough historical data to calculate Gold (JPY) change.")
            except Exception as e:
                logger.error(f"Failed to calculate Gold (JPY) price: {e}")

        self.data['world'] = world_data
        logger.info(f"Fetched data for world indices.")

    def fetch_all_data(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        logger.info("--- Starting Raw Data Fetch ---")
        self.fetch_world_indices_data()
        with open(RAW_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"--- Raw Data Fetch Completed. Saved to {RAW_DATA_PATH} ---")
        return self.data

    def generate_report(self):
        logger.info("--- Starting Report Generation ---")
        if not os.path.exists(RAW_DATA_PATH):
           self.fetch_all_data() # Fetch if raw data doesn't exist
        with open(RAW_DATA_PATH, 'r', encoding='utf-8') as f:
            self.data = json.load(f)

        jst = timezone(timedelta(hours=9))
        self.data['date'] = datetime.now(jst).strftime('%Y-%m-%d')
        self.data['last_updated'] = datetime.now(jst).isoformat()
        self.data = self._clean_non_compliant_floats(self.data)

        final_path = f"{FINAL_DATA_PATH_PREFIX}{self.data['date']}.json"
        with open(final_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        with open(os.path.join(DATA_DIR, 'data.json'), 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"--- Report Generation Completed. Saved to {final_path} ---")
        return self.data

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    if os.path.basename(os.getcwd()) == 'backend':
        os.chdir('..')
    if len(sys.argv) > 1:
        fetcher = MarketDataFetcher()
        if sys.argv[1] == 'fetch':
            fetcher.fetch_all_data()
        elif sys.argv[1] == 'generate':
            fetcher.generate_report()
        else:
            print("Usage: python backend/data_fetcher.py [fetch|generate]")
    else:
        print("Usage: python backend/data_fetcher.py [fetch|generate]")