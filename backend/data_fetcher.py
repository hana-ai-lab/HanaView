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
        {"name": "日経平均", "ticker": "^N225", "country_code": "JP"},
        {"name": "日経時間外", "ticker": "NI=F", "country_code": "JP"},
        {"name": "TOPIX", "ticker": "^TPX", "country_code": "JP"},
        {"name": "日経VI", "ticker": "^JNIV", "country_code": "JP"},
        {"name": "グロース250", "ticker": "^TJG250", "country_code": "JP"},
        {"name": "JPX400", "ticker": "^JPX400", "country_code": "JP"},
    ],
    "米国": [
        {"name": "ダウ", "ticker": "^DJI", "country_code": "US"},
        {"name": "ナスダック", "ticker": "^IXIC", "country_code": "US"},
        {"name": "S&P500", "ticker": "^GSPC", "country_code": "US"},
        {"name": "SOX指数", "ticker": "^SOX", "country_code": "US"},
        {"name": "VIX", "ticker": "^VIX", "country_code": "US"},
        {"name": "ラッセル2000", "ticker": "^RUT", "country_code": "US"},
    ],
    "債券": [
        {"name": "米国債10年", "ticker": "^TNX", "country_code": "US"},
        {"name": "日本国債10年", "ticker": "^TNJ", "country_code": "JP"},
        {"name": "米国債2年", "ticker": "^TWO", "country_code": "US"},
        {"name": "SOFR3ヶ月", "ticker": "SOFR3M=F", "country_code": "US"},
    ],
    "為替": [
        {"name": "ドル円", "ticker": "JPY=X", "country_code": "FX"},
        {"name": "ユーロ円", "ticker": "EURJPY=X", "country_code": "FX"},
        {"name": "ポンド円", "ticker": "GBPJPY=X", "country_code": "FX"},
        {"name": "豪ドル円", "ticker": "AUDJPY=X", "country_code": "FX"},
        {"name": "カナダドル円", "ticker": "CADJPY=X", "country_code": "FX"},
        {"name": "スイスフラン円", "ticker": "CHFJPY=X", "country_code": "FX"},
    ],
    "コモディティ": [
        {"name": "ゴールド", "ticker": "GC=F", "country_code": "CM"},
        {"name": "ゴールド(円)", "ticker": "GOLD-JPY", "country_code": "CM", "calculated": True},
        {"name": "WTI原油", "ticker": "CL=F", "country_code": "CM"},
        {"name": "北海ブレント", "ticker": "BZ=F", "country_code": "CM"},
        {"name": "天然ガス", "ticker": "NG=F", "country_code": "CM"},
        {"name": "銅", "ticker": "HG=F", "country_code": "CM"},
        {"name": "ビットコイン", "ticker": "BTC-USD", "country_code": "CM"},
        {"name": "イーサリアム", "ticker": "ETH-USD", "country_code": "CM"},
    ],
    "北東アジア": [
        {"name": "上海総合", "ticker": "000001.SS", "country_code": "CN"},
        {"name": "CSI300", "ticker": "000300.SS", "country_code": "CN"},
        {"name": "韓国 KOSPI", "ticker": "^KS11", "country_code": "KR"},
        {"name": "香港 ハンセン", "ticker": "^HSI", "country_code": "HK"},
        {"name": "台湾 加権", "ticker": "^TWII", "country_code": "TW"},
    ],
    "欧州": [
        {"name": "イギリス FTSE", "ticker": "^FTSE", "country_code": "GB"},
        {"name": "ドイツ DAX", "ticker": "^GDAXI", "country_code": "DE"},
        {"name": "フランス CAC40", "ticker": "^FCHI", "country_code": "FR"},
        {"name": "イタリア MIB", "ticker": "FTSEMIB.MI", "country_code": "IT"},
        {"name": "スイス SMI", "ticker": "^SSMI", "country_code": "CH"},
        {"name": "ロシア RTSI", "ticker": "RTSI.ME", "country_code": "RU"},
    ],
    "ピックアップ": [
        {"name": "FANG+", "ticker": "^NYFANG", "country_code": "US"},
        {"name": "全世界株式 オルカン", "ticker": "ACWI", "country_code": "US"},
    ],
    "新興アジア": [
        {"name": "インド Nifty", "ticker": "^NSEI", "country_code": "IN"},
        {"name": "マレーシア KLCI", "ticker": "^KLSE", "country_code": "MY"},
        {"name": "タイ SET指数", "ticker": "^SET.BK", "country_code": "TH"},
        {"name": "ベトナム", "ticker": "^VNI", "country_code": "VN"},
        {"name": "シンガポールSTI", "ticker": "^STI", "country_code": "SG"},
        {"name": "インドネシアJKSE", "ticker": "^JKSE", "country_code": "ID"},
    ],
    "オセアニア": [
        {"name": "オーストラリアASX", "ticker": "^AXJO", "country_code": "AU"},
        {"name": "ニュージーランド", "ticker": "^NZ50", "country_code": "NZ"},
    ],
    "アメリカ大陸": [
        {"name": "カナダS&Pトロント総合", "ticker": "^GSPTSE", "country_code": "CA"},
        {"name": "メキシコIPC", "ticker": "^MXX", "country_code": "MX"},
        {"name": "アルゼンチン メンバル", "ticker": "^MERV", "country_code": "AR"},
        {"name": "ブラジル ボベスパ", "ticker": "^BVSP", "country_code": "BR"},
    ],
    "中東･アフリカ": [
        {"name": "トルコ Borsa100", "ticker": "XU100.IS", "country_code": "TR"},
        {"name": "ドバイ UAE", "ticker": "DFMGI.AE", "country_code": "AE"},
        {"name": "南アフリカ", "ticker": "^JALSH", "country_code": "ZA"},
        {"name": "サウジアラビア", "ticker": "^TASI.SR", "country_code": "SA"},
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

    def fetch_world_indices_data(self):
        """Fetches data for world indices for the new 'World' tab, organized by category."""
        logger.info("Fetching world indices data...")
        world_data = {}
        gold_usd_price = None
        usdjpy_price = None

        for category, indices in WORLD_INDICES.items():
            category_data = []
            for index_info in indices:
                name = index_info["name"]
                ticker_symbol = index_info["ticker"]

                if index_info.get("calculated"):
                    continue

                try:
                    ticker = yf.Ticker(ticker_symbol, session=self.yf_session)
                    hist_5d = ticker.history(period="7d", interval="1h")

                    if hist_5d.empty:
                        logger.warning(f"No 5d history for {name} ({ticker_symbol}), using daily.")
                        hist_daily = ticker.history(period="3mo", interval="1d")
                        if hist_daily.empty:
                             logger.error(f"No data at all for {name} ({ticker_symbol}), skipping.")
                             continue
                        current_price = hist_daily['Close'].iloc[-1]
                        prev_close = hist_daily['Close'].iloc[-2]
                        chart_data = [{"time": index.strftime('%Y-%m-%dT%H:%M:%S'), "value": row['Close']} for index, row in hist_daily.iterrows()]
                    else:
                        current_price = hist_5d['Close'].iloc[-1]
                        hist_daily = ticker.history(period="2d", interval="1d")
                        if len(hist_daily) < 2:
                            prev_close = hist_5d.iloc[0]['Open']
                        else:
                            prev_close = hist_daily['Close'].iloc[-2]
                        chart_data = [{"time": index.strftime('%Y-%m-%dT%H:%M:%S'), "value": row['Close']} for index, row in hist_5d.tail(48).iterrows()]


                    change = current_price - prev_close
                    percent_change = (change / prev_close) * 100 if prev_close != 0 else 0


                    if ticker_symbol == "GC=F": gold_usd_price = current_price
                    if ticker_symbol == "JPY=X": usdjpy_price = current_price

                    category_data.append({
                        "name": name,
                        "ticker": ticker_symbol,
                        "country_code": index_info["country_code"],
                        "current_price": round(current_price, 2),
                        "change": round(change, 2),
                        "percent_change": round(percent_change, 2),
                        "chart_data": chart_data
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
                gold_hist = yf.Ticker("GC=F", session=self.yf_session).history(period="2d")
                jpy_hist = yf.Ticker("JPY=X", session=self.yf_session).history(period="2d")

                if len(gold_hist) > 1 and len(jpy_hist) > 1:
                    prev_gold_usd = gold_hist['Close'].iloc[-2]
                    prev_jpy_usd = jpy_hist['Close'].iloc[-2]
                    prev_gold_jpy_per_gram = (prev_gold_usd * prev_jpy_usd) / 31.1035
                    change = gold_jpy_per_gram - prev_gold_jpy_per_gram
                    percent_change = (change / prev_gold_jpy_per_gram) * 100 if prev_gold_jpy_per_gram != 0 else 0
                    gold_usd_chart_data = next((item['chart_data'] for item in world_data.get("コモディティ", []) if item['ticker'] == 'GC=F'), [])

                    world_data["コモディティ"].insert(1, {
                        "name": "ゴールド(円)",
                        "ticker": "GOLD-JPY",
                        "country_code": "CM",
                        "current_price": round(gold_jpy_per_gram, 2),
                        "change": round(change, 2),
                        "percent_change": round(percent_change, 2),
                        "chart_data": gold_usd_chart_data
                    })
                else:
                    logger.warning("Not enough historical data to calculate Gold (JPY) change.")
            except Exception as e:
                logger.error(f"Failed to calculate Gold (JPY) price: {e}")

        self.data['world'] = world_data
        logger.info(f"Fetched data for world indices.")

    # --- Other methods from the original file should be here ---
    # ... (omitted for brevity, assuming they are unchanged)

    def fetch_all_data(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        logger.info("--- Starting Raw Data Fetch ---")
        self.fetch_world_indices_data()
        # ... (rest of the fetch calls)
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

        # AI Generation Steps
        # ... (AI generation calls)

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