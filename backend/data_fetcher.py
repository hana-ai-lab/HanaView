import json
import logging
import logging.handlers
import os
import re
import sys
from datetime import datetime, timedelta, timezone
import time
import math
import pandas as pd
import yfinance as yf
from bs4 import BeautifulSoup
from curl_cffi.requests import Session
import openai
import httpx
from io import StringIO
from .image_generator import generate_fear_greed_chart

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

# Country to Emoji Mapping
COUNTRY_EMOJI_MAP = {
    "jpn": "🇯🇵",
    "usa": "🇺🇸",
    "eur": "🇪🇺",
    "gbr": "🇬🇧",
    "deu": "🇩🇪",
    "fra": "🇫🇷",
    "aus": "🇦🇺",
    "nzl": "🇳🇿",
    "can": "🇨🇦",
    "che": "🇨🇭",
    "chn": "🇨🇳",
    "hkg": "🇭🇰",
    "ind": "🇮🇳",
    "bra": "🇧🇷",
    "zaf": "🇿🇦",
    "tur": "🇹🇷",
    "kor": "🇰🇷",
    "sgp": "🇸🇬",
}

# Important tickers from originalcalendar.py
US_TICKER_LIST = ["AAPL", "NVDA", "MSFT", "GOOG", "META", "AMZN", "NFLX", "BRK-B", "TSLA", "AVGO", 
                  "LLY", "WMT", "JPM", "V", "UNH", "XOM", "ORCL", "MA", "HD", "PG", "COST", "JNJ", 
                  "ABBV", "TMUS", "BAC", "CRM", "KO", "CVX", "VZ", "MRK", "AMD", "PEP", "CSCO", 
                  "LIN", "ACN", "WFC", "TMO", "ADBE", "MCD", "ABT", "BX", "PM", "NOW", "IBM", "AXP", 
                  "MS", "TXN", "GE", "QCOM", "CAT", "ISRG", "DHR", "INTU", "DIS", "CMCSA", "AMGN", 
                  "T", "GS", "PFE", "NEE", "CHTR", "RTX", "BKNG", "UBER", "AMAT", "SPGI", "LOW", 
                  "BLK", "PGR", "UNP", "SYK", "HON", "ETN", "SCHW", "LMT", "TJX", "COP", "ANET", 
                  "BSX", "KKR", "VRTX", "C", "PANW", "ADP", "NKE", "BA", "MDT", "FI", "UPS", "SBUX", 
                  "ADI", "CB", "GILD", "MU", "BMY", "DE", "PLD", "MMC", "INTC", "AMT", "SO", "LRCX", 
                  "ELV", "DELL", "PLTR", "REGN", "MDLZ", "MO", "HCA", "SHW", "KLAC", "ICE", "CI", "ABNB"]

JP_TICKER_LIST = ["7203", "8306", "6501", "6861", "6758", "9983", "6098", "9984", "8316", "9432", 
                  "4519", "4063", "8058", "8001", "8766", "8035", "9433", "8031", "7974", "4568", 
                  "9434", "8411", "2914", "7267", "7741", "7011", "4502", "6857", "6902", "4661", 
                  "6503", "3382", "6367", "8725", "4578", "6702", "6981", "6146", "7751", "6178", 
                  "4543", "4901", "6273", "8053", "8002", "6954", "5108", "8591", "6301", "8801", 
                  "6723", "8750", "6762", "6594", "9020", "6701", "9613", "4503", "8267", "8630", 
                  "6752", "6201", "9022", "7733", "4452", "4689", "2802", "5401", "1925", "7269", 
                  "8802", "8113", "2502", "8015", "4612", "4307", "1605", "8309", "8308", "1928", 
                  "8604", "9101", "6326", "4684", "7532", "9735", "8830", "9503", "5020", "3659", 
                  "9843", "6971", "7832", "4091", "7309", "4755", "9104", "4716", "7936", "9766", 
                  "4507", "8697", "5802", "2503", "7270", "6920", "6869", "6988", "2801", "2587", 
                  "3407", "5803", "7201", "8593", "9531", "4523", "9107", "7202", "3092", "8601", 
                  "5019", "9202", "9435", "1802", "4768", "7911", "4151", "9502", "6586", "7701", 
                  "3402", "7272", "9532", "9697", "4911", "9021", "8795", "3064", "7259", "1812", 
                  "2897", "7912", "4324", "6504", "7013", "7550", "6645", "5713", "5411", "4188"]

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
        else:
            http_client = httpx.Client(trust_env=False)
            self.openai_client = openai.OpenAI(api_key=api_key, http_client=http_client)

    def _clean_non_compliant_floats(self, obj):
        if isinstance(obj, dict):
            return {k: self._clean_non_compliant_floats(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._clean_non_compliant_floats(elem) for elem in obj]
        if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
            return None
        return obj

    # --- Ticker List Fetching ---
    def _get_sp500_tickers(self):
        logger.info("Fetching S&P 500 ticker list from Wikipedia...")
        try:
            response = self.http_session.get(SP500_WIKI_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'id': 'constituents'})
            tickers = [row.find_all('td')[0].text.strip() for row in table.find_all('tr')[1:]]
            return [t.replace('.', '-') for t in tickers]
        except Exception as e:
            logger.error(f"Failed to get S&P 500 tickers: {e}")
            return []

    def _get_nasdaq100_tickers(self):
        logger.info("Fetching NASDAQ 100 ticker list from Wikipedia...")
        try:
            response = self.http_session.get(NASDAQ100_WIKI_URL, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'id': 'constituents'})
            tickers = [row.find_all('td')[0].text.strip() for row in table.find_all('tr')[1:] if len(row.find_all('td')) > 0]
            return [t.replace('.', '-') for t in tickers]
        except Exception as e:
            logger.error(f"Failed to get NASDAQ 100 tickers: {e}")
            return []

    # --- Data Fetching Methods ---
    def _fetch_yfinance_data(self, ticker_symbol, period="5d", interval="1h", resample_period='4h'):
        """Yahoo Finance API対策を含むデータ取得"""
        try:
            ticker = yf.Ticker(ticker_symbol, session=self.yf_session)
            hist = ticker.history(period=period, interval=interval)

            if hist.empty:
                raise ValueError("No data returned")

            hist.index = hist.index.tz_convert('Asia/Tokyo')
            resampled_hist = hist['Close'].resample(resample_period).ohlc().dropna()
            current_price = hist['Close'].iloc[-1]
            history_list = [
                {
                    "time": index.strftime('%Y-%m-%dT%H:%M:%S'),
                    "open": round(row['open'], 2),
                    "high": round(row['high'], 2),
                    "low": round(row['low'], 2),
                    "close": round(row['close'], 2)
                } for index, row in resampled_hist.iterrows()
            ]
            return {"current": round(current_price, 2), "history": history_list}
        except Exception as e:
            logger.error(f"Error fetching {ticker_symbol}: {e}")
            raise MarketDataError("E003", f"yfinance failed for {ticker_symbol}: {e}") from e

    def fetch_vix(self):
        logger.info("Fetching VIX data...")
        try:
            self.data['market']['vix'] = self._fetch_yfinance_data(VIX_TICKER, period="60d")
        except MarketDataError as e:
            self.data['market']['vix'] = {"current": None, "history": [], "error": str(e)}
            logger.error(f"VIX fetch failed: {e}")

    def fetch_t_note_future(self):
        logger.info("Fetching T-note future data...")
        try:
            self.data['market']['t_note_future'] = self._fetch_yfinance_data(T_NOTE_TICKER, period="60d")
        except MarketDataError as e:
            self.data['market']['t_note_future'] = {"current": None, "history": [], "error": str(e)}
            logger.error(f"T-Note fetch failed: {e}")

    def _get_historical_value(self, data, days_ago):
        target_date = datetime.now() - timedelta(days=days_ago)
        closest_item = min(data, key=lambda x: abs(datetime.fromtimestamp(x['x'] / 1000) - target_date))
        return closest_item['y'] if closest_item else None

    def _get_fear_greed_category(self, value):
        if value is None: return "Unknown"
        if value <= 25: return "Extreme Fear";
        if value <= 45: return "Fear";
        if value <= 55: return "Neutral";
        if value <= 75: return "Greed";
        return "Extreme Greed"

    def fetch_fear_greed_index(self):
        logger.info("Fetching Fear & Greed Index...")
        try:
            start_date = (datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')
            url = f"{CNN_FEAR_GREED_URL}{start_date}"
            response = self.http_session.get(url, timeout=30)
            response.raise_for_status()
            api_data = response.json()
            fg_data = api_data.get('fear_and_greed_historical', {}).get('data', [])
            if not fg_data: raise ValueError("No historical data found")

            current_value = fg_data[-1]['y']
            previous_close_val = self._get_historical_value(fg_data, 1)
            week_ago_val = self._get_historical_value(fg_data, 7)
            month_ago_val = self._get_historical_value(fg_data, 30)
            year_ago_val = self._get_historical_value(fg_data, 365)

            # Store the original data structure for other parts of the app
            self.data['market']['fear_and_greed'] = {
                'now': round(current_value),
                'previous_close': round(previous_close_val) if previous_close_val is not None else None,
                'prev_week': round(week_ago_val) if week_ago_val is not None else None,
                'prev_month': round(month_ago_val) if month_ago_val is not None else None,
                'prev_year': round(year_ago_val) if year_ago_val is not None else None,
                'category': self._get_fear_greed_category(current_value)
            }

            # Prepare data for image generation
            chart_data = {
                "center_value": round(current_value),
                "history": {
                    "previous_close": {"label": "Previous close", "status": self._get_fear_greed_category(previous_close_val), "value": round(previous_close_val) if previous_close_val is not None else 'N/A'},
                    "week_ago": {"label": "1 week ago", "status": self._get_fear_greed_category(week_ago_val), "value": round(week_ago_val) if week_ago_val is not None else 'N/A'},
                    "month_ago": {"label": "1 month ago", "status": self._get_fear_greed_category(month_ago_val), "value": round(month_ago_val) if month_ago_val is not None else 'N/A'},
                    "year_ago": {"label": "1 year ago", "status": self._get_fear_greed_category(year_ago_val), "value": round(year_ago_val) if year_ago_val is not None else 'N/A'}
                }
            }

            # Generate the chart
            logger.info("Generating Fear & Greed gauge chart...")
            generate_fear_greed_chart(chart_data)

        except Exception as e:
            logger.error(f"Error fetching or generating Fear & Greed Index: {e}")
            self.data['market']['fear_and_greed'] = {'now': None, 'error': f"[E004] {ERROR_CODES['E004']}: {e}"}

    def fetch_calendar_data(self):
        """Fetch economic indicators and earnings calendar."""
        dt_now = datetime.now()
        
        # Fetch economic indicators
        self._fetch_economic_indicators(dt_now)

        # Fetch earnings
        logger.info("Fetching earnings calendar data...")
        try:
            # Fetch US earnings
            self._fetch_us_earnings(dt_now)
            
            # Fetch JP earnings
            self._fetch_jp_earnings(dt_now)
            
        except Exception as e:
            logger.error(f"Error during earnings data fetching: {e}")
            if 'error' not in self.data['indicators']:
                 self.data['indicators']['error'] = f"[E007] {ERROR_CODES['E007']}: {e}"

    def _fetch_economic_indicators(self, dt_now):
        """Fetch economic indicators from Monex using curl_cffi and BeautifulSoup. Timezone-aware."""
        logger.info("Fetching economic indicators from Monex...")
        try:
            response = self.http_session.get(MONEX_ECONOMIC_CALENDAR_URL, timeout=30)
            response.raise_for_status()
            html_content = response.content.decode('shift_jis', errors='replace')
            soup = BeautifulSoup(html_content, 'lxml')

            table = soup.find('table', class_='eindicator-list')
            if not table:
                logger.warning("Could not find the expected economic calendar table.")
                self.data['indicators']['economic'] = []
                return

            indicators = []
            jst = timezone(timedelta(hours=9))
            dt_now_jst = datetime.now(jst)
            current_date_str = ""

            for row in table.find('tbody').find_all('tr'):
                cells = row.find_all('td')

                try:
                    # Handle date cells with rowspan
                    if 'rowspan' in cells[0].attrs:
                        current_date_str = cells[0].text.strip()
                        cell_offset = 0
                    else:
                        cell_offset = -1

                    time_str = cells[1 + cell_offset].text.strip()
                    if not time_str or time_str == '-':
                        continue

                    full_date_str = f"{dt_now_jst.year}/{current_date_str.split('(')[0]} {time_str}"
                    tdatetime = datetime.strptime(full_date_str, '%Y/%m/%d %H:%M')
                    tdatetime_aware = tdatetime.replace(tzinfo=jst)

                    if not (dt_now_jst - timedelta(hours=2) < tdatetime_aware < dt_now_jst + timedelta(hours=26)):
                        continue

                    importance_str = cells[2 + cell_offset].text.strip()
                    if "★" not in importance_str:
                        continue

                    # Extract country emoji
                    country_cell = cells[3 + cell_offset]
                    img_tag = country_cell.find('img')
                    emoji = ''
                    if img_tag and img_tag.get('src'):
                        match = re.search(r'inner_flag_(\w+)\.(?:gif|png)', img_tag['src'])
                        if match:
                            country_code = match.group(1)
                            emoji = COUNTRY_EMOJI_MAP.get(country_code, '')

                    def get_value(cell_index, default='--'):
                        val = cells[cell_index].text.strip()
                        return val if val else default

                    name = get_value(4 + cell_offset)

                    indicator = {
                        "datetime": tdatetime_aware.strftime('%m/%d %H:%M'),
                        "name": f"{emoji} {name}".strip(),
                        "importance": importance_str,
                        "previous": get_value(5 + cell_offset),
                        "forecast": get_value(6 + cell_offset),
                        "type": "economic"
                    }
                    indicators.append(indicator)

                except (ValueError, IndexError) as e:
                    logger.debug(f"Skipping row in economic indicators: {row.text.strip()} due to {e}")
                    continue
            
            self.data['indicators']['economic'] = indicators
            logger.info(f"Fetched {len(indicators)} economic indicators successfully.")

        except Exception as e:
            logger.error(f"Error fetching economic indicators: {e}")
            self.data['indicators']['economic'] = []

    def _fetch_us_earnings(self, dt_now):
        """Fetch US earnings calendar from Monex using curl_cffi."""
        logger.info("Fetching US earnings calendar from Monex...")
        try:
            response = self.http_session.get(MONEX_US_EARNINGS_URL, timeout=30)
            response.raise_for_status()
            html_content = response.content.decode('shift_jis', errors='replace')
            tables = pd.read_html(StringIO(html_content), flavor='lxml')
            
            earnings = []
            for df in tables:
                if df.empty: continue
                for i in range(len(df)):
                    try:
                        ticker, company_name, date_str, time_str = None, None, None, None
                        for col_idx in range(len(df.columns)):
                            val = str(df.iloc[i, col_idx]) if pd.notna(df.iloc[i, col_idx]) else ""
                            if val in US_TICKER_LIST: ticker = val
                            elif "/" in val and len(val) >= 8: date_str = val
                            elif ":" in val and len(val) >= 5: time_str = val
                            elif len(val) > 3 and val != "nan" and not company_name: company_name = val[:20]

                        if ticker and date_str and time_str:
                            text0 = date_str[:10] + " " + time_str[:5]
                            tdatetime = datetime.strptime(text0, '%Y/%m/%d %H:%M') + timedelta(hours=13)
                            if tdatetime > dt_now - timedelta(hours=2):
                                earnings.append({"datetime": tdatetime.strftime('%m/%d %H:%M'), "ticker": ticker, "company": f"({company_name})" if company_name else "", "type": "us_earnings"})
                    except Exception as e:
                        logger.debug(f"Skipping row {i} in US earnings: {e}")
            
            self.data['indicators']['us_earnings'] = earnings
            logger.info(f"Fetched {len(earnings)} US earnings")
        except Exception as e:
            logger.error(f"Error fetching US earnings: {e}")
            self.data['indicators']['us_earnings'] = []

    def _fetch_jp_earnings(self, dt_now):
        """Fetch Japanese earnings calendar from Monex using curl_cffi."""
        logger.info("Fetching Japanese earnings calendar from Monex...")
        try:
            response = self.http_session.get(MONEX_JP_EARNINGS_URL, timeout=30)
            response.raise_for_status()
            html_content = response.content.decode('shift_jis', errors='replace')
            tables = pd.read_html(StringIO(html_content), flavor='lxml')

            earnings = []
            for df in tables:
                if df.empty: continue
                for i in range(len(df)):
                    try:
                        ticker, company_name, date_time_str = None, None, None
                        for col_idx in range(len(df.columns)):
                            val = str(df.iloc[i, col_idx]) if pd.notna(df.iloc[i, col_idx]) else ""
                            match = re.search(r'(\d{4})', val)
                            if not ticker and match and match.group(1) in JP_TICKER_LIST:
                                ticker = match.group(1)
                                if not val.strip().isdigit():
                                    name_match = re.search(r'^([^（\(]+)', val)
                                    if name_match: company_name = name_match.group(1).strip()[:20]
                            elif not date_time_str and "/" in val and "日" in val: date_time_str = val.strip()
                            elif not company_name and len(val) > 2 and val != 'nan' and not val.strip().isdigit() and "/" not in val: company_name = val.strip()[:20]

                        if ticker and date_time_str:
                             earnings.append({"datetime": date_time_str[:16], "ticker": ticker, "company": f"({company_name})" if company_name else "", "type": "jp_earnings"})
                    except Exception as e:
                        logger.debug(f"Skipping row {i} in JP earnings: {e}")

            self.data['indicators']['jp_earnings'] = earnings
            logger.info(f"Fetched {len(earnings)} Japanese earnings")
        except Exception as e:
            logger.error(f"Error fetching Japanese earnings: {e}")
            self.data['indicators']['jp_earnings'] = []

    def fetch_yahoo_finance_news(self):
        """Fetches recent news from Yahoo Finance using the yfinance library and filters them."""
        logger.info("Fetching and filtering news from Yahoo Finance using yfinance...")
        try:
            # Define tickers for major US indices
            indices = {"NASDAQ Composite (^IXIC)": "^IXIC", "S&P 500 (^GSPC)": "^GSPC", "Dow 30 (^DJI)": "^DJI"}
            all_raw_news = []

            for name, ticker_symbol in indices.items():
                logger.info(f"Fetching news for {name}...")
                try:
                    ticker = yf.Ticker(ticker_symbol, session=self.yf_session)
                    news = ticker.news
                    if news:
                        all_raw_news.extend(news)
                    else:
                        logger.warning(f"No news returned from yfinance for {ticker_symbol}.")
                except Exception as e:
                    logger.error(f"Failed to fetch news for {ticker_symbol}: {e}")
                    continue # Continue to the next ticker

            # Deduplicate news based on the article link to avoid redundancy
            unique_news = []
            seen_links = set()
            for article in all_raw_news:
                try:
                    # The unique identifier for a news article is its URL.
                    link = article['content']['canonicalUrl']['url']
                    if link not in seen_links:
                        unique_news.append(article)
                        seen_links.add(link)
                except KeyError:
                    # Log if a link is not found, but continue processing other articles.
                    logger.warning(f"Could not find link for article, skipping: {article.get('content', {}).get('title', 'No Title')}")
                    continue

            raw_news = unique_news

            if not raw_news:
                logger.warning("No news returned from yfinance for any of the specified indices.")
                self.data['news_raw'] = []
                return

            now_utc = datetime.now(timezone.utc)
            twenty_four_hours_ago = now_utc - timedelta(hours=24)

            # 1. Filter news within the last 24 hours
            filtered_news = []
            for article in raw_news:
                try:
                    # pubDate is a string like '2025-09-08T17:42:03Z'
                    pub_date_str = article['content']['pubDate']
                    # fromisoformat doesn't like the 'Z' suffix
                    publish_time = datetime.fromisoformat(pub_date_str.replace('Z', '+00:00'))

                    if publish_time >= twenty_four_hours_ago:
                        article['publish_time_dt'] = publish_time # Store for sorting
                        filtered_news.append(article)
                except (KeyError, TypeError) as e:
                    logger.warning(f"Could not process article, skipping: {e} - {article}")
                    continue

            # 2. Sort by publish time descending (latest first)
            filtered_news.sort(key=lambda x: x['publish_time_dt'], reverse=True)

            # 3. Format all filtered news
            formatted_news = [
                {
                    "title": item['content']['title'],
                    "link": item['content']['canonicalUrl']['url'],
                    "publisher": item['content']['provider']['displayName'],
                    "summary": item['content'].get('summary', '')
                }
                for item in filtered_news
            ]

            self.data['news_raw'] = formatted_news
            logger.info(f"Fetched {len(all_raw_news)} raw news items, found {len(unique_news)} unique articles, {len(filtered_news)} within the last 24 hours, storing the top {len(formatted_news)}.")

        except Exception as e:
            logger.error(f"Error fetching or processing yfinance news: {e}")
            self.data['news_raw'] = []

    def fetch_heatmap_data(self):
        """ヒートマップデータ取得（API対策強化版）"""
        logger.info("Fetching heatmap data...")
        try:
            sp500_tickers = self._get_sp500_tickers()
            nasdaq100_tickers = self._get_nasdaq100_tickers()
            logger.info(f"Found {len(sp500_tickers)} S&P 500 tickers and {len(nasdaq100_tickers)} NASDAQ 100 tickers.")

            # Fetch S&P 500 data
            sp500_heatmaps = self._fetch_stock_performance_for_heatmap(sp500_tickers, batch_size=30)
            self.data['sp500_heatmap_1d'] = sp500_heatmaps.get('1d', {"stocks": []})
            self.data['sp500_heatmap_1w'] = sp500_heatmaps.get('1w', {"stocks": []})
            self.data['sp500_heatmap_1m'] = sp500_heatmaps.get('1m', {"stocks": []})
            # For backward compatibility with AI commentary
            self.data['sp500_heatmap'] = self.data.get('sp500_heatmap_1d', {"stocks": []})

            # Fetch NASDAQ 100 data
            nasdaq_heatmaps = self._fetch_stock_performance_for_heatmap(nasdaq100_tickers, batch_size=30)
            self.data['nasdaq_heatmap_1d'] = nasdaq_heatmaps.get('1d', {"stocks": []})
            self.data['nasdaq_heatmap_1w'] = nasdaq_heatmaps.get('1w', {"stocks": []})
            self.data['nasdaq_heatmap_1m'] = nasdaq_heatmaps.get('1m', {"stocks": []})
            # For backward compatibility with AI commentary
            self.data['nasdaq_heatmap'] = self.data.get('nasdaq_heatmap_1d', {"stocks": []})

            # Fetch Sector ETF data
            sector_etf_tickers = ["XLK", "XLY", "XLV", "XLP", "XLB", "XLU", "XLI", "XLC", "XLRE", "XLF", "XLE"]
            logger.info(f"Fetching data for {len(sector_etf_tickers)} sector ETFs.")
            sector_etf_heatmaps = self._fetch_etf_performance_for_heatmap(sector_etf_tickers)
            self.data['sector_etf_heatmap_1d'] = sector_etf_heatmaps.get('1d', {"etfs": []})
            self.data['sector_etf_heatmap_1w'] = sector_etf_heatmaps.get('1w', {"etfs": []})
            self.data['sector_etf_heatmap_1m'] = sector_etf_heatmaps.get('1m', {"etfs": []})

            # Create combined S&P 500 and ETF heatmaps
            logger.info("Creating combined S&P 500 and Sector ETF heatmap data...")
            for period in ['1d', '1w', '1m']:
                sp500_stocks = self.data.get(f'sp500_heatmap_{period}', {}).get('stocks', [])
                etfs = self.data.get(f'sector_etf_heatmap_{period}', {}).get('etfs', [])

                # The frontend only needs ticker and performance.
                # No need to add a 'type' field as they will be rendered identically.
                combined_items = sp500_stocks + etfs
                self.data[f'sp500_combined_heatmap_{period}'] = {"items": combined_items}

        except Exception as e:
            logger.error(f"Error during heatmap data fetching: {e}")
            error_payload = {"stocks": [], "error": f"[E006] {ERROR_CODES['E006']}: {e}"}
            self.data['sp500_heatmap_1d'] = error_payload
            self.data['sp500_heatmap_1w'] = error_payload
            self.data['sp500_heatmap_1m'] = error_payload
            self.data['nasdaq_heatmap_1d'] = error_payload
            self.data['nasdaq_heatmap_1w'] = error_payload
            self.data['nasdaq_heatmap_1m'] = error_payload
            self.data['sp500_heatmap'] = error_payload
            self.data['nasdaq_heatmap'] = error_payload
            etf_error_payload = {"etfs": [], "error": f"[E006] {ERROR_CODES['E006']}: {e}"}
            self.data['sector_etf_heatmap_1d'] = etf_error_payload
            self.data['sector_etf_heatmap_1w'] = etf_error_payload
            self.data['sector_etf_heatmap_1m'] = etf_error_payload
            self.data['sp500_combined_heatmap_1d'] = {"items": []}
            self.data['sp500_combined_heatmap_1w'] = {"items": []}
            self.data['sp500_combined_heatmap_1m'] = {"items": []}

    def _fetch_stock_performance_for_heatmap(self, tickers, batch_size=30):
        """改善版：レート制限対策を含むヒートマップ用データ取得（業種・フラット構造対応）。1日、1週間、1ヶ月のパフォーマンスを計算する。"""
        if not tickers:
            return {"1d": {"stocks": []}, "1w": {"stocks": []}, "1m": {"stocks": []}}

        heatmaps = {
            "1d": {"stocks": []},
            "1w": {"stocks": []},
            "1m": {"stocks": []}
        }

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i+batch_size]

            for ticker_symbol in batch:
                try:
                    ticker_obj = yf.Ticker(ticker_symbol, session=self.yf_session)
                    info = ticker_obj.info
                    # 1ヶ月分のデータを取得（約22営業日 + 余裕）
                    hist = ticker_obj.history(period="35d")

                    if hist.empty:
                        logger.warning(f"No history for {ticker_symbol}, skipping.")
                        continue

                    sector = info.get('sector', 'N/A')
                    industry = info.get('industry', 'N/A')
                    market_cap = info.get('marketCap', 0)

                    if sector == 'N/A' or industry == 'N/A' or market_cap == 0:
                        logger.warning(f"Skipping {ticker_symbol} due to missing sector, industry, or market cap.")
                        continue

                    base_stock_data = {
                        "ticker": ticker_symbol,
                        "sector": sector,
                        "industry": industry,
                        "market_cap": market_cap
                    }

                    latest_close = hist['Close'].iloc[-1]

                    # 1-Day Performance
                    if len(hist) >= 2 and hist['Close'].iloc[-2] != 0:
                        perf_1d = ((latest_close - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
                        stock_1d = base_stock_data.copy()
                        stock_1d["performance"] = round(perf_1d, 2)
                        heatmaps["1d"]["stocks"].append(stock_1d)

                    # 1-Week Performance (5 trading days)
                    if len(hist) >= 6 and hist['Close'].iloc[-6] != 0:
                        perf_1w = ((latest_close - hist['Close'].iloc[-6]) / hist['Close'].iloc[-6]) * 100
                        stock_1w = base_stock_data.copy()
                        stock_1w["performance"] = round(perf_1w, 2)
                        heatmaps["1w"]["stocks"].append(stock_1w)

                    # 1-Month Performance (20 trading days)
                    if len(hist) >= 21 and hist['Close'].iloc[-21] != 0:
                        perf_1m = ((latest_close - hist['Close'].iloc[-21]) / hist['Close'].iloc[-21]) * 100
                        stock_1m = base_stock_data.copy()
                        stock_1m["performance"] = round(perf_1m, 2)
                        heatmaps["1m"]["stocks"].append(stock_1m)

                except Exception as e:
                    logger.error(f"Could not fetch data for {ticker_symbol}: {e}")
                    time.sleep(0.5)
                    continue

            if i + batch_size < len(tickers):
                logger.info(f"Processed {min(i + batch_size, len(tickers))}/{len(tickers)} tickers, waiting...")
                time.sleep(3)

        return heatmaps

    def _fetch_etf_performance_for_heatmap(self, tickers):
        """Fetches 1-day, 1-week, and 1-month performance for a list of ETFs."""
        if not tickers:
            return {"1d": {"etfs": []}, "1w": {"etfs": []}, "1m": {"etfs": []}}

        heatmaps = {
            "1d": {"etfs": []},
            "1w": {"etfs": []},
            "1m": {"etfs": []}
        }

        for ticker_symbol in tickers:
            try:
                ticker_obj = yf.Ticker(ticker_symbol, session=self.yf_session)
                # 1ヶ月分のデータを取得（約22営業日 + 余裕）
                hist = ticker_obj.history(period="35d")

                if hist.empty:
                    logger.warning(f"No history for ETF {ticker_symbol}, skipping.")
                    continue

                base_etf_data = {
                    "ticker": ticker_symbol,
                }

                latest_close = hist['Close'].iloc[-1]

                # 1-Day Performance
                if len(hist) >= 2 and hist['Close'].iloc[-2] != 0:
                    perf_1d = ((latest_close - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2]) * 100
                    etf_1d = base_etf_data.copy()
                    etf_1d["performance"] = round(perf_1d, 2)
                    heatmaps["1d"]["etfs"].append(etf_1d)

                # 1-Week Performance (5 trading days)
                if len(hist) >= 6 and hist['Close'].iloc[-6] != 0:
                    perf_1w = ((latest_close - hist['Close'].iloc[-6]) / hist['Close'].iloc[-6]) * 100
                    etf_1w = base_etf_data.copy()
                    etf_1w["performance"] = round(perf_1w, 2)
                    heatmaps["1w"]["etfs"].append(etf_1w)

                # 1-Month Performance (20 trading days)
                if len(hist) >= 21 and hist['Close'].iloc[-21] != 0:
                    perf_1m = ((latest_close - hist['Close'].iloc[-21]) / hist['Close'].iloc[-21]) * 100
                    etf_1m = base_etf_data.copy()
                    etf_1m["performance"] = round(perf_1m, 2)
                    heatmaps["1m"]["etfs"].append(etf_1m)

            except Exception as e:
                logger.error(f"Could not fetch data for ETF {ticker_symbol}: {e}")
                continue

        # Sort by ticker name
        for period in heatmaps:
            if 'etfs' in heatmaps[period]:
                heatmaps[period]['etfs'].sort(key=lambda x: x['ticker'])

        return heatmaps

    # --- AI Generation ---
    def _call_openai_api(self, prompt, max_completion_tokens=150):
        if not self.openai_client:
            raise MarketDataError("E005", "OpenAI client is not available.")
        try:
            logger.info(f"Calling OpenAI API (max_completion_tokens={max_completion_tokens})...")

            messages = [
                {
                    "role": "system",
                    "content": "You are a helpful assistant designed to output JSON. Your response must be valid JSON."
                },
                {"role": "user", "content": prompt}
            ]

            kwargs = {
                "model": "gpt-4o-mini",
                "messages": messages,
                "temperature": 0.7,
                "max_completion_tokens": max_completion_tokens,
                "response_format": {"type": "json_object"}
            }

            response = self.openai_client.chat.completions.create(**kwargs)

            logger.debug(f"Response object type: {type(response)}")
            if hasattr(response, 'model'): logger.debug(f"Response model: {response.model}")
            if hasattr(response, 'usage'): logger.debug(f"Response usage: {response.usage}")

            if not response or not response.choices:
                logger.error("Empty response from OpenAI API")
                raise MarketDataError("E005", "Empty response from OpenAI API")

            if response.choices[0].finish_reason == 'length':
                logger.warning("Response may be truncated due to max_completion_tokens limit.")

            content = response.choices[0].message.content

            if not content:
                logger.error("Empty content in OpenAI API response")
                raise MarketDataError("E005", "Empty content in OpenAI API response")

            content = content.strip()
            logger.debug(f"Received response (first 200 chars): {content[:200]}")

            try:
                return json.loads(content)
            except json.JSONDecodeError as je:
                logger.error(f"Failed to parse JSON response: {content[:500]}")
                raise MarketDataError("E005", f"Invalid JSON response: {je}") from je

        except openai.APIError as api_error:
            logger.error(f"OpenAI API error: {api_error}")
            raise MarketDataError("E005", f"API error: {api_error}") from api_error
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            raise MarketDataError("E005", str(e)) from e

    def generate_market_commentary(self):
        logger.info("Generating AI commentary...")
        vix = self.data.get('market', {}).get('vix', {}).get('current', 'N/A')
        t_note = self.data.get('market', {}).get('t_note_future', {}).get('current', 'N/A')
        fear_greed_data = self.data.get('market', {}).get('fear_and_greed', {})
        fear_greed_value = fear_greed_data.get('now', 'N/A')
        fear_greed_category = fear_greed_data.get('category', 'N/A')

        prompt = f"""以下の市場データを基に、日本の個人投資家向けに本日の米国市場の状況を150字程度で簡潔に解説してください。

        - VIX指数: {vix}
        - 米国10年債先物: {t_note}
        - Fear & Greed Index: {fear_greed_value} ({fear_greed_category})

        必ず以下のJSON形式で出力してください：
        {{"response": "ここに解説を記述"}}

        重要：出力は有効なJSONである必要があります。"""

        try:
            response_json = self._call_openai_api(prompt, max_completion_tokens=250)
            self.data['market']['ai_commentary'] = response_json.get('response', 'AI解説の生成に失敗しました。')
        except Exception as e:
            logger.error(f"Failed to generate and parse AI commentary: {e}")
            self.data['market']['ai_commentary'] = "AI解説の生成中にエラーが発生しました。"

    def generate_news_analysis(self):
        """Generates AI news summary and topics based on fetched Yahoo Finance news."""
        logger.info("Generating AI news analysis...")

        raw_news = self.data.get('news_raw')
        if not raw_news:
            logger.warning("No raw news available to generate AI news.")
            self.data['news'] = {
                "summary": "ニュースが取得できなかったため、AIによる分析は行えませんでした。",
                "topics": [],
            }
            return

        # The limit of 5 news items has been removed to allow the AI to analyze all news from the last 24 hours.
        top_news = raw_news

        news_content = ""
        for i, item in enumerate(top_news):
            news_content += f"記事{i+1}:\n"
            news_content += f"  - タイトル: {item['title']}\n"
            news_content += f"  - 概要: {item.get('summary', 'N/A')}\n"
            news_content += f"  - URL: {item['link']}\n\n"

        prompt = f"""
        以下の米国市場に関する最新ニュース記事群を分析し、日本の個人投資家向けに解説してください。

        # ニュース記事
        ---
        {news_content}
        ---

        # 指示
        1.  上記のニュース全体から、今日の市場のムードが最も伝わるように「今朝の3行サマリー」を作成してください。
        2.  次に、以下の「トピック選択の指針」に従って、最も重要と思われる「主要トピック」を3つ選んでください。
        3.  各トピックについて、以下の情報を1つにまとめてください。
            - そのニュースが市場でどのように受け止められているかの「解釈」。
            - 今後の市場（特にS&P 500やNASDAQ）に与えうる短期的な「市場への影響」。
            - 分析の基となった記事の「URL」。

        # トピック選択の指針
        市場全体への影響度が大きいニュースを優先してください。特に、以下の点を重視してください。
        - **巨大テック企業 (Mega-cap Tech):** Apple, Microsoft, NVIDIAなど、時価総額が極めて大きい企業の動向は市場全体に影響を与えやすいため重要です。
        - **マクロ経済:** 金利の変動やVIX指数の動きに直接関連するニュース（例: FRBの金融政策、インフレ指標、雇用統計など）は、最も高い優先度で扱ってください。

        # 出力形式
        以下のJSON形式（外側は二重引用符、内側は一重引用符などJSONのルールを厳守）で、厳密に出力してください。

        {{
          "summary": "（ここに3行のサマリーを記述）",
          "topics": [
            {{
              "title": "（トピック1のタイトル、20文字以内）",
              "analysis": "（解釈と市場への影響をここにまとめて記述）",
              "url": "（基となった記事のURLをここに記述）"
            }},
            {{
              "title": "（トピック2のタイトル、20文字以内）",
              "analysis": "（解釈と市場への影響をここにまとめて記述）",
              "url": "（基となった記事のURLをここに記述）"
            }},
            {{
              "title": "（トピック3のタイトル、20文字以内）",
              "analysis": "（解釈と市場への影響をここにまとめて記述）",
              "url": "（基となった記事のURLをここに記述）"
            }}
          ]
        }}
        """
        try:
            news_data = self._call_openai_api(prompt, max_completion_tokens=1024)
            if isinstance(news_data, str) or 'error' in news_data:
                 raise MarketDataError("E005", f"AI news analysis failed: {news_data}")
            self.data['news'] = news_data
        except Exception as e:
            logger.error(f"Could not generate AI news: {e}")
            self.data['news'] = {
                "summary": "AIによるニュースの分析に失敗しました。",
                "topics": [],
                "error": str(e)
            }

    def generate_column(self):
        # Monday is 0
        if datetime.now(timezone(timedelta(hours=9))).weekday() != 0:
            logger.info("Skipping weekly column generation (not Monday).")
            self.data['column'] = {}
            return

        logger.info("Generating weekly column...")
        vix = self.data.get('market', {}).get('vix', {}).get('current', 'N/A')
        t_note = self.data.get('market', {}).get('t_note_future', {}).get('current', 'N/A')
        fear_greed = self.data.get('market', {}).get('fear_and_greed', {})

        prompt = f"""
        今週の米国市場の展望について、日本の個人投資家向けに300字程度のコラムを執筆してください。
        先週の市場を簡潔に振り返り、今週の注目経済指標やイベントに触れながら、専門家としての洞察を加えてください。

        # 参考データ
        - VIX指数: {vix}
        - 米国10年債先物: {t_note}
        - Fear & Greed Index: 現在値 {fear_greed.get('now', 'N/A')}, 1週間前 {fear_greed.get('prev_week', 'N/A')}
        - 今週の主な経済指標: {self.data.get('indicators', {}).get('economic', [])[:5]}

        # 出力形式
        必ず以下のJSON形式で出力してください：
        {{
            "response": "ここにコラムを記述"
        }}

        重要：出力は有効なJSONである必要があります。
        """
        try:
            response_json = self._call_openai_api(prompt, max_completion_tokens=500)
            content = response_json.get('response', '週次コラムの生成に失敗しました。')
            self.data['column'] = {"weekly_report": {"title": "今週の注目ポイント (AIコラム)", "content": content, "date": datetime.now().strftime('%Y-%m-%d')}}
        except Exception as e:
            logger.error(f"Failed to generate and parse weekly column: {e}")
            self.data['column'] = {}

    def generate_heatmap_commentary(self):
        """Generates AI commentary for heatmaps based on 1-day, 1-week, and 1-month performance."""
        logger.info("Generating heatmap AI commentary...")

        def get_stock_performance(stocks, count=5):
            if not stocks: return [], []
            # Ensure performance is a float for sorting
            valid_stocks = [s for s in stocks if isinstance(s.get('performance'), (int, float))]
            sorted_stocks = sorted(valid_stocks, key=lambda x: x.get('performance', 0), reverse=True)
            top = sorted_stocks[:count]
            bottom = sorted_stocks[-count:]
            return top, bottom

        for index_base_name in ['sp500', 'nasdaq']:
            try:
                heatmap_1d = self.data.get(f'{index_base_name}_heatmap_1d', {})
                if not heatmap_1d.get('stocks'):
                    logger.warning(f"No 1-day data for {index_base_name}, skipping AI commentary.")
                    self.data[f'{index_base_name}_heatmap']['ai_commentary'] = "データ不足のためヒートマップ解説をスキップしました。"
                    continue

                top_5_stocks, bottom_5_stocks = get_stock_performance(heatmap_1d.get('stocks', []))
                top_stocks_str = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in top_5_stocks]) if top_5_stocks else "N/A"
                bottom_stocks_str = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in bottom_5_stocks]) if bottom_5_stocks else "N/A"

                if index_base_name == 'sp500':
                    # --- SP500: Use Sector ETF data ---
                    etf_heatmap_1d = self.data.get('sector_etf_heatmap_1d', {}).get('etfs', [])
                    etf_heatmap_1w = self.data.get('sector_etf_heatmap_1w', {}).get('etfs', [])
                    etf_heatmap_1m = self.data.get('sector_etf_heatmap_1m', {}).get('etfs', [])

                    if not etf_heatmap_1d:
                        logger.warning("No Sector ETF data available for SP500 commentary.")
                        self.data[f'{index_base_name}_heatmap']['ai_commentary'] = "セクターETFデータが不足しているため、ヒートマップ解説をスキップしました。"
                        continue

                    etfs_1d_sorted = sorted(etf_heatmap_1d, key=lambda x: x.get('performance', 0), reverse=True)
                    etfs_1w_sorted = sorted(etf_heatmap_1w, key=lambda x: x.get('performance', 0), reverse=True)
                    etfs_1m_sorted = sorted(etf_heatmap_1m, key=lambda x: x.get('performance', 0), reverse=True)

                    top_3_etfs_1d = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in etfs_1d_sorted[:3]]) if etfs_1d_sorted else "N/A"
                    bottom_3_etfs_1d = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in etfs_1d_sorted[-3:]]) if etfs_1d_sorted else "N/A"
                    top_3_etfs_1w = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in etfs_1w_sorted[:3]]) if etfs_1w_sorted else "N/A"
                    top_3_etfs_1m = ', '.join([f"{s['ticker']} ({s['performance']:.2f}%)" for s in etfs_1m_sorted[:3]]) if etfs_1m_sorted else "N/A"

                    prompt = f"""
                    あなたはプロの金融アナリストです。以下のS&P 500のヒートマップデータと、セクター別ETFのパフォーマンスを分析し、日本の個人投資家向けに、市場の状況を分かりやすく解説してください。

                    # データ
                    ## セクター別ETFパフォーマンス
                    - **1日間**
                      - 上位3セクターETF: {top_3_etfs_1d}
                      - 下位3セクターETF: {bottom_3_etfs_1d}
                    - **1週間**
                      - 上位3セクターETF: {top_3_etfs_1w}
                    - **1ヶ月**
                      - 上位3セクターETF: {top_3_etfs_1m}

                    ## S&P 500 個別銘柄パフォーマンス (1日間)
                    - 上昇上位5銘柄: {top_stocks_str}
                    - 下落上位5銘柄: {bottom_stocks_str}

                    # 指示
                    以下の3つの点を必ず含めて、250字〜300字程度で解説を作成してください。

                    1.  **短期・中期トレンドの要約**: セクターETFの1日、1週間、1ヶ月のデータから、現在の市場の短期的な勢いと中期的なトレンドを読み解いてください。
                    2.  **セクターローテーションの兆候**: 短期と中期のパフォーマンスを比較し、資金がどのセクターからどのセクターへ移動しているか（セクターローテーション）の兆候をETFの動きから指摘してください。例えば、「ハイテク(XLK)からエネルギー(XLE)へ資金が流れている可能性があります」のように記述します。
                    3.  **市場の牽引役**: 1日のパフォーマンスが特に良かったS&P 500の個別銘柄をいくつか挙げ、それらが属するセクターのETFの動きと関連付けて、当日の相場をどのセクター・銘柄が牽引したかを説明してください。

                    # 出力形式
                    必ず以下のJSON形式で出力してください：
                    {{
                        "response": "ここに解説を記述"
                    }}

                    重要：出力は有効なJSONである必要があります。
                    """
                else: # index_base_name == 'nasdaq'
                    prompt = f"""
                    あなたはプロの金融アナリストです。以下の{index_base_name.upper()}のヒートマップデータを分析し、日本の個人投資家向けに、市場の状況を分かりやすく解説してください。

                    # データ
                    ## 個別銘柄パフォーマンス (1日間)
                    - 上昇上位5銘柄: {top_stocks_str}
                    - 下落上位5銘柄: {bottom_stocks_str}

                    # 指示
                    以下の2つの点を必ず含めて、200字〜250字程度で解説を作成してください。

                    1.  **市場の概観**: 上昇・下落が目立った銘柄を基に、当日の{index_base_name.upper()}市場がどのようなテーマで動いたかを要約してください。
                    2.  **注目銘柄**: 特にパフォーマンスが良かった、あるいは悪かった銘柄をいくつか挙げ、その背景にどのようなニュースや要因があった可能性があるかについて、あなたの専門知識を基に推測を加えてください。

                    # 出力形式
                    必ず以下のJSON形式で出力してください：
                    {{
                        "response": "ここに解説を記述"
                    }}

                    重要：出力は有効なJSONである必要があります。
                    """

                response_json = self._call_openai_api(prompt, max_completion_tokens=700)
                commentary = response_json.get('response', 'AI解説の生成に失敗しました。')
                # Assign commentary to the existing dictionary to avoid overwriting other keys
                if f'{index_base_name}_heatmap' not in self.data:
                    self.data[f'{index_base_name}_heatmap'] = {}
                self.data[f'{index_base_name}_heatmap']['ai_commentary'] = commentary

            except Exception as e:
                logger.error(f"Failed to generate and parse AI commentary for {index_base_name}: {e}")
                if f'{index_base_name}_heatmap' not in self.data:
                    self.data[f'{index_base_name}_heatmap'] = {}
                self.data[f'{index_base_name}_heatmap']['ai_commentary'] = "AI解説の生成中にエラーが発生しました。"

    def cleanup_old_data(self):
        """Deletes data files older than 7 days."""
        logger.info("Cleaning up old data files...")
        try:
            today = datetime.now()
            seven_days_ago = today - timedelta(days=7)

            for filename in os.listdir(DATA_DIR):
                match = re.match(r'data_(\d{4}-\d{2}-\d{2})\.json', filename)
                if match:
                    file_date_str = match.group(1)
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    if file_date < seven_days_ago:
                        file_path = os.path.join(DATA_DIR, filename)
                        os.remove(file_path)
                        logger.info(f"Deleted old data file: {filename}")
        except Exception as e:
            logger.error(f"Error during data cleanup: {e}")

    # --- Main Execution Methods ---
    def fetch_all_data(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        logger.info("--- Starting Raw Data Fetch ---")

        fetch_tasks = [
            self.fetch_vix,
            self.fetch_t_note_future,
            self.fetch_fear_greed_index,
            self.fetch_calendar_data,  # Changed from fetch_economic_indicators
            self.fetch_yahoo_finance_news,
            self.fetch_heatmap_data
        ]

        for task in fetch_tasks:
            try:
                task()
            except MarketDataError as e:
                logger.error(f"Failed to execute fetch task '{task.__name__}': {e}")

        # Clean the data before writing to file
        self.data = self._clean_non_compliant_floats(self.data)

        with open(RAW_DATA_PATH, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"--- Raw Data Fetch Completed. Saved to {RAW_DATA_PATH} ---")
        return self.data

    def generate_report(self):
        logger.info("--- Starting Report Generation ---")
        if not os.path.exists(RAW_DATA_PATH):
            logger.error(f"{RAW_DATA_PATH} not found. Run fetch first.")
            return
        with open(RAW_DATA_PATH, 'r', encoding='utf-8') as f:
            self.data = json.load(f)

        # AI Generation Steps
        try:
            self.generate_market_commentary()
        except MarketDataError as e:
            logger.error(f"Could not generate AI commentary: {e}")
            self.data['market']['ai_commentary'] = "現在、AI解説に不具合が生じております。"

        try:
            self.generate_news_analysis()
        except MarketDataError as e:
            logger.error(f"Could not generate AI news: {e}")
            self.data['news'] = {"summary": f"Error: {e}", "topics": []}

        try:
            self.generate_heatmap_commentary()
        except MarketDataError as e:
            logger.error(f"Could not generate heatmap AI commentary: {e}")
            self.data['sp500_heatmap']['ai_commentary'] = f"Error: {e}"
            self.data['nasdaq_heatmap']['ai_commentary'] = f"Error: {e}"

        try:
            self.generate_column()
        except MarketDataError as e:
            logger.error(f"Could not generate weekly column: {e}")
            self.data['column'] = {}

        jst = timezone(timedelta(hours=9))
        self.data['date'] = datetime.now(jst).strftime('%Y-%m-%d')
        self.data['last_updated'] = datetime.now(jst).isoformat()

        # Clean the data before writing to file
        self.data = self._clean_non_compliant_floats(self.data)

        final_path = f"{FINAL_DATA_PATH_PREFIX}{self.data['date']}.json"
        with open(final_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        with open(os.path.join(DATA_DIR, 'data.json'), 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        logger.info(f"--- Report Generation Completed. Saved to {final_path} ---")

        self.cleanup_old_data()

        return self.data


if __name__ == '__main__':
    # For running the script directly, load .env file.
    # In the Docker container, cron runs this from /app/backend,
    # so it should find the .env file in /app.
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