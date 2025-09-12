# HanaView 詳細仕様書

## 1. システム仕様

### 1.1 動作環境仕様

#### 1.1.1 サーバー環境
| 項目 | 仕様 |
|------|------|
| OS | Linux (Docker対応) |
| Docker | 20.10以降 |
| Docker Compose | 2.0以降 |
| CPU | 2コア以上推奨 |
| メモリ | 4GB以上推奨 |
| ストレージ | 10GB以上の空き容量 |

#### 1.1.2 クライアント環境
| 項目 | 仕様 |
|------|------|
| ブラウザ | Chrome 90+, Safari 14+, Firefox 88+, Edge 90+ |
| 画面解像度 | 1024x768以上推奨 |
| JavaScript | 有効化必須 |
| ネットワーク | ブロードバンド接続推奨 |

### 1.2 使用技術仕様

#### 1.2.1 Backend
| 技術 | バージョン | 用途 |
|------|------------|------|
| Python | 3.11 | メイン言語 |
| FastAPI | 0.104.1 | Webフレームワーク |
| uvicorn | 0.24.0 | ASGIサーバー |
| yfinance | 0.2.33 | 金融データ取得 |
| curl-cffi | 0.5.10 | HTTPクライアント |
| beautifulsoup4 | 4.12.2 | HTML解析 |
| openai | 1.6.1 | AI機能 |
| pandas | 2.1.4 | データ処理 |
| Pillow | 10.1.0 | 画像処理 |

#### 1.2.2 Frontend
| 技術 | バージョン | 用途 |
|------|------------|------|
| HTML5 | - | マークアップ |
| CSS3 | - | スタイリング |
| JavaScript | ES6+ | インタラクション |
| lightweight-charts | 最新 | チャート表示 |

## 2. データ仕様

### 2.1 データファイル仕様

#### 2.1.1 メインデータファイル
**ファイル名：** `data_YYYY-MM-DD.json`

**ファイル構造：**
```json
{
  "date": "YYYY-MM-DD",
  "last_updated": "YYYY-MM-DDTHH:MM:SS+09:00",
  "market": {
    "vix": {
      "current": 15.23,
      "history": [
        {
          "time": "2025-01-01T09:00:00",
          "open": 15.10,
          "high": 15.45,
          "low": 15.05,
          "close": 15.23
        }
      ]
    },
    "t_note_future": {
      "current": 109.50,
      "history": []
    },
    "fear_and_greed": {
      "now": 48,
      "previous_close": 47,
      "prev_week": 45,
      "prev_month": 55,
      "prev_year": 70,
      "category": "Fear"
    },
    "ai_commentary": "市況解説テキスト"
  },
  "news": [
    {
      "title": "ニュースタイトル",
      "publisher": "発行元",
      "link": "URL",
      "published": "YYYY-MM-DDTHH:MM:SS"
    }
  ],
  "indicators": {
    "economic": [
      {
        "date": "1月1日（水）",
        "time": "22:30",
        "country": "US",
        "name": "ISM製造業景況指数",
        "importance": 3,
        "previous_fluctuation": "±5pips",
        "previous_value": "48.7",
        "forecast": "49.3",
        "result": "---"
      }
    ]
  },
  "column": {
    "weekly_report": {
      "title": "今週の注目ポイント",
      "content": "コラム内容",
      "date": "YYYY-MM-DD"
    }
  }
}
```

#### 2.1.2 中間データファイル
**ファイル名：** `data_raw.json`
**用途：** データ取得とレポート生成の分離のための一時ファイル
**保持期間：** 次回実行まで

### 2.2 ログファイル仕様

#### 2.2.1 Cronログ
**ファイル名：** `logs/cron.log`
```
2025-01-01 06:30:00: Starting data fetch...
2025-01-01 06:35:00: Data fetch completed
2025-01-01 07:00:00: Starting report generation...
2025-01-01 07:03:00: Report generation completed
```

#### 2.2.2 処理ログ
**ファイル名：** `logs/fetch.log`, `logs/generate.log`
**フォーマット：** `YYYY-MM-DD HH:MM:SS - LEVEL - メッセージ`

## 3. API仕様

### 3.1 内部API仕様

#### 3.1.1 市場データ取得API
```
GET /api/data
```

**リクエスト：** なし

**レスポンス：**
```json
{
  "date": "string",
  "last_updated": "string (ISO 8601)",
  "market": {
    "vix": {
      "current": "number",
      "history": "array",
      "error": "string (optional)"
    },
    "t_note_future": {
      "current": "number",
      "history": "array",
      "error": "string (optional)"
    },
    "ai_commentary": "string"
  },
  "news": "array",
  "indicators": "object",
  "column": "object"
}
```

**ステータスコード：**
- 200: 成功
- 404: データなし
- 500: サーバーエラー

#### 3.1.2 ヘルスチェックAPI
```
GET /api/health
```

**レスポンス：**
```json
{
  "status": "healthy"
}
```

### 3.2 外部API仕様

#### 3.2.1 OpenAI API
**エンドポイント：** `https://api.openai.com/v1/chat/completions`

**リクエスト例：**
```python
{
  "model": "gpt-5-mini",
  "messages": [
    {"role": "system", "content": "システムプロンプト"},
    {"role": "user", "content": "ユーザープロンプト"}
  ],
  "max_completion_tokens": 500,
  "temperature": 0.8
}
```

#### 3.2.2 yfinance データ取得
**ティッカーシンボル：**
- VIX: `^VIX`
- 10年債利回り: `^TNX`
- S&P500: `^GSPC`
- NASDAQ: `^IXIC`
- DOW: `^DJI`

**データ取得例：**
```python
ticker = yf.Ticker("^VIX", session=session)
hist = ticker.history(period="5d", interval="1h")
```

## 4. 処理仕様

### 4.1 データ取得処理（6:30実行）

#### 4.1.1 処理フロー
1. **VIXデータ取得**
   - yfinanceから^VIXの5日間1時間足データ取得
   - 4時間足にリサンプリング
   - JSONフォーマットに変換

2. **10年債先物データ取得**
   - yfinanceから^TNXの5日間1時間足データ取得
   - 4時間足にリサンプリング
   - JSONフォーマットに変換

3. **経済指標取得**
   - みんかぶFXにアクセス
   - HTMLパース
   - 重要度3以上の指標を抽出

4. **Fear & Greed Index取得**
   - CNN Fear & Greed APIから過去1年分のデータを取得
   - 現在、前日、1週間前、1ヶ月前、1年前のデータを抽出
   - JSONフォーマットに変換

5. **ニュース取得**
   - 主要指数のニュースを取得
   - 重複除去
   - 上位5件を選択

6. **中間データ保存**
   - data_raw.jsonに保存

#### 4.1.2 Fear & Greed Index取得 実装例
```python
def fetch_fear_greed_index(self):
    """Fear & Greed Indexを取得"""
    try:
        # CNN Fear & Greed APIから取得
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        url = f"{CNN_FEAR_GREED_URL}{start_date}"

        response = requests.get(url, impersonate="chrome110", timeout=30)
        data = response.json()

        # 最新データと履歴データを整理
        fear_greed_data = data.get('fear_and_greed_historical', {}).get('data', [])

        if fear_greed_data:
            # 現在の値
            current = fear_greed_data[-1]
            current_value = current['y']

            # 過去の値を計算
            now = datetime.now()
            prev_close = self._get_historical_value(fear_greed_data, 1)
            week_ago = self._get_historical_value(fear_greed_data, 7)
            month_ago = self._get_historical_value(fear_greed_data, 30)
            year_ago = self._get_historical_value(fear_greed_data, 365)

            self.data['market']['fear_and_greed'] = {
                'now': round(current_value),
                'previous_close': round(prev_close) if prev_close else None,
                'prev_week': round(week_ago) if week_ago else None,
                'prev_month': round(month_ago) if month_ago else None,
                'prev_year': round(year_ago) if year_ago else None,
                'category': self._get_fear_greed_category(current_value)
            }
            logger.info(f"Fear & Greed Index fetched: {current_value}")

    except Exception as e:
        logger.error(f"Error fetching Fear & Greed Index: {e}")
        self.data['market']['fear_and_greed'] = {
            'now': None,
            'error': str(e)
        }

def _get_historical_value(self, data, days_ago):
    """指定日前のデータを取得"""
    target_timestamp = (datetime.now() - timedelta(days=days_ago)).timestamp() * 1000

    # 最も近いタイムスタンプのデータを探す
    closest_data = None
    min_diff = float('inf')

    for item in data:
        diff = abs(item['x'] - target_timestamp)
        if diff < min_diff:
            min_diff = diff
            closest_data = item

    return closest_data['y'] if closest_data else None

def _get_fear_greed_category(self, value):
    """Fear & Greedのカテゴリを判定"""
    if value <= 25:
        return "Extreme Fear"
    elif value <= 45:
        return "Fear"
    elif value <= 55:
        return "Neutral"
    elif value <= 75:
        return "Greed"
    else:
        return "Extreme Greed"
```

### 4.2 レポート生成処理（7:00実行）

#### 4.2.1 処理フロー
1. **中間データ読み込み**
   - data_raw.jsonを読み込み

2. **AI市況解説生成**
   ```python
   prompt = f"""
   以下の市場データを基に、日本の個人投資家向けに
   本日の米国市場の状況を簡潔に解説してください。
   
   VIX: {vix_value}
   米国10年債先物: {t_note_value}
   
   150文字程度で解説してください
   """
   ```

4. **週次コラム生成（月曜のみ）**
   - 300文字程度
   - 先週の振り返りと今週の注目点

5. **最終データ保存**
   - data_YYYY-MM-DD.jsonに保存
   - 7日前のファイル削除

### 4.3 フロントエンド処理

#### 4.3.1 初期化処理
```javascript
1. DOMContentLoaded イベント待機
2. InvestmentDashboard クラスのインスタンス化
3. /api/data からデータ取得
4. イベントリスナー設定
5. 初期タブ（市況）のレンダリング
```

#### 4.3.2 タブ切り替え処理
```javascript
1. タブボタンクリックイベント
2. アクティブクラスの更新
3. コンテンツエリアの切り替え
4. 該当タブのレンダリング関数呼び出し
```

#### 4.3.3 チャート描画処理
```javascript
1. lightweight-charts ライブラリの初期化
2. チャート設定（色、グリッド等）
3. データのフォーマット変換
4. チャートへのデータセット
5. 現在値の表示
```

## 5. エラー処理仕様

### 5.1 エラーコード定義

| コード | 説明 | 対処 |
|--------|------|------|
| E001 | OpenAI APIキー未設定 | 環境変数確認 |
| E002 | データファイル読み込み失敗 | ファイル存在確認 |
| E003 | 外部API接続失敗 | リトライまたはスキップ |
| E004 | Fear&Greedデータ取得失敗 | キャッシュまたはデフォルト値使用 |
| E005 | AI生成失敗 | デフォルトメッセージ表示 |
| E006 | ヒートマップデータ取得失敗 | 前回データまたは簡易表示 |

### 5.2 リトライ仕様

```python
MAX_RETRIES = 3
RETRY_DELAY = 5  # 秒

for attempt in range(MAX_RETRIES):
    try:
        # 処理実行
        break
    except Exception as e:
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)
        else:
            logger.error(f"Failed after {MAX_RETRIES} attempts")
```

## 6. Cron仕様

### 6.1 スケジュール設定

```cron
# データ取得（月曜〜土曜 6:30）
30 6 * * 1-6 /app/backend/cron_job.sh

# レポート生成（月曜〜土曜 7:00）
0 7 * * 1-6 /app/backend/cron_job_generate.sh
```

### 6.2 実行スクリプト

**cron_job.sh:**
```bash
#!/bin/bash
LOG_DIR="/app/logs"
mkdir -p $LOG_DIR
echo "$(date): Starting data fetch..." >> $LOG_DIR/cron.log
cd /app/backend
python data_fetcher.py fetch >> $LOG_DIR/fetch.log 2>&1
echo "$(date): Data fetch completed" >> $LOG_DIR/cron.log
```

## 7. Docker仕様

### 7.1 コンテナ構成

```yaml
services:
  app:
    build: .
    ports:
      - "8000:8000"
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
    environment:
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - TZ=Asia/Tokyo
    restart: unless-stopped
```

### 7.2 ビルド仕様

```dockerfile
FROM python:3.11-slim-bookworm
WORKDIR /app

# システムパッケージインストール
RUN apt-get update && apt-get install -y \
    cron curl wget gnupg

# Pythonパッケージインストール
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションファイルコピー
COPY backend /app/backend
COPY frontend /app/frontend

# 起動コマンド
CMD cron && cd /app/backend && \
    uvicorn main:app --host 0.0.0.0 --port 8000
```

## 8. セキュリティ仕様

### 8.1 APIキー管理
- 環境変数での管理
- .envファイルのGit除外
- ログ出力時のマスキング

### 8.2 アクセス制御
- 現バージョンでは認証なし
- ローカルネットワークでの使用想定
- 将来的にBasic認証追加予定

## 9. 運用仕様

### 9.1 初回セットアップ手順
1. リポジトリクローン
2. data, logsディレクトリ作成
3. 環境変数設定（OPENAI_API_KEY）
4. docker-compose up -d --build
5. 手動データ取得（オプション）

### 9.2 メンテナンス作業
- ログファイルの定期削除（月次）
- Dockerイメージの更新（月次）
- 依存パッケージの更新（四半期）

### 9.3 バックアップ仕様
- データファイル：7日間自動保持
- ログファイル：手動削除まで保持
- 設定ファイル：Gitで管理