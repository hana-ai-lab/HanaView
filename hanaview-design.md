# HanaView 設計書

## 1. システムアーキテクチャ

### 1.1 全体構成
```
┌─────────────────────────────────────────────┐
│                  Docker Container            │
│                                              │
│  ┌────────────┐  ┌──────────┐  ┌─────────┐ │
│  │  Frontend  │  │  Backend │  │  Cron   │ │
│  │   (HTML/   │←→│ (FastAPI)│←─│  Jobs   │ │
│  │  JS/CSS)   │  │          │  └─────────┘ │
│  └────────────┘  └──────────┘               │
│                        ↓                     │
│                  ┌──────────┐               │
│                  │   Data    │               │
│                  │  Storage  │               │
│                  └──────────┘               │
└─────────────────────────────────────────────┘
                        ↓
            External APIs & Services
     ┌──────────┬──────────┬──────────┐
     │ yfinance │ OpenAI   │  Web     │
     │   API    │   API    │ Scraping │
     └──────────┴──────────┴──────────┘
```

### 1.2 コンポーネント設計

#### 1.2.1 Frontend層
- **技術スタック：** HTML5, CSS3, Vanilla JavaScript
- **主要ライブラリ：** lightweight-charts
- **責務：**
  - UIレンダリング
  - ユーザーインタラクション処理
  - データの可視化
  - レスポンシブ対応

#### 1.2.2 Backend層
- **技術スタック：** Python 3.11, FastAPI
- **主要ライブラリ：**
  - yfinance: 金融データ取得
  - curl-cffi: HTTPリクエスト処理
  - beautifulsoup4: HTML解析
  - openai: AI機能
- **責務：**
  - APIエンドポイント提供
  - データ取得・加工
  - AI解説生成
  - ファイル管理

#### 1.2.3 データ層
- **形式：** JSON
- **保存場所：** ローカルファイルシステム
- **構造：**
  ```json
  {
    "date": "YYYY-MM-DD",
    "last_updated": "ISO 8601",
    "market": {},
    "nasdaq_heatmap": {},
    "sp500_heatmap": {},
    "news": {},
    "indicators": {},
    "column": {}
  }
  ```

## 2. データフロー設計

### 2.1 データ取得フロー
```
6:30 JST → Cron Job (fetch)
    ↓
MarketDataFetcher.fetch_raw_data()
    ↓
┌──────────────────────────────┐
│ 1. VIXデータ取得（yfinance）  │
│ 2. 10年債データ取得           │
│ 3. 経済指標取得（みんかぶ）    │
│ 4. Fear & Greed Index取得(CNN API)│
│ 5. ニュース取得（Yahoo）      │
└──────────────────────────────┘
    ↓
data_raw.json保存

7:00 JST → Cron Job (generate)
    ↓
MarketDataFetcher.generate_report_async()
    ↓
┌──────────────────────────────┐
│ 1. AI解説生成                │
│ 2. 週次コラム生成（月曜のみ）  │
└──────────────────────────────┘
    ↓
data_YYYY-MM-DD.json保存
```

### 2.2 データ表示フロー
```
ユーザーアクセス
    ↓
Frontend (index.html)
    ↓
fetch('/api/data')
    ↓
Backend (FastAPI)
    ↓
最新JSONファイル読み込み
    ↓
JSONレスポンス返却
    ↓
Frontend でレンダリング
```

## 3. API設計

### 3.1 内部API

#### 3.1.1 GET /api/data
- **説明：** 最新の市場データを取得
- **レスポンス：**
  ```json
  {
    "date": "string",
    "last_updated": "string",
    "market": {
      "vix": {
        "current": "number",
        "history": []
      },
      "t_note_future": {},
      "fear_and_greed": {
        "now": "number",
        "previous_close": "number",
        "prev_week": "number",
        "prev_month": "number",
        "prev_year": "number",
        "category": "string"
      },
      "ai_commentary": "string"
    },
    "news": [],
    "indicators": {},
    "column": {}
  }
  ```

#### 3.1.2 GET /api/health
- **説明：** ヘルスチェック
- **レスポンス：** `{"status": "healthy"}`

### 3.2 外部API連携

#### 3.2.1 yfinance
- **用途：** VIX、10年債先物、ニュース取得
- **制限：** 15分遅延データ
- **エラー処理：** セッション再接続、リトライ機能

#### 3.2.2 OpenAI API
- **用途：** 市況解説、週次コラム生成
- **モデル：** gpt-5-mini (gpt-4o-mini)
- **制限：** トークン数制限（500トークン）

#### 3.2.3 Webスクレイピング
- **対象サイト：**
  - Finviz (ヒートマップ)
  - みんかぶFX (経済指標)
- **技術：** Playwright + Stealth Mode

#### 3.2.4 CNN Fear & Greed API
- **用途：** Fear & Greed Indexの時系列データ取得
- **エンドポイント：** `https://production.dataviz.cnn.io/index/fearandgreed/graphdata`
- **エラー処理：** リトライ、失敗時はエラー情報を格納

## 4. UI/UX設計

### 4.1 画面構成

#### 4.1.1 レイアウト
```
┌─────────────────────────────┐
│      Tab Navigation         │
├─────────────────────────────┤
│                             │
│      Content Area           │
│                             │
│   - 市況タブ                │
│   - NASDAQタブ              │
│   - S&P500タブ              │
│   - ニュースタブ            │
│   - 指標タブ                │
│   - コラムタブ              │
│                             │
└─────────────────────────────┘
```

#### 4.1.2 レスポンシブ対応
- **ブレークポイント：** 768px
- **モバイル最適化：**
  - タブのスクロール対応
  - グリッドレイアウトの調整
  - フォントサイズの自動調整

### 4.2 カラースキーム
```css
--background: #E6F3F7;          /* 薄い水色 */
--card-background: #FFFFFF;     /* 白 */
--tab-text: #006B6B;           /* 濃い青緑色 */
--tab-active-bg: #006B6B;      /* 濃い青緑色 */
--tab-active-text: #000000;    /* 黒 */
--text-primary: #212121;       /* テキスト */
--text-secondary: #757575;     /* サブテキスト */
--border-color: #e0e0e0;       /* ボーダー */
```

### 4.3 タブデザイン
- **通常状態**: 白背景（#FFFFFF）に濃い青緑色文字（#006B6B）
- **選択状態**: 濃い青緑色背景（#006B6B）に黒文字（#000000）
- **ホバー効果**: 半透明のオーバーレイ
- **トランジション**: 0.3秒のスムーズな切り替え

### 4.4 Fear & Greed Indexデザイン
- **APIデータに基づく表示**:
  - APIから取得した現在のインデックス値を半円形のゲージメーターで表示
  - 5段階のカテゴリ（Extreme Fear, Fear, Neutral, Greed, Extreme Greed）に応じて色と針の位置を決定
  - 前日比、週次、月次、年次のデータをテキストで表示

### 4.5 ヒートマップデザイン
- **Finvizスタイルの再現**:
  - サイズ順のTreemap配置
  - パフォーマンスベースの色分け
  - ホバー時の詳細情報表示
  - セクター別のグループ化

## 5. セキュリティ設計

### 5.1 認証・認可
- 現バージョンでは認証なし（ローカル環境想定）

### 5.2 API キー管理
- 環境変数による管理
- .envファイルのGit除外
- Docker環境変数としての受け渡し

### 5.3 データ保護
- 外部APIアクセスのレート制限対応
- エラーログでの機密情報除外

## 6. エラー処理設計

### 6.1 エラー分類

| レベル | 種類 | 処理 |
|--------|------|------|
| Critical | APIキー不正 | サービス停止、ログ記録 |
| Error | データ取得失敗 | リトライ、代替データ使用 |
| Warning | 部分的失敗 | 継続実行、ログ記録 |
| Info | 正常処理 | ログ記録のみ |

### 6.2 フォールバック戦略
1. **データ取得失敗時：** 前回のデータを使用
2. **API制限時：** 待機後リトライ（最大3回）
3. **Fear & Greedデータ取得失敗：** スキップして継続

## 7. パフォーマンス設計

### 7.1 最適化戦略
- チャートライブラリの遅延読み込み
- 画像のBase64エンコード（キャッシュ効果）
- JSONファイルの圧縮保存
- 非同期処理による並列実行

### 7.2 キャッシュ戦略
- ブラウザキャッシュの活用
- データの差分更新
- 静的ファイルのCDN化（将来）

## 8. 運用設計

### 8.1 デプロイメント
```bash
# ビルド
docker-compose build

# 起動
docker-compose up -d

# 停止
docker-compose down
```

### 8.2 監視項目
- Cronジョブの実行状況
- データ更新の成功/失敗
- ディスク使用量
- APIクォータ使用状況

### 8.3 バックアップ
- データファイルの日次保存
- 7日間の自動保持
- ログファイルの定期ローテーション

## 9. テスト設計

### 9.1 単体テスト
- データ取得関数のモック化
- AI生成機能のテスト
- エラー処理のテスト

### 9.2 統合テスト
- エンドツーエンドの動作確認
- 外部API連携テスト
- Cron実行テスト

### 9.3 性能テスト
- ページロード時間測定
- 同時アクセステスト
- メモリ使用量監視