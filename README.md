# HanaView Market Dashboard

## 1. 概要 (Overview)

HanaViewは、個人投資家が毎朝の市場チェックを効率化するための統合ダッシュボードです。
このアプリケーションは、VIX、Fear & Greed Index、米国10年債などの主要な市場指標、S&P 500とNASDAQ 100のヒートマップ、経済指標カレンダーなどを一元的に表示します。

データは毎日定時に自動で更新されますが、管理者が手動で更新プロセスを実行することも可能です。

## 2. セットアップ手順 (Setup)

### 前提条件
- Docker
- Docker Compose
- `git`

### インストールと起動
1.  **リポジトリをクローンします。**
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **環境変数ファイルを作成します。**
    プロジェクトのルートに `.env` ファイルを作成し、OpenAI APIキーを設定します。
    ```
    OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    ```

3.  **Dockerコンテナをビルドして起動します。**
    ```bash
    docker-compose up -d --build
    ```
    初回起動には数分かかることがあります。

4.  **アプリケーションにアクセスします。**
    ブラウザで `http://localhost` を開きます。

## 3. 手動でのデータ更新 (Manual Data Update)

データはcronによって自動的に更新されますが、管理者は以下の手順で手動で更新プロセスをトリガーできます。

1.  **実行中のコンテナ内でbashセッションを開始します。**
    ```bash
    docker compose exec app bash
    ```

2.  **データ取得 (fetch) を実行します。**
    コンテナ内で以下のコマンドを実行すると、外部APIから最新の生データが取得され、`data/data_raw.json` に保存されます。
    **注意:** この処理は、S&P 500とNASDAQ 100の全銘柄（約600）の情報を取得するため、完了までに5〜10分程度かかる場合があります。
    ```bash
    python -m backend.data_fetcher fetch
    ```

3.  **レポート生成 (generate) を実行します。**
    `fetch`が完了したら、以下のコマンドを実行します。これにより、`data_raw.json` が読み込まれ、AIによる解説（設定済みの場合）が追加され、最終的なデータファイル `data/data_YYYY-MM-DD.json` および `data/data.json` が生成されます。
    ```bash
    python -m backend.data_fetcher generate
    ```

これで、フロントエンドに表示されるデータが手動で更新されます。

## 4. VPSへのデプロイ手順 (Deployment to VPS)

このセクションでは、本アプリケーションを一般的なVPS（Virtual Private Server）にデプロイする手順を解説します。この手順では、NginxやHTTPS化を行わず、HTTPで直接アプリケーションを公開します。

### 4.1. 前提条件

- **VPS契約**: サーバーが利用可能な状態であること。
- **ドメイン取得 (任意)**: ドメインを使用する場合は取得済みであること。

### 4.2. サーバーの初期設定

VPSにSSHでログインし、DockerとDocker Composeをインストールします。

```bash
# Ubuntu/Debianの場合
sudo apt-get update
sudo apt-get install -y docker.io docker-compose git

# CentOSの場合
sudo yum update -y
sudo yum install -y docker docker-compose git
sudo systemctl start docker
sudo systemctl enable docker
```

### 4.3. DNSの設定 (ドメインを使用する場合)

ドメインのDNS設定で、VPSのIPアドレスを指す**Aレコード**を作成します。
- **タイプ**: A
- **名前**: `example.com` (ドメイン名) または `@`
- **IPv4アドレス**: VPSのIPアドレス

*注意: Cloudflareなどのプロキシは使用しないでください。使用するとHTTP接続に問題が発生する可能性があります。*

### 4.4. アプリケーションのデプロイ

1.  **サーバーにリポジトリをクローンします。**
    VPSにSSHでログインし、任意の場所にリポジトリをクローンします。
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  **環境変数ファイルを作成します。**
    プロジェクトのルートに `.env` ファイルを作成し、OpenAI APIキーを設定します。
    ```
    OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
    ```

3.  **アプリケーションを起動します。**
    プロジェクトディレクトリ内で、Docker Composeを起動します。
    ```bash
    sudo docker-compose up -d --build
    ```

これでデプロイは完了です。ブラウザで `http://<VPSのIPアドレス>` または `http://<あなたのドメイン>` にアクセスすると、アプリケーションが表示されます。
