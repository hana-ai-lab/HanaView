# This file will contain the FastAPI application.
import os
import json
import re
from datetime import datetime, timedelta, timezone
from fastapi import Depends, FastAPI, HTTPException, Header, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from jose import JWTError, jwt
from dotenv import load_dotenv
from pywebpush import webpush, WebPushException
from typing import Dict, Any, Optional

# Import security manager
from .security_manager import security_manager

# Load environment variables from .env file
load_dotenv()

# --- FastAPI App Initialization ---
app = FastAPI()

# --- Project Directories ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
FRONTEND_DIR = os.path.join(PROJECT_ROOT, 'frontend')

# --- Initialize security keys on startup ---
@app.on_event("startup")
async def startup_event():
    """Initialize security keys on application startup"""
    security_manager.data_dir = DATA_DIR
    security_manager.initialize()

    # 設定の確認表示
    print("\n" + "=" * 60)
    print("HanaView Security Configuration Initialized")
    print("=" * 60)
    print(f"JWT Secret: ***{security_manager.jwt_secret[-8:]}")
    print(f"VAPID Public Key: {security_manager.vapid_public_key[:20]}...")
    print(f"VAPID Subject: {security_manager.vapid_subject}")
    print("=" * 60 + "\n")

# --- Configuration ---
AUTH_PIN = os.getenv("AUTH_PIN", "123456")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_DAYS", 30))

# In-memory storage for subscriptions
push_subscriptions: Dict[str, Any] = {}

# --- Pydantic Models ---
class PinVerification(BaseModel):
    pin: str

class PushSubscription(BaseModel):
    endpoint: str
    keys: Dict[str, str]
    expirationTime: Any = None

# --- Helper Functions ---
def create_access_token(data: dict, expires_delta: timedelta):
    """Creates a new JWT access token."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, security_manager.jwt_secret, algorithm=ALGORITHM)
    return encoded_jwt

def get_latest_data_file():
    """Finds the latest data_YYYY-MM-DD.json file in the DATA_DIR."""
    if not os.path.isdir(DATA_DIR):
        return None
    files = os.listdir(DATA_DIR)
    data_files = [f for f in files if re.match(r'^data_(\d{4}-\d{2}-\d{2})\.json$', f)]
    if not data_files:
        fallback_path = os.path.join(DATA_DIR, 'data.json')
        return fallback_path if os.path.exists(fallback_path) else None
    latest_file = sorted(data_files, reverse=True)[0]
    return os.path.join(DATA_DIR, latest_file)

# --- Authentication Dependency ---
async def get_current_user(authorization: Optional[str] = Header(None)):
    """Authorizationヘッダーからトークンを取得して検証"""

    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization header missing",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Bearer トークンの形式チェック
    if not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization header format",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = authorization[7:]  # "Bearer " を除去

    try:
        payload = jwt.decode(token, security_manager.jwt_secret, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token payload"
            )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials"
        )

    return username

# --- API Endpoints ---

@app.post("/api/auth/verify")
def verify_pin(pin_data: PinVerification):
    """PINを検証し、JWTトークンを返す（クッキーは設定しない）"""

    if pin_data.pin == AUTH_PIN:
        expires = timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
        access_token = create_access_token(
            data={"sub": "user"},
            expires_delta=expires
        )

        # トークンをJSONレスポンスで返す（クッキー設定なし）
        return {
            "success": True,
            "token": access_token,
            "expires_in": int(expires.total_seconds()),
            "expires_at": (datetime.now(timezone.utc) + expires).isoformat()
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect authentication code"
        )

@app.get("/api/auth/check")
def check_authentication(authorization: Optional[str] = Header(None)):
    """認証状態を確認（Authorizationヘッダーを使用）"""

    if not authorization or not authorization.startswith("Bearer "):
        return {"authenticated": False}

    token = authorization[7:]

    try:
        payload = jwt.decode(token, security_manager.jwt_secret, algorithms=[ALGORITHM])
        if payload and payload.get("sub"):
            return {"authenticated": True}
    except JWTError:
        pass

    return {"authenticated": False}

@app.post("/api/auth/logout")
def logout():
    """ログアウト（クライアント側でトークンを削除）"""
    return {"success": True, "message": "Please clear local token"}

@app.get("/api/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}

@app.get("/api/data")
def get_market_data(current_user: str = Depends(get_current_user)):
    """Endpoint to get the latest market data."""
    try:
        data_file = get_latest_data_file()
        if data_file is None or not os.path.exists(data_file):
            raise HTTPException(status_code=404, detail="Data file not found.")
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/vapid-public-key")
def get_vapid_public_key():
    """Returns the VAPID public key for push notifications."""
    return {"public_key": security_manager.vapid_public_key}

@app.post("/api/subscribe")
async def subscribe_push(subscription: PushSubscription, current_user: str = Depends(get_current_user)):
    """Subscribe to push notifications for 6:30 AM updates."""
    # Store subscription (in production, use a database)
    subscription_id = str(hash(subscription.endpoint))
    push_subscriptions[subscription_id] = subscription.dict()

    # Save to file for persistence
    subscriptions_file = os.path.join(DATA_DIR, 'push_subscriptions.json')
    try:
        # Load existing subscriptions
        existing = {}
        if os.path.exists(subscriptions_file):
            with open(subscriptions_file, 'r') as f:
                existing = json.load(f)

        # Add new subscription
        existing[subscription_id] = subscription.dict()

        # Save back
        with open(subscriptions_file, 'w') as f:
            json.dump(existing, f)
    except Exception as e:
        print(f"Error saving subscription: {e}")

    return {"status": "subscribed", "id": subscription_id}

@app.post("/api/send-notification")
async def send_notification(current_user: str = Depends(get_current_user)):
    """Manually send push notification to all subscribers (for testing)."""
    # Load subscriptions from file
    subscriptions_file = os.path.join(DATA_DIR, 'push_subscriptions.json')
    if not os.path.exists(subscriptions_file):
        return {"sent": 0, "failed": 0, "message": "No subscriptions found"}

    with open(subscriptions_file, 'r') as f:
        saved_subscriptions = json.load(f)

    notification_data = {
        "title": "HanaView テスト通知",
        "body": "手動送信のテスト通知です",
        "type": "test"
    }

    sent_count = 0
    failed_count = 0

    for sub_id, subscription in list(saved_subscriptions.items()):
        try:
            webpush(
                subscription_info=subscription,
                data=json.dumps(notification_data),
                vapid_private_key=security_manager.vapid_private_key,
                vapid_claims={
                    "sub": security_manager.vapid_subject,
                }
            )
            sent_count += 1
        except WebPushException as ex:
            print(f"Push failed for {sub_id}: {ex}")
            # Remove invalid subscription
            if ex.response and ex.response.status_code == 410:
                del saved_subscriptions[sub_id]
            failed_count += 1

    # Save updated subscriptions
    with open(subscriptions_file, 'w') as f:
        json.dump(saved_subscriptions, f)

    return {
        "sent": sent_count,
        "failed": failed_count
    }

# Mount the frontend directory to serve static files
# This must come AFTER all API routes
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="static")