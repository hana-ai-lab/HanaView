# This file will contain the FastAPI application.
import os
import json
import re
from datetime import datetime, timedelta, timezone
from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from jose import JWTError, jwt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
AUTH_PIN = os.getenv("AUTH_PIN")
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_DAYS = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_DAYS", 30))
TOKEN_NAME = "access_token"

# --- FastAPI App Initialization ---
app = FastAPI()

# --- Project Directories ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
FRONTEND_DIR = os.path.join(PROJECT_ROOT, 'frontend')

# --- Pydantic Models ---
class PinVerification(BaseModel):
    pin: str

# --- Helper Functions ---
def create_access_token(data: dict, expires_delta: timedelta):
    """Creates a new JWT access token."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, JWT_SECRET_KEY, algorithm=ALGORITHM)
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

# --- API Endpoints ---

@app.post("/api/auth/verify")
def verify_pin(pin_data: PinVerification, response: Response):
    """
    Verifies the 6-digit PIN. If correct, sets an HTTPOnly cookie.
    """
    if pin_data.pin == AUTH_PIN:
        expires = timedelta(days=ACCESS_TOKEN_EXPIRE_DAYS)
        access_token = create_access_token(data={"sub": "user"}, expires_delta=expires)

        response.set_cookie(
            key=TOKEN_NAME,
            value=access_token,
            httponly=True,
            max_age=expires.total_seconds(),
            samesite="lax",
            path="/",
            secure=False  # Set to True in production with HTTPS
        )
        return {"message": "Authentication successful"}
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect authentication code",
        )

@app.get("/api/auth/check")
def check_authentication(request: Request):
    """
    Checks if the user has a valid authentication cookie.
    """
    token = request.cookies.get(TOKEN_NAME)
    if not token:
        return {"authenticated": False}

    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        # You can add more checks here, e.g., for the 'sub' field
        if payload:
            return {"authenticated": True}
    except JWTError:
        # This handles expired tokens, invalid signatures, etc.
        return {"authenticated": False}

    return {"authenticated": False}


@app.get("/api/health")
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


# --- Authentication Dependency ---
credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Could not validate credentials",
    headers={"WWW-Authenticate": "Bearer"},
)

async def get_current_user(request: Request):
    """Dependency to get and validate the current user from the auth token cookie."""
    token = request.cookies.get(TOKEN_NAME)
    if token is None:
        raise credentials_exception
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    return username

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

# Mount the frontend directory to serve static files
# This must come AFTER all API routes
app.mount("/", StaticFiles(directory=FRONTEND_DIR, html=True), name="static")
