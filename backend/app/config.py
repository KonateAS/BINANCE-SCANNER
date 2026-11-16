import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")
REST_URL = os.getenv("BINANCE_REST", "https://fapi.binance.com").rstrip("/")
WS_URL = os.getenv("BINANCE_WS", "wss://fstream.binance.com/market/stream")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://scanner:scanner_local@localhost:5432/scanner")
HISTORY_CANDLES = max(40, int(os.getenv("HISTORY_CANDLES", "120")))
WS_GROUP_SIZE = max(1, min(200, int(os.getenv("WS_GROUP_SIZE", "180"))))
WS_SILENCE_TIMEOUT_SECONDS = max(30, int(os.getenv("WS_SILENCE_TIMEOUT_SECONDS", "180")))
REST_OPERATOR_TOKEN = os.getenv("REST_OPERATOR_TOKEN", "").strip()
TIMEFRAMES = ("5m", "15m", "1h", "4h")
