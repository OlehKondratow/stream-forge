import os
import ssl
import json # Keep json import for now, might be needed for INDICATORS_CONFIG parsing
from pathlib import Path
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# Загрузка переменных окружения
# ───────────────────────────────────────────────
env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

# ───────────────────────────────────────────────
# Основные переменные
# ───────────────────────────────────────────────
QUEUE_ID: str = os.getenv("QUEUE_ID", "indicator-calculator-default")
# SYMBOL и DB_COLLECTION теперь будут браться только из переменных окружения
SYMBOL: str = os.getenv("SYMBOL", "BTCUSDT")
DB_COLLECTION: str = os.getenv("DB_COLLECTION", "technical_indicators_stream")

# INDICATORS_CONFIG теперь будет браться из переменной окружения,
# которая должна быть JSON-строкой. Если переменная не задана,
# будет использоваться пустой список.
INDICATORS_CONFIG_STR: str = os.getenv("INDICATORS_CONFIG", "[]")
try:
    INDICATORS_CONFIG: list = json.loads(INDICATORS_CONFIG_STR)
except json.JSONDecodeError:
    raise ValueError("INDICATORS_CONFIG environment variable is not a valid JSON string.")

# Candle Aggregation Settings
CANDLE_INTERVAL: str = os.getenv("CANDLE_INTERVAL", "5m") # e.g., "1s", "5s", "1m"
CANDLE_WINDOW_SIZE: int = int(os.getenv("CANDLE_WINDOW_SIZE", "40")) # Number of candles to keep for indicator calculation
VOLUME_PROFILE_PRICE_STEP: float = float(os.getenv("VOLUME_PROFILE_PRICE_STEP", "0.0001")) # Price step for volume profile

# ───────────────────────────────────────────────
# Kafka общие параметры
# ───────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
KAFKA_TOPIC_ORDERBOOK: str = os.getenv("KAFKA_TOPIC_ORDERBOOK", f"{SYMBOL.lower()}-orderbook")
KAFKA_TOPIC_TRADES: str = os.getenv("KAFKA_TOPIC_TRADES", f"{SYMBOL.lower()}-trades")
KAFKA_TOPICS_LIST: list[str] = [KAFKA_TOPIC_ORDERBOOK, KAFKA_TOPIC_TRADES]

KAFKA_USER_PRODUCER: str = os.getenv("KAFKA_USER_PRODUCER", "")
KAFKA_PASSWORD_PRODUCER: str = os.getenv("KAFKA_PASSWORD_PRODUCER", "")
KAFKA_USER_CONSUMER: str = os.getenv("KAFKA_USER_CONSUMER", "")
KAFKA_PASSWORD_CONSUMER: str = os.getenv("KAFKA_PASSWORD_CONSUMER", "")
CA_PATH: str = os.getenv("CA_PATH", "/usr/local/share/ca-certificates/ca.crt")

# ───────────────────────────────────────────────
# Топики телеметрии и управления
# ───────────────────────────────────────────────
TELEMETRY_TOPIC: str = os.getenv("TELEMETRY_TOPIC", "queue-events")
CONTROL_TOPIC: str = os.getenv("CONTROL_TOPIC", "queue-control")

# ───────────────────────────────────────────────
# ArangoDB
# ───────────────────────────────────────────────
ARANGO_URL: str = os.getenv("ARANGO_URL", "http://localhost:8529")
ARANGO_DB: str = os.getenv("ARANGO_DB", "streamforge")
ARANGO_USER: str = os.getenv("ARANGO_USER", "root")
ARANGO_PASSWORD: str = os.getenv("ARANGO_PASSWORD", "")

# ───────────────────────────────────────────────
# SSL context helper (если вдруг используем client cert)
# ───────────────────────────────────────────────
def get_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=CA_PATH)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    return context
