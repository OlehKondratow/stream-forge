import os
import ssl
import json
from pathlib import Path
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# Загрузка переменных окружения
# ───────────────────────────────────────────────
env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

# ───────────────────────────────────────────────
# Загрузка конфигурации индикаторов из config.json
# ───────────────────────────────────────────────
CONFIG_FILE_PATH = Path(__file__).parent.parent / "config.json"
try:
    with open(CONFIG_FILE_PATH, "r") as f:
        APP_CONFIG = json.load(f)
except FileNotFoundError:
    raise RuntimeError(f"Config file not found at {CONFIG_FILE_PATH}")
except json.JSONDecodeError:
    raise RuntimeError(f"Error decoding JSON from config file at {CONFIG_FILE_PATH}")

# ───────────────────────────────────────────────
# Основные переменные
# ───────────────────────────────────────────────
QUEUE_ID: str = os.getenv("QUEUE_ID", "indicator-calculator-default")
SYMBOL: str = APP_CONFIG.get("symbol", os.getenv("SYMBOL", "BTCUSDT"))
DB_COLLECTION: str = APP_CONFIG.get("db_collection", os.getenv("DB_COLLECTION", "technical_indicators_stream"))
INDICATORS_CONFIG: list = APP_CONFIG.get("indicators", [])

# ───────────────────────────────────────────────
# Kafka общие параметры
# ───────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", f"{SYMBOL.lower()}-orderbook") # Input topic for orderbook data

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
