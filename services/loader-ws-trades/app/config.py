# app/config.py

import os
import ssl
from pathlib import Path
from dotenv import load_dotenv

# ───────────────────────────────────────────────
# Загрузка переменных окружения
# ───────────────────────────────────────────────
env_path = Path(".") / ".env"
load_dotenv(dotenv_path=env_path)

# ───────────────────────────────────────────────
# Основные переменные очереди
# ───────────────────────────────────────────────
QUEUE_ID: str = os.getenv("QUEUE_ID", "unknown-queue")
SYMBOL: str = os.getenv("SYMBOL", "BTCUSDT")
TYPE: str = os.getenv("TYPE", "ws_trades")

# ───────────────────────────────────────────────
# Binance WebSocket
# ───────────────────────────────────────────────
BINANCE_WS_URL: str = os.getenv("BINANCE_WS_URL", f"wss://stream.binance.com:9443/ws/{SYMBOL.lower()}@trade")

# ───────────────────────────────────────────────
# Kafka общие параметры
# ───────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", f"{SYMBOL.lower()}-trades")

# Kafka TLS + SCRAM
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
# Интервал телеметрии
# ───────────────────────────────────────────────
TELEMETRY_INTERVAL: int = int(os.getenv("TELEMETRY_INTERVAL", "10"))

# ───────────────────────────────────────────────
# Отладка
# ───────────────────────────────────────────────
DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

# ───────────────────────────────────────────────
# SSL context helper (если вдруг используем client cert)
# ───────────────────────────────────────────────
def get_ssl_context() -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=CA_PATH)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_REQUIRED
    return context