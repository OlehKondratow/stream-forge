# app/telemetry.py

import json
import time
import asyncio
from loguru import logger
from aiokafka import AIOKafkaProducer
from app import config

class TelemetryProducer:
    def __init__(self):
        self.producer = None
        self.last_send_time = 0
        self.queue_id = config.QUEUE_ID
        self.symbol = config.SYMBOL
        self.topic = config.TELEMETRY_TOPIC

    async def start(self):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            sasl_mechanism="SCRAM-SHA-512",
            security_protocol="SASL_SSL",
            sasl_plain_username=config.KAFKA_USER_PRODUCER,
            sasl_plain_password=config.KAFKA_PASSWORD_PRODUCER,
            ssl_context=config.get_ssl_context()
        )
        await self.producer.start()
        logger.info("✅ Telemetry Kafka producer запущен")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("🛑 Telemetry Kafka producer остановлен")

    async def send_status_update(self, status: str, message: str = "", finished: bool = False):
        event = {
            "event": "status",
            "queue_id": self.queue_id,
            "symbol": self.symbol,
            "type": config.TYPE,
            "status": status,
            "message": message,
            "finished": finished,
            "sent_at": time.time(),
        }
        await self._send(event)

    async def send_progress(self, message_count: int):
        now = time.time()
        if now - self.last_send_time < config.TELEMETRY_INTERVAL:
            return  # avoid spamming
        self.last_send_time = now

        event = {
            "event": "progress",
            "queue_id": self.queue_id,
            "symbol": self.symbol,
            "type": config.TYPE,
            "message_count": message_count,
            "sent_at": now,
        }
        await self._send(event)

    async def _send(self, data: dict):
        try:
            encoded = json.dumps(data).encode("utf-8")
            await self.producer.send_and_wait(self.topic, encoded)
            logger.debug(f"📡 Отправлена телеметрия: {data}")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке телеметрии: {e}")

async def close_telemetry(tp: TelemetryProducer):
    try:
        await tp.stop()
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при закрытии TelemetryProducer: {e}")