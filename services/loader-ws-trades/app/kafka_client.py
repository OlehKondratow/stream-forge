# app/kafka_client.py

import json
import ssl
from typing import AsyncGenerator
from loguru import logger
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

from app import config

class KafkaProducerClient:
    def __init__(self):
        self.producer = None

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
        logger.info("✅ KafkaProducerClient запущен")

    async def send_json(self, topic: str, data: dict):
        if not self.producer:
            raise RuntimeError("Producer не инициализирован")

        try:
            encoded = json.dumps(data).encode("utf-8")
            await self.producer.send_and_wait(topic, encoded)
            logger.debug(f"📤 Отправлено в Kafka ({topic}): {data}")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки в Kafka: {e}")

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("🛑 KafkaProducerClient остановлен")


class KafkaControlListener:
    def __init__(self, queue_id: str):
        self.queue_id = queue_id
        self.consumer = None

    async def start(self):
        self.consumer = AIOKafkaConsumer(
            config.CONTROL_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{self.queue_id}-control-group",
            enable_auto_commit=True,
            sasl_mechanism="SCRAM-SHA-512",
            security_protocol="SASL_SSL",
            sasl_plain_username=config.KAFKA_USER_CONSUMER,
            sasl_plain_password=config.KAFKA_PASSWORD_CONSUMER,
            ssl_context=config.get_ssl_context(),
            auto_offset_reset="latest"
        )
        await self.consumer.start()
        logger.info("🎧 KafkaControlListener запущен")

    async def listen(self) -> AsyncGenerator[dict, None]:
        if not self.consumer:
            logger.error("Consumer не инициализирован")
            return

        try:
            async for msg in self.consumer:
                # Проверяем, что сообщение не пустое (tombstone)
                if not msg.value:
                    logger.debug("🕳️ Получено пустое сообщение (tombstone), пропускаем.")
                    continue

                try:
                    data = json.loads(msg.value.decode("utf-8"))
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ Не удалось декодировать JSON из сообщения Kafka: {msg.value}")
                    continue # Пропускаем некорректное сообщение

                # Фильтруем сообщения по queue_id
                if data.get("queue_id") == self.queue_id:
                    logger.info(f"📩 Получена команда для своей очереди: {data}")
                    yield data
                else:
                    # Это нормальная ситуация, просто логируем в debug
                    logger.debug(f"🔕 Пропущена команда для другой очереди: {data.get('queue_id')}")

        except Exception as e:
            logger.error(f"❌ Критическая ошибка в KafkaControlListener: {e}", exc_info=True)
            # В зависимости от стратегии, можно переподключиться или остановить приложение
            raise

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()
            logger.info("🛑 KafkaControlListener остановлен")
