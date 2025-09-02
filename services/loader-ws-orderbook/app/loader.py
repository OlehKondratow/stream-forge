# app/loader.py

import asyncio
import json
import websockets
from loguru import logger
from datetime import datetime

from app import config
from app.kafka_client import KafkaProducerClient
from app.telemetry import TelemetryProducer


async def run_loader(stop_event: asyncio.Event, telemetry: TelemetryProducer):
    """
    Подключается к Binance WebSocket, получает данные и отправляет их в Kafka.
    """
    uri = f"{config.BINANCE_WS_URL}/{config.SYMBOL}@depth"
    logger.info(f"🔌 Подключение к Binance WebSocket: {uri}")
    producer = KafkaProducerClient()
    await producer.start()

    message_count = 0
    telemetry_counter = 0
    telemetry_interval = config.TELEMETRY_INTERVAL

    try:
        async with websockets.connect(uri) as ws:
            logger.info("✅ WebSocket подключен успешно.")

            while not stop_event.is_set():
                try:
                    message = await asyncio.wait_for(ws.recv(), timeout=10)
                    data = json.loads(message)

                    parsed = {
                        "symbol": config.SYMBOL,
                        "timestamp": data.get("E"),
                        "bids": data.get("b", []),
                        "asks": data.get("a", []),
                        "event_time": datetime.utcnow().isoformat(),
                        "queue_id": config.QUEUE_ID,
                        "type": config.TYPE,
                        "message_count": message_count
                    }

                    await producer.send_json(config.KAFKA_TOPIC, parsed)
                    message_count += 1
                    telemetry_counter += 1

                    if telemetry_counter >= telemetry_interval:
                        await telemetry.send_progress(message_count)
                        telemetry_counter = 0

                except asyncio.TimeoutError:
                    logger.warning("⏳ WebSocket таймаут ожидания данных.")
                except websockets.ConnectionClosed:
                    logger.warning("🔁 WebSocket соединение закрыто. Переподключение...")
                    break
                except Exception as e:
                    logger.error(f"❌ Ошибка обработки сообщения: {e}")

    except Exception as e:
        logger.exception(f"🚨 Ошибка подключения к WebSocket: {e}")
        await telemetry.send_status_update(status="error", message=str(e), finished=True)

    finally:
        await producer.stop()
        logger.info(f"📦 Всего сообщений отправлено в Kafka: {message_count}")
        await telemetry.send_progress(message_count)
