import asyncio
import json
import time
from datetime import datetime, timezone
from loguru import logger
# import websockets # Removed
import pandas as pd
import pandas_ta as ta
from arango import ArangoClient
from aiokafka import AIOKafkaConsumer

from app import config
from app.telemetry import TelemetryProducer
from app.metrics import indicators_calculated_total, documents_saved_total, errors_total

class IndicatorCalculator:
    def __init__(self, telemetry: TelemetryProducer):
        self.telemetry = telemetry
        self.symbol = config.SYMBOL
        self.db_collection_name = config.DB_COLLECTION
        self.indicators_config = config.INDICATORS_CONFIG
        self.arango_db = None
        self.arango_collection = None
        # self.websocket_uri = f"wss://stream.binance.com:9443/ws/{self.symbol.lower()}@depth" # Removed
        self.data_buffer = [] # Buffer to store order book data for VWAP calculation
        self.buffer_lock = asyncio.Lock()
        self.last_calculation_time = 0
        self.calculation_interval = 5 # Calculate every 5 seconds for now, can be configurable
        self.kafka_consumer = None # New Kafka consumer instance

    async def _connect_arango(self):
        client = ArangoClient(hosts=config.ARANGO_URL)
        self.arango_db = client.db(
            config.ARANGO_DB,
            username=config.ARANGO_USER,
            password=config.ARANGO_PASSWORD,
        )
        if not self.arango_db.has_collection(self.db_collection_name):
            logger.info(f"Collection '{self.db_collection_name}' not found, creating it.")
            self.arango_collection = self.arango_db.create_collection(self.db_collection_name)
        else:
            self.arango_collection = self.arango_db.collection(self.db_collection_name)
        logger.info(f"Connected to ArangoDB collection: {self.db_collection_name}")

    async def _calculate_vwap(self) -> float:
        """Calculates Volume Weighted Average Price (VWAP) from the current order book buffer."""
        async with self.buffer_lock:
            if not self.data_buffer:
                return 0.0

            total_bid_price_volume = 0.0
            total_bid_volume = 0.0
            total_ask_price_volume = 0.0
            total_ask_volume = 0.0

            for entry in self.data_buffer:
                bids = entry.get('b', [])
                asks = entry.get('a', [])

                for bid_price, bid_qty in bids:
                    bid_price = float(bid_price)
                    bid_qty = float(bid_qty)
                    total_bid_price_volume += bid_price * bid_qty
                    total_bid_volume += bid_qty

                for ask_price, ask_qty in asks:
                    ask_price = float(ask_price)
                    ask_qty = float(ask_qty)
                    total_ask_price_volume += ask_price * ask_qty
                    total_ask_volume += ask_qty
            
            # Simple mid-price VWAP for now, can be refined
            if total_bid_volume > 0 and total_ask_volume > 0:
                vwap_bid = total_bid_price_volume / total_bid_volume
                vwap_ask = total_ask_price_volume / total_ask_volume
                return (vwap_bid + vwap_ask) / 2
            elif total_bid_volume > 0:
                return total_bid_price_volume / total_bid_volume
            elif total_ask_volume > 0:
                return total_ask_price_volume / total_ask_volume
            else:
                return 0.0

    async def _process_kafka_message(self, message: dict):
        """Processes a single Kafka message, adding it to the buffer."""
        async with self.buffer_lock:
            self.data_buffer.append(message)
            # Optionally, limit buffer size to avoid excessive memory usage
            # if len(self.data_buffer) > 100:
            #     self.data_buffer.pop(0)

        current_time = time.time()
        if current_time - self.last_calculation_time >= self.calculation_interval:
            self.last_calculation_time = current_time
            await self._perform_calculations_and_save()

    async def _perform_calculations_and_save(self):
        """Calculates indicators and saves to ArangoDB."""
        vwap = await self._calculate_vwap()
        if vwap == 0.0:
            logger.warning("VWAP is 0, skipping indicator calculation.")
            return

        # For simplicity, we'll use VWAP as the 'close' price for indicators
        # In a real scenario, you'd build OHLCV candles or use a series of VWAP values
        # For pandas-ta, we need a pandas Series
        price_series = pd.Series([vwap])

        calculated_indicators = {}
        for indicator_config in self.indicators_config:
            if indicator_config.get("enabled", False):
                name = indicator_config["name"].lower()
                params = indicator_config.get("params", {})
                try:
                    # Dynamically call pandas_ta functions
                    indicator_func = getattr(ta, name)
                    indicator_result = indicator_func(price_series, **params)

                    # pandas_ta returns a Series or DataFrame. Extract the value.
                    if isinstance(indicator_result, pd.Series):
                        value = indicator_result.iloc[-1]
                    elif isinstance(indicator_result, pd.DataFrame):
                        value = indicator_result.iloc[-1].to_dict()
                    else:
                        value = indicator_result # Should not happen often

                    calculated_indicators[f"{name}_{'_'.join(map(str, params.values()))}"] = value
                    indicators_calculated_total.labels(name).inc()
                except Exception as e:
                    logger.error(f"❌ Error calculating indicator {name}: {e}")
                    errors_total.inc()

        if not calculated_indicators:
            logger.info("No indicators calculated or enabled.")
            return

        timestamp_ms = int(time.time() * 1000)
        document = {
            "_key": f"{self.symbol}_{timestamp_ms}",
            "symbol": self.symbol,
            "timestamp": timestamp_ms,
            "indicators": calculated_indicators,
            "metadata": {
                "source": "kafka_orderbook", # Changed source
                "processed_at": datetime.now(timezone.utc).isoformat(),
                "vwap": vwap # Include VWAP for context
            }
        }

        try:
            await self.arango_collection.insert(document)
            documents_saved_total.inc()
            logger.info(f"✅ Документ сохранен в ArangoDB: {document['_key']}")
            await self.telemetry.send_status_update("processing", f"Saved document {document['_key']}")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения документа в ArangoDB: {e}")
            errors_total.inc()
            await self.telemetry.send_status_update("error", f"Failed to save document: {e}")
        
        # Clear buffer after calculation
        async with self.buffer_lock:
            self.data_buffer.clear()

    async def start(self):
        await self._connect_arango()
        logger.info(f"Starting Kafka consumer for topic: {config.KAFKA_TOPIC}")
        self.kafka_consumer = AIOKafkaConsumer(
            config.KAFKA_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{config.QUEUE_ID}-orderbook-consumer", # New consumer group
            enable_auto_commit=True,
            sasl_mechanism="SCRAM-SHA-512",
            security_protocol="SASL_SSL",
            sasl_plain_username=config.KAFKA_USER_CONSUMER,
            sasl_plain_password=config.KAFKA_PASSWORD_CONSUMER,
            ssl_context=config.get_ssl_context(),
            auto_offset_reset="latest"
        )
        await self.kafka_consumer.start()
        logger.info(f"Kafka consumer connected to topic: {config.KAFKA_TOPIC}")

        try:
            async for msg in self.kafka_consumer:
                if not msg.value:
                    logger.debug("🕳️ Received empty message (tombstone), skipping.")
                    continue
                try:
                    data = json.loads(msg.value.decode("utf-8"))
                    await self._process_kafka_message(data)
                except json.JSONDecodeError:
                    logger.error(f"Failed to decode JSON from Kafka message: {msg.value}")
                    errors_total.inc()
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {e}")
                    errors_total.inc()
        finally:
            if self.kafka_consumer:
                await self.kafka_consumer.stop()
                logger.info("Kafka consumer stopped.")