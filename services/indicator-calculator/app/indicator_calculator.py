import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict
from loguru import logger
import numpy as np # Added
import pandas as pd
import pandas_ta as ta
from arango import ArangoClient
from aiokafka import AIOKafkaConsumer

from app import config
from app.telemetry import TelemetryProducer
from app.metrics import indicators_calculated_total, documents_saved_total, errors_total

# --- Custom indicator functions from user's example ---
def ema(series, span):
    return series.ewm(span=span, adjust=False).mean()

def rsi(series, period=14):
    delta = series.diff()
    up = np.where(delta > 0, delta, 0.0)
    down = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(up, index=series.index).rolling(period).mean()
    roll_down = pd.Series(down, index=series.index).rolling(period).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi

def atr(high, low, close, period=14):
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def bollinger(close, period=20, num_std=2):
    ma = close.rolling(period).mean()
    std = close.rolling(period).std(ddof=0)
    upper = ma + num_std * std
    lower = ma - num_std * std
    return ma, upper, lower

def vwap_candle(high, low, close, volume): # Renamed to avoid conflict with _calculate_vwap
    tp = (high + low + close) / 3.0
    cum_pv = (tp * volume).cumsum()
    cum_v = volume.cumsum()
    return cum_pv / cum_v

def floor_ts(ts_ms: int, rule: str) -> int:
    """Округление timestamp (мс) вниз к началу интервала."""
    dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
    if rule.endswith("ms"):
        step = int(rule[:-2])
        base_ms = (ts_ms // step) * step
        return base_ms
    if rule.endswith("s"):
        step = int(rule[:-1])
        base = dt.replace(microsecond=0)
        base -= timedelta(seconds=base.second % step)
        return int(base.timestamp() * 1000)
    if rule in ("1min","1m"):
        base = dt.replace(second=0, microsecond=0)
        return int(base.timestamp() * 1000)
    # простая поддержка 5m, 15m и т.д.
    if rule.endswith("m"):
        step = int(rule[:-1])
        base = dt.replace(second=0, microsecond=0)
        minute = (base.minute // step) * step
        base = base.replace(minute=minute)
        return int(base.timestamp() * 1000)
    raise ValueError(f"Unsupported rule: {rule}")

class CandleAgg:
    """Агрегирует входящие события в OHLCV по времени."""
    def __init__(self, interval_rule: str, window_size: int):
        self.rule = interval_rule
        self.window_size = window_size
        self.candles = deque(maxlen=window_size)  # хранит готовые свечи (dict)
        self._buckets = defaultdict(lambda: {
            "open": None, "high": -np.inf, "low": np.inf, "close": None,
            "volume": 0.0, "quote_volume": 0.0,
            "first_ts": None, "last_ts": None,
            "price_volume_distribution": defaultdict(float) # New field
        })
        self._tob_cache = {}  # последний top-of-book по бакету

    def on_trade(self, ts_ms: int, price: float, qty: float):
        key = floor_ts(ts_ms, self.rule)
        logger.debug(f"OnTrade: ts_ms={ts_ms}, key={key}")
        b = self._buckets[key]
        if b["open"] is None:
            b["open"] = price
            b["first_ts"] = ts_ms
        b["high"] = max(b["high"], price)
        b["low"] = min(b["low"], price)
        b["close"] = price
        b["volume"] += qty
        b["quote_volume"] += qty * price
        b["last_ts"] = ts_ms
        
        # Accumulate volume for price levels
        # Round price to the nearest VOLUME_PROFILE_PRICE_STEP
        rounded_price = round(price / config.VOLUME_PROFILE_PRICE_STEP) * config.VOLUME_PROFILE_PRICE_STEP
        b["price_volume_distribution"][rounded_price] += qty

    def on_tob(self, ts_ms: int, best_bid: float, best_ask: float):
        key = floor_ts(ts_ms, self.rule)
        logger.debug(f"OnTob: ts_ms={ts_ms}, key={key}")
        self._tob_cache[key] = (best_bid, best_ask)
        # если нет трейдов в этом бакете — можно заполнять суррогат по mid
        b = self._buckets[key]
        if b["open"] is None:
            mid = (best_bid + best_ask) / 2.0
            b["open"] = b["high"] = b["low"] = b["close"] = mid
            b["first_ts"] = ts_ms
            b["last_ts"] = ts_ms

    def _finalize_bucket(self, key: int):
        b = self._buckets.pop(key, None)
        if not b:
            return None

        # Ensure all numeric fields are actual numbers, not inf or None
        open_val = b["open"] if b["open"] is not None and np.isfinite(b["open"]) else 0.0
        high_val = b["high"] if b["high"] is not None and np.isfinite(b["high"]) else open_val
        low_val = b["low"] if b["low"] is not None and np.isfinite(b["low"]) else open_val
        close_val = b["close"] if b["close"] is not None and np.isfinite(b["close"]) else open_val
        volume_val = b["volume"] if b["volume"] is not None and np.isfinite(b["volume"]) else 0.0
        quote_volume_val = b["quote_volume"] if b["quote_volume"] is not None and np.isfinite(b["quote_volume"]) else 0.0

        # If high/low are still inf/-inf after initial processing, set them to open_val
        if not np.isfinite(high_val) or high_val < low_val: # Check for inf and also if high < low
            high_val = open_val
        if not np.isfinite(low_val) or low_val > high_val: # Check for inf and also if low > high
            low_val = open_val
        
        # Final check to ensure high >= low
        if high_val < low_val:
            high_val = low_val = open_val # Fallback if something went wrong

        # Handle first_ts and last_ts which can be None if no trades/TOB
        first_ts_val = b["first_ts"] if b["first_ts"] is not None else key
        last_ts_val = b["last_ts"] if b["last_ts"] is not None else key

        logger.debug(f"Finalizing bucket for key={key}")
        logger.debug(f"  open={open_val}, high={high_val}, low={low_val}, close={close_val}")
        logger.debug(f"  volume={volume_val}, quote_volume={quote_volume_val}")
        logger.debug(f"  first_ts={first_ts_val}, last_ts={last_ts_val}")
        logger.debug(f"  price_volume_distribution={dict(b['price_volume_distribution'])}")
        return {
            "ts": key,
            "open": open_val,
            "high": high_val,
            "low": low_val,
            "close": close_val,
            "volume": volume_val,
            "quote_volume": quote_volume_val,
            "first_ts": first_ts_val,
            "last_ts": last_ts_val,
            "price_volume_distribution": dict(b["price_volume_distribution"]) if b["price_volume_distribution"] else None
        }

    def flush_ready(self, now_ms: int):
        """Достаём завершённые свечи (все бакеты, чьё окно полностью закончилось)."""
        # бакет текущего интервала ещё НЕ закрыт
        current_key = floor_ts(now_ms, self.rule)
        logger.debug(f"FlushReady: now_ms={now_ms}, rule={self.rule}, current_key={current_key}")
        logger.debug(f"FlushReady: _buckets keys={list(self._buckets.keys())}")
        ready_keys = [k for k in list(self._buckets.keys()) if k < current_key]
        ready_keys.sort()
        logger.debug(f"FlushReady: ready_keys={ready_keys}")
        out = []
        for k in ready_keys:
            c = self._finalize_bucket(k)
            if c:
                self.candles.append(c)
                out.append(c)
        return out

def compute_indicators(candles_deque: deque, indicators_config: list): # Modified to accept indicators_config
    """candles -> pandas DF -> индикаторы по последним 40."""
    df = pd.DataFrame(list(candles_deque))
    if df.empty:
        return None
    df = df.sort_values("ts")
    
    # Ensure numeric types
    for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    vol   = df.get("volume", pd.Series(np.nan, index=df.index))

    calculated_indicators = {}
    for indicator_config in indicators_config:
        if indicator_config.get("enabled", False):
            name = indicator_config["name"].lower()
            params = indicator_config.get("params", {})
            try:
                # Dynamically call custom indicator functions or pandas_ta
                if name == "ema":
                    indicator_result = ema(close, **params)
                elif name == "rsi":
                    indicator_result = rsi(close, **params)
                elif name == "atr":
                    indicator_result = atr(high, low, close, **params)
                elif name == "bollinger": # Assuming bollinger is BBANDS
                    ma, upper, lower = bollinger(close, **params)
                    calculated_indicators[f"bb_ma{params.get('period',20)}"] = ma.iloc[-1]
                    calculated_indicators[f"bb_up{params.get('period',20)}"] = upper.iloc[-1]
                    calculated_indicators[f"bb_lo{params.get('period',20)}"] = lower.iloc[-1]
                    continue # Skip common processing for bollinger
                elif name == "vwap": # Assuming vwap is VWAP
                    indicator_result = vwap_candle(high, low, close, vol.fillna(0))
                else:
                    # Try pandas_ta for other indicators
                    indicator_func = getattr(ta, name)
                    indicator_result = indicator_func(close, **params) # Most TA indicators use close price

                # Extract the last value from Series or DataFrame
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
    return calculated_indicators


class IndicatorCalculator:
    def __init__(self, telemetry: TelemetryProducer):
        self.telemetry = telemetry
        self.symbol = config.SYMBOL
        self.db_collection_name = config.DB_COLLECTION
        self.indicators_config = config.INDICATORS_CONFIG
        self.arango_db = None
        self.arango_collection = None
        self.aggregator = CandleAgg(config.CANDLE_INTERVAL, config.CANDLE_WINDOW_SIZE) # New aggregator
        self.last_flush_time = 0
        self.flush_interval = 1 # Flush every 1 second, can be configurable
        self.last_saved_ts = 0

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

    async def _process_kafka_message(self, message: dict):
        """Processes a single Kafka message, feeding it to the aggregator."""
        logger.debug(f"Received Kafka message: {message}")
        # Assuming message is a trade or TOB update
        if "price" in message and "qty" in message: # It's a trade
            self.aggregator.on_trade(int(message["timestamp"]), float(message["price"]), float(message["qty"]))
            logger.debug(f"Processed trade: {message.get('timestamp')}")
        elif "best_bid" in message and "best_ask" in message: # It's TOB
            self.aggregator.on_tob(int(message["timestamp"]), float(message["best_bid"]), float(message["best_ask"]))
            logger.debug(f"Processed TOB: {message.get('timestamp')}")
        elif "bids" in message and "asks" in message: # It's an order book update
            if message["bids"] and message["asks"]:
                best_bid = float(message["bids"][0][0])
                best_ask = float(message["asks"][0][0])
                self.aggregator.on_tob(int(message["timestamp"]), best_bid, best_ask)
                logger.debug(f"Processed Order Book TOB: {message.get('timestamp')}")
            else:
                logger.debug(f"Received empty bids or asks in order book update: {message}")
        else:
            logger.warning(f"Unknown message format: {message}")
            errors_total.inc()

        current_time = time.time()
        if current_time - self.last_flush_time >= self.flush_interval:
            self.last_flush_time = current_time
            await self._flush_and_calculate()

    async def _flush_and_calculate(self):
        """Flushes ready candles, calculates indicators, and saves to ArangoDB."""
        now_ms = int(datetime.now(tz=timezone.utc).timestamp()*1000)
        logger.debug(f"Flush: now_ms={now_ms}")
        ready_candles = self.aggregator.flush_ready(now_ms)
        
        if not ready_candles:
            logger.debug("No new candles to process.")
            return

        last_candle_ts = self.aggregator.candles[-1]["ts"] if self.aggregator.candles else None

        if last_candle_ts is None or last_candle_ts == self.last_saved_ts:
            logger.debug(f"Skipping save: no new candles or candle with timestamp {last_candle_ts} already processed.")
            return

        # The compute_indicators function now works on the deque directly
        # and returns the calculated indicators for the last window
        calculated_indicators_dict = compute_indicators(self.aggregator.candles, self.indicators_config)

        if not calculated_indicators_dict:
            logger.info("No indicators calculated or enabled for the current window.")
            return

        # Use the timestamp of the last candle in the window for the document
        
        document = {
            "_key": f"{self.symbol}_{last_candle_ts}",
            "symbol": self.symbol,
            "timestamp": last_candle_ts,
            "indicators": calculated_indicators_dict,
            "metadata": {
                "source": "kafka_trades_tob", # Updated source
                "processed_at": datetime.now(timezone.utc).isoformat(),
                # VWAP is now part of indicators if calculated
            }
        }

        try:
            self.arango_collection.insert(document)
            documents_saved_total.inc()
            logger.info(f"✅ Документ сохранен в ArangoDB: {document['_key']}")
            await self.telemetry.send_status_update("processing", f"Saved document {document['_key']}")
            self.last_saved_ts = last_candle_ts # Update last saved timestamp
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения документа в ArangoDB: {e}")
            errors_total.inc()
            await self.telemetry.send_status_update("error", f"Failed to save document: {e}")
        
        # No need to clear data_buffer here, CandleAgg manages its own deque

    async def start(self):
        await self._connect_arango()
        logger.info(f"Starting Kafka consumer for topic: {config.KAFKA_TOPIC}")
        self.kafka_consumer = AIOKafkaConsumer(
            config.KAFKA_TOPIC, # Assuming this topic contains both trades and TOB, or just trades
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{config.QUEUE_ID}-orderbook-consumer",
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