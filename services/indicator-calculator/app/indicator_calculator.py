import asyncio
import json
import time
from datetime import datetime, timezone, timedelta
from collections import deque, defaultdict
from loguru import logger
import numpy as np
import pandas as pd
import pandas_ta as ta
from scipy.stats import zscore
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

def vwap_candle(high, low, close, volume):
    tp = (high + low + close) / 3.0
    cum_pv = (tp * volume).cumsum()
    cum_v = volume.cumsum()
    return cum_pv / cum_v

def floor_ts(ts_ms: int, rule: str) -> int:
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
    if rule.endswith("m"):
        step = int(rule[:-1])
        base = dt.replace(second=0, microsecond=0)
        minute = (base.minute // step) * step
        base = base.replace(minute=minute)
        return int(base.timestamp() * 1000)
    raise ValueError(f"Unsupported rule: {rule}")

class CandleAgg:
    def __init__(self, interval_rule: str, window_size: int):
        self.rule = interval_rule
        self.window_size = window_size
        self.candles = deque(maxlen=window_size)
        self._buckets = defaultdict(lambda: {
            "open": None, "high": -np.inf, "low": np.inf, "close": None,
            "volume": 0.0, "quote_volume": 0.0,
            "first_ts": None, "last_ts": None,
            "price_volume_distribution": defaultdict(float)
        })
        self._tob_cache = {}

    def on_trade(self, ts_ms: int, price: float, qty: float):
        key = floor_ts(ts_ms, self.rule)
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
        step = config.VOLUME_PROFILE_PRICE_STEP
        rounded_price = round(round(price / step) * step, 8)
        b["price_volume_distribution"][rounded_price] += qty

    def on_tob(self, ts_ms: int, best_bid: float, best_ask: float):
        key = floor_ts(ts_ms, self.rule)
        self._tob_cache[key] = (best_bid, best_ask)
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
        open_val = b["open"] if b["open"] is not None and np.isfinite(b["open"]) else 0.0
        high_val = b["high"] if b["high"] is not None and np.isfinite(b["high"]) else open_val
        low_val = b["low"] if b["low"] is not None and np.isfinite(b["low"]) else open_val
        close_val = b["close"] if b["close"] is not None and np.isfinite(b["close"]) else open_val
        volume_val = b["volume"] if b["volume"] is not None and np.isfinite(b["volume"]) else 0.0
        quote_volume_val = b["quote_volume"] if b["quote_volume"] is not None and np.isfinite(b["quote_volume"]) else 0.0
        if not np.isfinite(high_val) or high_val < low_val:
            high_val = open_val
        if not np.isfinite(low_val) or low_val > high_val:
            low_val = open_val
        if high_val < low_val:
            high_val = low_val = open_val
        first_ts_val = b["first_ts"] if b["first_ts"] is not None else key
        last_ts_val = b["last_ts"] if b["last_ts"] is not None else key
        return {
            "ts": key, "open": open_val, "high": high_val, "low": low_val, "close": close_val,
            "volume": volume_val, "quote_volume": quote_volume_val, "first_ts": first_ts_val,
            "last_ts": last_ts_val,
            "price_volume_distribution": dict(b["price_volume_distribution"]) if b["price_volume_distribution"] else None
        }

    def flush_ready(self, now_ms: int):
        current_key = floor_ts(now_ms, self.rule)
        ready_keys = sorted([k for k in self._buckets if k < current_key])
        out = []
        for k in ready_keys:
            c = self._finalize_bucket(k)
            if c:
                self.candles.append(c)
                out.append(c)
        return out

def compute_indicators(candles_deque: deque, indicators_config: list, rl_lookback: int):
    df = pd.DataFrame(list(candles_deque))
    if df.empty or len(df) < 2:
        return {}, {}
    df = df.sort_values("ts")
    for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    close, high, low, vol = df["close"], df["high"], df["low"], df.get("volume", pd.Series(np.nan, index=df.index))
    
    indicators = {}
    rl_state = {}

    # Prepare base RL features (normalized OHLCV)
    if len(df) >= rl_lookback:
        last_n_candles = df.iloc[-rl_lookback:]
        last_close = last_n_candles['close'].iloc[-1]
        for col in ['open', 'high', 'low', 'close', 'volume']:
            # Normalize by last close price, volume by its own mean
            norm_series = last_n_candles[col] / last_close if col != 'volume' else last_n_candles[col] / (last_n_candles[col].mean() + 1e-9)
            rl_state[f'{col}_norm_hist'] = norm_series.tolist()

    for indicator_config in indicators_config:
        if not indicator_config.get("enabled", False):
            continue
        
        name = indicator_config["name"].lower()
        params = indicator_config.get("params", {})
        full_name = f"{name}_{'_'.join(map(str, params.values()))}"
        needs_normalization = indicator_config.get("normalize", False)

        try:
            indicator_func = getattr(ta, name)
            indicator_result = indicator_func(close=close, high=high, low=low, volume=vol, **params)

            if indicator_result is None:
                continue

            if isinstance(indicator_result, pd.DataFrame):
                for col in indicator_result.columns:
                    series = indicator_result[col].dropna()
                    if not series.empty:
                        indicators[col] = series.iloc[-1]
                        if len(series) >= rl_lookback:
                            history = series.iloc[-rl_lookback:]
                            if needs_normalization:
                                history = pd.Series(zscore(history), index=history.index)
                            rl_state[f'{col}_hist'] = history.tolist()
            elif isinstance(indicator_result, pd.Series):
                series = indicator_result.dropna()
                if not series.empty:
                    indicators[full_name] = series.iloc[-1]
                    if len(series) >= rl_lookback:
                        history = series.iloc[-rl_lookback:]
                        if needs_normalization:
                            history = pd.Series(zscore(history), index=history.index)
                        rl_state[f'{full_name}_hist'] = history.tolist()
            
            indicators_calculated_total.labels(name).inc()

        except Exception as e:
            logger.error(f"❌ Error calculating indicator {name}: {e}")
            errors_total.inc()
            
    # Clean up NaN/inf values
    for key, value in indicators.items():
        if isinstance(value, dict):
            indicators[key] = {k: (None if pd.isna(v) else v) for k, v in value.items()}
        elif pd.isna(value):
            indicators[key] = None

    return indicators, rl_state

def get_message_timestamp(msg_value: dict, topic: str) -> int:
    try:
        if topic == config.KAFKA_TOPIC_TRADES:
            return int(msg_value["trade_time"])
        elif topic == config.KAFKA_TOPIC_ORDERBOOK:
            return int(msg_value["timestamp"])
    except (KeyError, TypeError) as e:
        logger.warning(f"Could not extract timestamp from message on topic {topic}: {e}. Message: {msg_value}")
    return 0

class IndicatorCalculator:
    def __init__(self, telemetry: TelemetryProducer):
        self.telemetry = telemetry
        self.symbol = config.SYMBOL
        self.db_collection_name = config.DB_COLLECTION
        self.indicators_config = config.INDICATORS_CONFIG
        self.arango_db = None
        self.arango_collection = None
        self.aggregator = CandleAgg(config.CANDLE_INTERVAL, config.CANDLE_WINDOW_SIZE)
        self.last_flush_time = 0
        self.flush_interval = 1
        self.last_saved_ts = 0
        self.kafka_consumer = None

    async def _connect_arango(self):
        client = ArangoClient(hosts=config.ARANGO_URL)
        self.arango_db = client.db(config.ARANGO_DB, username=config.ARANGO_USER, password=config.ARANGO_PASSWORD)
        if not self.arango_db.has_collection(self.db_collection_name):
            self.arango_collection = self.arango_db.create_collection(self.db_collection_name)
        else:
            self.arango_collection = self.arango_db.collection(self.db_collection_name)
        logger.info(f"Connected to ArangoDB collection: {self.db_collection_name}")

    async def _process_kafka_message(self, msg):
        message = json.loads(msg.value.decode("utf-8"))
        if msg.topic == config.KAFKA_TOPIC_TRADES:
            self.aggregator.on_trade(int(message["trade_time"]), float(message["price"]), float(message["quantity"]))
        elif msg.topic == config.KAFKA_TOPIC_ORDERBOOK:
            if "best_bid" in message and "best_ask" in message:
                self.aggregator.on_tob(int(message["timestamp"]), float(message["best_bid"]), float(message["best_ask"]))
            elif "bids" in message and "asks" in message and message["bids"] and message["asks"]:
                self.aggregator.on_tob(int(message["timestamp"]), float(message["bids"][0][0]), float(message["asks"][0][0]))
        current_time = time.time()
        if current_time - self.last_flush_time >= self.flush_interval:
            self.last_flush_time = current_time
            await self._flush_and_calculate()

    async def _flush_and_calculate(self):
        now_ms = int(datetime.now(tz=timezone.utc).timestamp()*1000)
        self.aggregator.flush_ready(now_ms)
        if not self.aggregator.candles:
            return

        last_candle = self.aggregator.candles[-1]
        last_candle_ts = last_candle["ts"]
        if last_candle_ts == self.last_saved_ts:
            return

        indicators, rl_state = compute_indicators(self.aggregator.candles, self.indicators_config, config.RL_LOOKBACK_PERIOD)

        document = {
            "_key": f"{self.symbol}_{last_candle_ts}",
            "symbol": self.symbol,
            "timestamp": last_candle_ts,
            "candle": {"open": last_candle["open"], "high": last_candle["high"], "low": last_candle["low"], "close": last_candle["close"], "volume": last_candle["volume"], "quote_volume": last_candle["quote_volume"]},
            "indicators": indicators,
            "rl_state": rl_state,
            "volume_profile": last_candle.get("price_volume_distribution"),
            "metadata": {"source": "kafka_trades_tob", "processed_at": datetime.now(timezone.utc).isoformat()}
        }

        try:
            self.arango_collection.insert(document, overwrite=True)
            documents_saved_total.inc()
            logger.info(f"✅ Документ сохранен в ArangoDB: {last_candle_ts}")
            await self.telemetry.send_status_update("processing", f"Saved document {document['_key']}")
            self.last_saved_ts = last_candle_ts
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения документа в ArangoDB: {e}")
            errors_total.inc()
            await self.telemetry.send_status_update("error", f"Failed to save document: {e}")

    async def start(self):
        await self._connect_arango()
        group_id = f"{config.QUEUE_ID}-consumer-{int(time.time())}"
        self.kafka_consumer = AIOKafkaConsumer(
            *config.KAFKA_TOPICS_LIST, bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS, group_id=group_id,
            enable_auto_commit=False, sasl_mechanism="SCRAM-SHA-512", security_protocol="SASL_SSL",
            sasl_plain_username=config.KAFKA_USER_CONSUMER, sasl_plain_password=config.KAFKA_PASSWORD_CONSUMER,
            ssl_context=config.get_ssl_context(), auto_offset_reset="earliest"
        )
        await self.kafka_consumer.start()
        logger.info(f"Kafka consumer connected to topics: {config.KAFKA_TOPICS_LIST} with group_id: {group_id}")

        first_timestamps = {topic: None for topic in config.KAFKA_TOPICS_LIST}
        watermark, synchronizing, message_buffer = 0, True, []
        logger.info("Synchronizing streams to find watermark...")
        
        try:
            async for msg in self.kafka_consumer:
                if synchronizing:
                    message_buffer.append(msg)
                    topic = msg.topic
                    if first_timestamps[topic] is None:
                        try:
                            ts = get_message_timestamp(json.loads(msg.value.decode('utf-8')), topic)
                            if ts > 0:
                                first_timestamps[topic] = ts
                                logger.info(f"Got first message from topic {topic} with timestamp {datetime.fromtimestamp(ts/1000, tz=timezone.utc)}")
                        except (json.JSONDecodeError, KeyError): continue

                    if all(first_timestamps.values()):
                        watermark = max(v for v in first_timestamps.values() if v is not None)
                        logger.info(f"🚀 Watermark established: {datetime.fromtimestamp(watermark/1000, tz=timezone.utc)}")
                        synchronizing = False
                        message_buffer.sort(key=lambda m: get_message_timestamp(json.loads(m.value.decode('utf-8')), m.topic))
                        for buffered_msg in message_buffer:
                            try:
                                if get_message_timestamp(json.loads(buffered_msg.value.decode('utf-8')), buffered_msg.topic) >= watermark:
                                    await self._process_kafka_message(buffered_msg)
                            except (json.JSONDecodeError, KeyError): continue
                        message_buffer = []
                        await self.kafka_consumer.commit()
                        logger.info("Initial buffer processed, switching to real-time.")
                else:
                    if not msg.value:
                        await self.kafka_consumer.commit()
                        continue
                    try:
                        if get_message_timestamp(json.loads(msg.value.decode('utf-8')), msg.topic) < watermark:
                            await self.kafka_consumer.commit()
                            continue
                        await self._process_kafka_message(msg)
                        await self.kafka_consumer.commit()
                    except (json.JSONDecodeError, KeyError):
                        await self.kafka_consumer.commit()
                    except Exception as e:
                        logger.error(f"Error processing Kafka message: {e}", exc_info=True)
                        errors_total.inc()
        finally:
            if self.kafka_consumer:
                await self.kafka_consumer.stop()
                logger.info("Kafka consumer stopped.")