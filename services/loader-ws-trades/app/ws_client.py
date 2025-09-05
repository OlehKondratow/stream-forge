import asyncio
import websockets
import json
from loguru import logger
from app import config

class BinanceWSClient:
    def __init__(self, symbol: str, stream_type: str):
        self.symbol = symbol.lower()
        self.stream_type = stream_type # e.g., "trade", "depth", "diffDepth"
        self.uri = self._build_uri()
        logger.info(f"Binance WebSocket Client initialized for URI: {self.uri}")

    def _build_uri(self):
        # For trades, stream type is typically "trade"
        stream = f"{self.symbol}@{self.stream_type}"
        return f"{config.BINANCE_WS_URL}/{stream}"

    async def connect(self):
        """Connects to the WebSocket and yields received messages."""
        logger.info(f"Connecting to WebSocket: {self.uri}")
        try:
            async with websockets.connect(self.uri) as websocket:
                logger.info(f"WebSocket connection established for {self.symbol} {self.stream_type}.")
                while True:
                    try:
                        message = await websocket.recv()
                        data = json.loads(message)
                        yield data
                    except websockets.exceptions.ConnectionClosedOK:
                        logger.info("WebSocket connection closed gracefully.")
                        break
                    except websockets.exceptions.ConnectionClosedError as e:
                        logger.error(f"WebSocket connection closed with error: {e}")
                        break
                    except json.JSONDecodeError:
                        logger.error(f"Failed to decode JSON from WebSocket message: {message}")
                    except Exception as e:
                        logger.error(f"Error receiving WebSocket message: {e}")
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket {self.uri}: {e}")
            # Re-raise or handle reconnection logic in the loader
            raise