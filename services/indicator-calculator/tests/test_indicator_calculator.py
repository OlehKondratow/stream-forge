import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
import pandas as pd
import pandas_ta as ta

# Mock config and telemetry for testing
class MockConfig:
    SYMBOL = "TESTUSDT"
    DB_COLLECTION = "test_indicators"
    INDICATORS_CONFIG = [
        {"name": "RSI", "enabled": True, "params": {"length": 3}},
        {"name": "MACD", "enabled": True, "params": {"fast": 2, "slow": 3, "signal": 1}},
    ]
    ARANGO_URL = "http://mock-arango:8529"
    ARANGO_DB = "mock_db"
    ARANGO_USER = "mock_user"
    ARANGO_PASSWORD = "mock_password"

class MockTelemetryProducer:
    async def start(self):
        pass
    async def stop(self):
        pass
    async def send_status_update(self, *args, **kwargs):
        pass
    async def send_progress(self, *args, **kwargs):
        pass

# Patch config and telemetry before importing the module under test
# This needs to be done carefully to avoid issues with module imports
# For simplicity in this example, we'll assume direct patching or
# that the module is imported within the test function after patching.
# In a real project, dependency injection or more sophisticated mocking
# strategies are preferred.

# Temporarily modify sys.modules for mocking
import sys
sys.modules['app.config'] = MockConfig
sys.modules['app.telemetry'] = MagicMock(TelemetryProducer=MockTelemetryProducer)
sys.modules['app.metrics'] = MagicMock(
    indicators_calculated_total=MagicMock(labels=MagicMock(return_value=MagicMock(inc=MagicMock()))),
    documents_saved_total=MagicMock(inc=MagicMock()),
    errors_total=MagicMock(inc=MagicMock())
)

from app.indicator_calculator import IndicatorCalculator

@pytest.fixture
def mock_indicator_calculator():
    telemetry = MockTelemetryProducer()
    calculator = IndicatorCalculator(telemetry)
    # Mock ArangoDB connection
    calculator.arango_db = MagicMock()
    calculator.arango_collection = MagicMock()
    calculator.arango_db.has_collection.return_value = True
    calculator.arango_db.collection.return_value = calculator.arango_collection
    return calculator

@pytest.mark.asyncio
async def test_calculate_vwap_empty_buffer(mock_indicator_calculator):
    mock_indicator_calculator.data_buffer = []
    vwap = await mock_indicator_calculator._calculate_vwap()
    assert vwap == 0.0

@pytest.mark.asyncio
async def test_calculate_vwap_with_data(mock_indicator_calculator):
    mock_indicator_calculator.data_buffer = [
        {"b": [["100", "10"]], "a": [["101", "5"]]},
        {"b": [["99", "20"]], "a": [["102", "10"]]},
    ]
    # Expected VWAP calculation:
    # Bid: (100*10 + 99*20) / (10+20) = (1000 + 1980) / 30 = 2980 / 30 = 99.333
    # Ask: (101*5 + 102*10) / (5+10) = (505 + 1020) / 15 = 1525 / 15 = 101.666
    # Mid: (99.333 + 101.666) / 2 = 100.4995
    vwap = await mock_indicator_calculator._calculate_vwap()
    assert vwap == pytest.approx(100.4995, abs=1e-3)

@pytest.mark.asyncio
async def test_perform_calculations_and_save(mock_indicator_calculator):
    mock_indicator_calculator.data_buffer = [
        {"b": [["100", "10"]], "a": [["101", "5"]]},
    ]
    mock_indicator_calculator.arango_collection.insert = AsyncMock()
    mock_indicator_calculator.telemetry.send_status_update = AsyncMock()

    await mock_indicator_calculator._perform_calculations_and_save()

    mock_indicator_calculator.arango_collection.insert.assert_called_once()
    mock_indicator_calculator.telemetry.send_status_update.assert_called_once()
    assert mock_indicator_calculator.data_buffer == [] # Buffer should be cleared

@pytest.mark.asyncio
async def test_process_websocket_message(mock_indicator_calculator):
    mock_indicator_calculator._perform_calculations_and_save = AsyncMock()
    mock_indicator_calculator.calculation_interval = -1 # Trigger immediate calculation

    message = {"e": "depthUpdate", "b": [["100", "10"]], "a": [["101", "5"]]}
    await mock_indicator_calculator._process_websocket_message(message)

    assert len(mock_indicator_calculator.data_buffer) == 1
    mock_indicator_calculator._perform_calculations_and_save.assert_called_once()

@pytest.mark.asyncio
async def test_start_websocket_connection_and_processing(mock_indicator_calculator):
    # Mock websockets.connect to simulate receiving one message and then closing
    mock_websocket = AsyncMock()
    mock_websocket.recv.side_effect = [
        json.dumps({"e": "depthUpdate", "b": [["100", "10"]], "a": [["101", "5"]]}),
        websockets.exceptions.ConnectionClosedOK, # Simulate graceful close after one message
    ]
    
    with (
        pytest.raises(websockets.exceptions.ConnectionClosedOK), # Expecting the exception to propagate
        # Patch websockets.connect
        # This is a bit tricky with async context managers.
        # A more robust way would be to mock the entire websockets module or use a test server.
        # For this basic test, we'll mock the connect function directly.
        # Note: This mock might not perfectly simulate the async with behavior for all cases.
        # A better approach for real integration tests would be to use a test WebSocket server.
        # For unit testing, mocking the internal methods is more appropriate.
    ):
        with MagicMock(return_value=mock_websocket) as mock_connect:
            websockets.connect = mock_connect
            # Run start() in a separate task and cancel it after a short delay
            # to prevent it from running indefinitely in the test environment.
            task = asyncio.create_task(mock_indicator_calculator.start())
            await asyncio.sleep(0.1) # Give it a moment to connect and process
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass # Expected cancellation

    mock_websocket.recv.assert_called()
    assert len(mock_indicator_calculator.data_buffer) == 1 # Should have processed one message
