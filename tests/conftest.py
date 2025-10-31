"""
Pytest fixtures and configuration for test suite.
"""

import json
import os
import sys
from unittest.mock import Mock, MagicMock
from typing import Dict, Any

import pytest
from confluent_kafka import KafkaException

# Add project root to path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)


# ============================================================================
# Test Data Fixtures
# ============================================================================


@pytest.fixture
def sample_binance_message() -> Dict[str, Any]:
    """Sample valid Binance WebSocket trade message."""
    return {
        "e": "trade",  # Event type
        "E": 1640000000000,  # Event time
        "s": "BTCUSDT",  # Symbol
        "t": 12345,  # Trade ID
        "p": "50000.00",  # Price
        "q": "0.5",  # Quantity
        "T": 1640000000000,  # Trade time
        "m": False,  # Is buyer market maker
        "M": True,  # Ignore
    }


@pytest.fixture
def sample_binance_message_json(sample_binance_message) -> str:
    """Sample Binance message as JSON string."""
    return json.dumps(sample_binance_message)


@pytest.fixture
def invalid_binance_message_json() -> str:
    """Invalid JSON message."""
    return '{"invalid json": }'


@pytest.fixture
def malformed_binance_message() -> Dict[str, Any]:
    """Binance message with missing required fields."""
    return {
        "e": "trade",
        "s": "BTCUSDT",
        # Missing: t, T, p, q, m
    }


@pytest.fixture
def binance_message_negative_price() -> Dict[str, Any]:
    """Binance message with invalid (negative) price."""
    return {
        "s": "BTCUSDT",
        "t": 12345,
        "T": 1640000000000,
        "p": "-50000.00",  # Invalid negative price
        "q": "0.5",
        "m": False,
    }


@pytest.fixture
def expected_internal_message() -> Dict[str, Any]:
    """Expected internal message format after transformation."""
    return {
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "id": "12345",
        "event_ts": 1640000000,  # Converted from milliseconds to seconds
        "price": 50000.00,
        "qty": 0.5,
        "side": "buy",  # m=False means buyer
        # ingest_ts will be dynamic, tested separately
    }


# ============================================================================
# Configuration Fixtures
# ============================================================================


@pytest.fixture
def test_env_vars(monkeypatch) -> Dict[str, str]:
    """Set test environment variables."""
    env_vars = {
        "KAFKA_BOOTSTRAP": "test-kafka:9092",
        "KAFKA_USERNAME": "test-user",
        "KAFKA_PASSWORD": "test-password",
        "KAFKA_TOPIC": "test.topic",
        "WS_URL": "wss://test.example.com/ws",
        "EXCHANGE": "binance",
        "METRICS_PORT": "9000",
        "LOG_LEVEL": "DEBUG",
    }

    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)

    return env_vars


@pytest.fixture
def test_config_dict() -> Dict[str, Any]:
    """Test configuration dictionary."""
    return {
        "kafka_bootstrap": "test-kafka:9092",
        "kafka_username": "test-user",
        "kafka_password": "test-password",
        "kafka_topic": "test.topic",
        "ws_url": "wss://test.example.com/ws",
        "exchange": "binance",
        "metrics_port": 9000,
        "max_reconnect_delay": 300,
        "enable_dlq": False,
        "dlq_topic": None,
    }


# ============================================================================
# Mock Object Fixtures
# ============================================================================


@pytest.fixture
def mock_kafka_producer():
    """Mock Kafka Producer."""
    producer = MagicMock()
    producer.produce = Mock()
    producer.poll = Mock()
    producer.flush = Mock()
    producer.__len__ = Mock(return_value=0)
    return producer


@pytest.fixture
def mock_websocket():
    """Mock WebSocket connection."""
    ws = MagicMock()
    ws.run_forever = Mock()
    ws.close = Mock()
    return ws


@pytest.fixture
def mock_websocket_app(mock_websocket):
    """Mock WebSocketApp class."""
    mock_class = MagicMock(return_value=mock_websocket)
    return mock_class


# ============================================================================
# Kafka Callback Fixtures
# ============================================================================


@pytest.fixture
def kafka_success_callback():
    """Simulated successful Kafka delivery callback."""

    def callback(err, msg):
        assert err is None
        mock_msg = Mock()
        mock_msg.topic.return_value = "test.topic"
        mock_msg.partition.return_value = 0
        mock_msg.offset.return_value = 12345
        return mock_msg

    return callback


@pytest.fixture
def kafka_error_callback():
    """Simulated failed Kafka delivery callback."""

    def callback(err, msg):
        assert err is not None
        raise KafkaException("Delivery failed")

    return callback


# ============================================================================
# Metric Fixtures
# ============================================================================


@pytest.fixture(autouse=True)
def reset_prometheus_metrics():
    """Reset Prometheus metrics before each test."""
    # This prevents metric contamination between tests
    from prometheus_client import REGISTRY

    collectors = list(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        try:
            REGISTRY.unregister(collector)
        except Exception:
            pass  # Ignore if already unregistered

    yield

    # Cleanup after test
    collectors = list(REGISTRY._collector_to_names.keys())
    for collector in collectors:
        try:
            REGISTRY.unregister(collector)
        except Exception:
            pass


# ============================================================================
# Temporary File Fixtures
# ============================================================================


@pytest.fixture
def temp_env_file(tmp_path):
    """Create a temporary .env file for testing."""
    env_file = tmp_path / ".env"
    env_content = """
KAFKA_BOOTSTRAP=test-kafka:9092
KAFKA_USERNAME=test-user
KAFKA_PASSWORD=test-password
KAFKA_TOPIC=test.topic
WS_URL=wss://test.example.com/ws
EXCHANGE=binance
"""
    env_file.write_text(env_content)
    return str(env_file)


# ============================================================================
# Pytest Configuration Hooks
# ============================================================================


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: Unit tests (fast, no external dependencies)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (may require external services)"
    )
    config.addinivalue_line("markers", "slow: Slow running tests")
    config.addinivalue_line("markers", "kafka: Tests that interact with Kafka")
    config.addinivalue_line("markers", "websocket: Tests that interact with WebSocket")
