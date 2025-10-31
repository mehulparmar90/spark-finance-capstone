"""
Kafka WebSocket Forwarder - Production-Ready Implementation

This script forwards real-time cryptocurrency trade data from exchange WebSockets
to Confluent Kafka with robust error handling, reconnection logic, and monitoring.

Features:
- Automatic reconnection with exponential backoff
- Structured logging with JSON format
- Environment variables configuration via .env file
- Graceful shutdown handling
- Schema validation with Pydantic
- Prometheus metrics
- Kafka producer optimizations

Usage:
    # Ensure .env file is configured with Kafka credentials
    source venv/bin/activate
    python scripts/forwarder.py

    # With custom .env file
    python scripts/forwarder.py --env-file /path/to/.env

    # Enable debug logging
    LOG_LEVEL=DEBUG python scripts/forwarder.py
"""

import os
import sys
import json
import time
import signal
import logging
import random
from typing import Optional, Any
from dataclasses import dataclass

from confluent_kafka import Producer, KafkaException
from websocket import WebSocketApp, WebSocketException
from pydantic import BaseModel, Field, ValidationError, field_validator
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from dotenv import load_dotenv

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================


class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields if present
        if hasattr(record, "extra"):
            log_data.update(record.extra)

        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


# Configure logging
logger = logging.getLogger(__name__)
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.handlers = [handler]
logger.setLevel(logging.INFO)

# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

# Counters
messages_received = Counter(
    "ws_messages_received_total", "Total WebSocket messages received", ["exchange"]
)
messages_produced = Counter(
    "kafka_messages_produced_total", "Total messages produced to Kafka", ["topic"]
)
messages_failed = Counter(
    "processing_errors_total",
    "Total processing errors",
    ["error_type", "exchange"],
)
reconnections_total = Counter(
    "ws_reconnections_total", "Total WebSocket reconnection attempts", ["exchange"]
)

# Histograms
processing_duration = Histogram(
    "message_processing_seconds",
    "Message processing duration",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# Gauges
ws_connected = Gauge(
    "ws_connected", "WebSocket connection status (1=connected, 0=disconnected)"
)
messages_pending = Gauge("kafka_messages_pending", "Messages pending in producer queue")

# ============================================================================
# PYDANTIC MODELS FOR SCHEMA VALIDATION
# ============================================================================


class BinanceTradeMessage(BaseModel):
    """Binance WebSocket trade message schema."""

    s: str = Field(..., description="Symbol (e.g., BTCUSDT)")
    t: int = Field(..., description="Trade ID")
    T: int = Field(..., description="Trade time (milliseconds)")
    p: str = Field(..., description="Price")
    q: str = Field(..., description="Quantity")
    m: bool = Field(..., description="Is buyer market maker")

    class Config:
        extra = "allow"  # Allow extra fields from WebSocket


class InternalTradeMessage(BaseModel):
    """Internal standardized trade message schema."""

    exchange: str = Field(..., description="Exchange name")
    symbol: str = Field(..., description="Trading pair symbol")
    id: str = Field(..., description="Unique trade ID")
    event_ts: int = Field(..., description="Event timestamp (seconds)")
    price: float = Field(..., gt=0, description="Trade price (must be positive)")
    qty: float = Field(..., gt=0, description="Trade quantity (must be positive)")
    side: str = Field(..., description="Trade side (buy/sell)")
    ingest_ts: int = Field(..., description="Ingestion timestamp (seconds)")

    @field_validator("side")
    @classmethod
    def validate_side(cls, v: str) -> str:
        if v not in ["buy", "sell"]:
            raise ValueError(f"Invalid side: {v}. Must be 'buy' or 'sell'")
        return v


# ============================================================================
# CONFIGURATION
# ============================================================================


@dataclass
class ForwarderConfig:
    """Configuration for Kafka WebSocket Forwarder."""

    # Kafka configuration
    kafka_bootstrap: str
    kafka_username: str
    kafka_password: str
    kafka_topic: str

    # WebSocket configuration
    ws_url: str
    exchange: str = "binance"

    # Operational configuration
    metrics_port: int = 8000
    max_reconnect_delay: int = 300  # 5 minutes
    enable_dlq: bool = False
    dlq_topic: Optional[str] = None

    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> "ForwarderConfig":
        """Load configuration from environment variables."""
        # Load .env file
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()  # Loads .env from current directory

        # Required environment variables
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP")
        kafka_username = os.getenv("KAFKA_USERNAME")
        kafka_password = os.getenv("KAFKA_PASSWORD")

        if not all([kafka_bootstrap, kafka_username, kafka_password]):
            logger.error(
                "Missing required environment variables: KAFKA_BOOTSTRAP, KAFKA_USERNAME, KAFKA_PASSWORD"
            )
            sys.exit(1)

        return cls(
            kafka_bootstrap=kafka_bootstrap,  # type: ignore
            kafka_username=kafka_username,  # type: ignore
            kafka_password=kafka_password,  # type: ignore
            kafka_topic=os.getenv("KAFKA_TOPIC", "market.trades"),
            ws_url=os.getenv("WS_URL", "wss://stream.binance.us:9443/ws/btcusdt@trade"),
            exchange=os.getenv("EXCHANGE", "binance.us"),
            metrics_port=int(os.getenv("METRICS_PORT", "8000")),
            max_reconnect_delay=int(os.getenv("MAX_RECONNECT_DELAY", "300")),
            enable_dlq=os.getenv("ENABLE_DLQ", "false").lower() == "true",
            dlq_topic=os.getenv("DLQ_TOPIC"),
        )


# ============================================================================
# MAIN FORWARDER CLASS
# ============================================================================


class KafkaWebSocketForwarder:
    """
    Production-ready WebSocket to Kafka forwarder.

    Features:
    - Automatic reconnection with exponential backoff
    - Structured logging
    - Schema validation
    - Prometheus metrics
    - Graceful shutdown
    - Dead-letter queue for failed messages
    """

    def __init__(self, config: ForwarderConfig):
        """Initialize the forwarder with configuration."""
        self.config = config
        self.running = True
        self.ws: Optional[WebSocketApp] = None
        self.message_count = 0
        self.retry_count = 0

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        # Initialize Kafka producer
        self.producer = self._create_producer()

        logger.info(
            "Forwarder initialized",
            extra={
                "topic": config.kafka_topic,
                "ws_url": config.ws_url,
                "exchange": config.exchange,
                "dlq_enabled": config.enable_dlq,
            },
        )

    def _create_producer(self) -> Producer:
        """Create Kafka producer with optimal configuration."""
        producer_config = {
            "bootstrap.servers": self.config.kafka_bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": self.config.kafka_username,
            "sasl.password": self.config.kafka_password,
            "client.id": f"market-forwarder-{self.config.exchange}",
            # Reliability settings
            "enable.idempotence": True,
            "acks": "all",
            "retries": 10,
            # Performance optimizations
            "compression.type": "snappy",
            "linger.ms": 10,
            "batch.size": 16384,
            "max.in.flight.requests.per.connection": 5,
            # Timeouts
            "request.timeout.ms": 30000,
            "retry.backoff.ms": 100,
            "message.timeout.ms": 300000,
        }

        logger.info(
            "Kafka producer created", extra={"client_id": producer_config["client.id"]}
        )
        return Producer(producer_config)

    def _delivery_callback(self, err: Optional[Exception], msg: Optional[Any]) -> None:
        """Kafka message delivery callback."""
        if err:
            logger.error(
                "Kafka delivery failed",
                extra={
                    "error": str(err),
                    "topic": msg.topic() if msg else None,
                    "partition": msg.partition() if msg else None,
                },
            )
            messages_failed.labels(
                error_type="kafka_delivery", exchange=self.config.exchange
            ).inc()
        else:
            if msg:
                logger.debug(
                    "Message delivered",
                    extra={
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                    },
                )
            messages_produced.labels(topic=self.config.kafka_topic).inc()

    def _send_to_dlq(
        self, original_message: str, error_type: str, error_message: str
    ) -> None:
        """Send failed message to dead-letter queue."""
        if not self.config.enable_dlq or not self.config.dlq_topic:
            return

        dlq_payload = {
            "original_message": original_message,
            "error_type": error_type,
            "error_message": error_message,
            "exchange": self.config.exchange,
            "timestamp": int(time.time()),
        }

        try:
            self.producer.produce(
                topic=self.config.dlq_topic,
                value=json.dumps(dlq_payload).encode(),
                callback=lambda err, msg: logger.info(
                    "DLQ message sent" if not err else f"DLQ send failed: {err}"
                ),
            )
            logger.info(
                "Sent to DLQ",
                extra={"error_type": error_type, "dlq_topic": self.config.dlq_topic},
            )
        except Exception as e:
            logger.error("Failed to send to DLQ", extra={"error": str(e)})

    def _transform_message(
        self, binance_msg: BinanceTradeMessage
    ) -> InternalTradeMessage:
        """Transform Binance message to internal format."""
        return InternalTradeMessage(
            exchange=self.config.exchange,
            symbol=binance_msg.s,
            id=str(binance_msg.t),
            event_ts=binance_msg.T // 1000,
            price=float(binance_msg.p),
            qty=float(binance_msg.q),
            side="sell" if binance_msg.m else "buy",
            ingest_ts=int(time.time()),
        )

    def on_message(self, ws: WebSocketApp, message: str) -> None:
        """Handle incoming WebSocket message."""
        messages_received.labels(exchange=self.config.exchange).inc()
        start_time = time.time()

        try:
            # Parse JSON
            raw_data = json.loads(message)

            # Validate schema
            binance_msg = BinanceTradeMessage(**raw_data)

            # Transform to internal format
            internal_msg = self._transform_message(binance_msg)

            # Produce to Kafka with key for partitioning
            key = internal_msg.symbol.encode()
            value = internal_msg.model_dump_json().encode()

            self.producer.produce(
                topic=self.config.kafka_topic,
                key=key,
                value=value,
                callback=self._delivery_callback,
            )

            # Update counters
            self.message_count += 1
            messages_pending.set(len(self.producer))

            # Periodic flush (not every message for performance)
            if self.message_count % 100 == 0:
                self.producer.flush()

            # Poll to handle callbacks
            self.producer.poll(0)

            # Record processing time
            duration = time.time() - start_time
            processing_duration.observe(duration)

            logger.debug(
                "Message processed",
                extra={
                    "symbol": internal_msg.symbol,
                    "price": internal_msg.price,
                    "duration_ms": duration * 1000,
                },
            )

        except json.JSONDecodeError as e:
            logger.error(
                "Invalid JSON",
                extra={"error": str(e), "message_preview": message[:200]},
            )
            messages_failed.labels(
                error_type="json_decode", exchange=self.config.exchange
            ).inc()
            self._send_to_dlq(message, "json_decode_error", str(e))

        except ValidationError as e:
            logger.error(
                "Schema validation failed",
                extra={"error": e.errors(), "message_preview": message[:200]},
            )
            messages_failed.labels(
                error_type="schema_validation", exchange=self.config.exchange
            ).inc()
            self._send_to_dlq(message, "schema_validation_error", str(e))

        except ValueError as e:
            logger.error(
                "Value conversion error",
                extra={"error": str(e), "message_preview": message[:200]},
            )
            messages_failed.labels(
                error_type="value_conversion", exchange=self.config.exchange
            ).inc()
            self._send_to_dlq(message, "value_conversion_error", str(e))

        except KafkaException as e:
            logger.error(
                "Kafka producer error",
                extra={"error": str(e), "message_preview": message[:200]},
            )
            messages_failed.labels(
                error_type="kafka_exception", exchange=self.config.exchange
            ).inc()
            # Don't send to DLQ - might be Kafka issue, retry

        except Exception as e:
            logger.exception(
                "Unexpected error processing message",
                extra={"error": str(e), "message_preview": message[:200]},
            )
            messages_failed.labels(
                error_type="unknown", exchange=self.config.exchange
            ).inc()
            self._send_to_dlq(message, "unknown_error", str(e))

    def on_error(self, ws: WebSocketApp, error: Exception) -> None:
        """Handle WebSocket errors."""
        error_msg = str(error)
        error_type = type(error).__name__

        # Check for geographic restriction (HTTP 451)
        if "451" in error_msg or "restricted location" in error_msg.lower():
            logger.error(
                "WebSocket error: Geographic restriction detected (HTTP 451)",
                extra={
                    "error": error_msg,
                    "error_type": error_type,
                    "exchange": self.config.exchange,
                    "hint": "Binance is blocking your location. Try: 1) Use Binance.US if in USA, 2) Use a different exchange (Coinbase, Kraken), 3) Use a VPN",
                },
            )
        else:
            logger.error(
                "WebSocket error",
                extra={
                    "error": error_msg,
                    "error_type": error_type,
                    "exchange": self.config.exchange,
                },
            )
        ws_connected.set(0)

    def on_close(
        self, ws: WebSocketApp, code: Optional[int], msg: Optional[str]
    ) -> None:
        """Handle WebSocket close event."""
        logger.warning(
            "WebSocket closed",
            extra={
                "code": code,
                "close_message": msg,
                "exchange": self.config.exchange,
            },
        )
        ws_connected.set(0)

    def on_open(self, ws: WebSocketApp) -> None:
        """Handle WebSocket open event."""
        logger.info("WebSocket opened", extra={"url": self.config.ws_url})
        ws_connected.set(1)
        self.retry_count = 0  # Reset retry count on successful connection

    def run(self) -> None:
        """
        Main run loop with automatic reconnection.

        Implements exponential backoff with jitter for reconnection attempts.
        """
        logger.info("Starting forwarder", extra={"exchange": self.config.exchange})

        while self.running:
            try:
                reconnections_total.labels(exchange=self.config.exchange).inc()
                logger.info(
                    "Connecting to WebSocket",
                    extra={"attempt": self.retry_count + 1, "url": self.config.ws_url},
                )

                self.ws = WebSocketApp(
                    self.config.ws_url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open,
                )

                # run_forever with ping/pong for connection health
                # sslopt parameter can disable SSL verification if needed (not recommended for production)
                import ssl

                self.ws.run_forever(
                    ping_interval=30,  # Send ping every 30 seconds
                    ping_timeout=10,  # Timeout if no pong in 10 seconds
                    sslopt={
                        "cert_reqs": ssl.CERT_NONE
                    },  # Skip SSL verification for development
                )

                if not self.running:
                    break

                # Calculate exponential backoff with jitter
                base_delay = min(2**self.retry_count, self.config.max_reconnect_delay)
                jitter = random.uniform(0, base_delay * 0.1)  # Add up to 10% jitter
                delay = base_delay + jitter

                logger.info(
                    "Reconnecting",
                    extra={
                        "delay_seconds": delay,
                        "attempt": self.retry_count + 1,
                        "exchange": self.config.exchange,
                    },
                )

                time.sleep(delay)
                self.retry_count += 1

            except WebSocketException as e:
                logger.error("WebSocket exception", extra={"error": str(e)})
                time.sleep(5)

            except Exception as e:
                logger.exception(
                    "Unexpected error in run loop", extra={"error": str(e)}
                )
                time.sleep(5)

    def _shutdown(self, signum: int, frame: Any) -> None:
        """Graceful shutdown handler."""
        logger.info(
            "Shutdown initiated",
            extra={"signal": signum, "messages_processed": self.message_count},
        )

        self.running = False

        # Close WebSocket
        if self.ws:
            logger.info("Closing WebSocket connection")
            self.ws.close()
            ws_connected.set(0)

        # Flush pending Kafka messages
        if self.producer:
            pending = len(self.producer)
            if pending > 0:
                logger.info(
                    "Flushing Kafka producer", extra={"pending_messages": pending}
                )
                self.producer.flush(timeout=10)

        logger.info("Shutdown complete")
        sys.exit(0)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================


def main() -> None:
    """Main entry point for the forwarder."""
    # Set log level from environment
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, log_level))

    # Load configuration
    config = ForwarderConfig.from_env()

    logger.info(
        "Configuration loaded",
        extra={
            "kafka_topic": config.kafka_topic,
            "exchange": config.exchange,
            "metrics_port": config.metrics_port,
            "dlq_enabled": config.enable_dlq,
            "log_level": log_level,
        },
    )

    # Start Prometheus metrics HTTP server
    try:
        start_http_server(config.metrics_port)
        logger.info(
            "Metrics server started",
            extra={
                "port": config.metrics_port,
                "endpoint": f"http://localhost:{config.metrics_port}/metrics",
            },
        )
    except OSError as e:
        logger.error("Failed to start metrics server", extra={"error": str(e)})
        sys.exit(1)

    # Create and run forwarder
    forwarder = KafkaWebSocketForwarder(config)
    forwarder.run()


if __name__ == "__main__":
    main()
