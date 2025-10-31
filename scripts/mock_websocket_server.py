#!/usr/bin/env python3
"""
Mock WebSocket Server for Testing Forwarder Locally

This server simulates Binance WebSocket trade messages for local development
when Binance.com is restricted in your geographic location.

Usage:
    # Terminal 1: Start mock server
    python scripts/mock_websocket_server.py

    # Terminal 2: Update .env and run forwarder
    # WS_URL=ws://localhost:8765
    # python scripts/forwarder.py
"""

import asyncio
import json
import random
import time
from datetime import datetime
from typing import Any

try:
    from websockets.server import serve
except ImportError:
    print("❌ websockets package not installed")
    print("Install with: pip install websockets")
    exit(1)


class MockBinanceTradeGenerator:
    """Generate realistic mock Binance trade messages."""

    def __init__(self, symbol: str = "BTCUSDT", base_price: float = 50000.0):
        self.symbol = symbol
        self.price = base_price
        self.trade_id = random.randint(100000000, 999999999)

    def generate_trade(self) -> dict:
        """Generate a single trade message."""
        # Simulate price movement (random walk)
        price_change = random.uniform(-100, 100)
        self.price = max(1000, self.price + price_change)

        # Generate trade
        self.trade_id += 1
        timestamp_ms = int(time.time() * 1000)

        trade = {
            "e": "trade",  # Event type
            "E": timestamp_ms,  # Event time
            "s": self.symbol,  # Symbol
            "t": self.trade_id,  # Trade ID
            "p": f"{self.price:.2f}",  # Price (string format like Binance)
            "q": f"{random.uniform(0.001, 2.0):.4f}",  # Quantity
            "b": random.randint(1000000, 9999999),  # Buyer order ID
            "a": random.randint(1000000, 9999999),  # Seller order ID
            "T": timestamp_ms,  # Trade time
            "m": random.choice([True, False]),  # Is buyer market maker
            "M": True,  # Ignore (always true)
        }

        return trade


async def handle_client(websocket: Any, path: str) -> None:
    """Handle a WebSocket client connection."""
    client_addr = websocket.remote_address
    print(f"✓ Client connected: {client_addr[0]}:{client_addr[1]}")

    # Create trade generator
    generator = MockBinanceTradeGenerator()
    trades_sent = 0

    try:
        while True:
            # Generate trade
            trade = generator.generate_trade()

            # Send to client
            await websocket.send(json.dumps(trade))
            trades_sent += 1

            # Log every 10th trade
            if trades_sent % 10 == 0:
                timestamp = datetime.now().strftime("%H:%M:%S")
                print(
                    f"[{timestamp}] Sent {trades_sent} trades | "
                    f"Latest: {trade['s']} @ ${trade['p']} (qty: {trade['q']})"
                )

            # Simulate realistic trade frequency (1-10 trades per second)
            await asyncio.sleep(random.uniform(0.1, 1.0))

    except Exception as e:
        print(
            f"✗ Client disconnected: {client_addr[0]}:{client_addr[1]} - {type(e).__name__}"
        )
        print(f"  Total trades sent: {trades_sent}")


async def main() -> None:
    """Start the mock WebSocket server."""
    host = "localhost"
    port = 8765

    print("=" * 70)
    print("Mock Binance WebSocket Server")
    print("=" * 70)
    print()
    print(f"Server: ws://{host}:{port}")
    print("Symbol: BTCUSDT")
    print("Trade Frequency: 1-10 per second")
    print()
    print("To use with forwarder, update .env:")
    print(f"  WS_URL=ws://{host}:{port}")
    print("  EXCHANGE=binance.mock")
    print()
    print("Press Ctrl+C to stop")
    print("=" * 70)
    print()

    async with serve(handle_client, host, port):
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print()
        print("✓ Server stopped")
