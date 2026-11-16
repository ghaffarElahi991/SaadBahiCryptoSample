#!/usr/bin/env python3
"""Stream exchange best bid/ask prices into Redis."""

import argparse
from decimal import Decimal, InvalidOperation
import json
import os
import sys
import time
from typing import Any

from dotenv import load_dotenv
import redis
import websocket

from market_data import DEFAULT_REDIS_URL, quote_cache_key


DEFAULT_WS_URLS = {
    "binance": "wss://stream.binance.com:9443/ws",
    "okx": "wss://ws.okx.com/ws/v5/public",
    "crypto_com": "wss://stream.crypto.com/exchange/v1/market",
}

load_dotenv()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cache best bid/ask prices in Redis")
    parser.add_argument("exchange", choices=("binance", "okx", "crypto_com"))
    parser.add_argument("instrument", help="Exchange instrument/symbol")
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", DEFAULT_REDIS_URL),
        help="Redis connection URL",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=30,
        help="Delete cached data after this many seconds without an update (default: 30)",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=2.0,
        help="Seconds before reconnecting (default: 2)",
    )
    parser.add_argument("--print-updates", action="store_true")
    args = parser.parse_args()
    if args.cache_ttl <= 0:
        parser.error("--cache-ttl must be greater than zero")
    if args.reconnect_delay < 0:
        parser.error("--reconnect-delay cannot be negative")
    return args


class QuoteCollector:
    def __init__(self, args: argparse.Namespace) -> None:
        self.exchange = args.exchange
        self.instrument = args.instrument.upper()
        self.cache_ttl = args.cache_ttl
        self.reconnect_delay = args.reconnect_delay
        self.print_updates = args.print_updates
        self.key = quote_cache_key(self.exchange, self.instrument)
        self.redis = redis.Redis.from_url(args.redis_url, decode_responses=True)
        self.seen_quote = False

        env_name = f"{self.exchange.upper()}_WS_URL"
        if self.exchange == "crypto_com":
            env_name = "CRYPTO_COM_WS_URL"
        self.ws_base_url = os.getenv(env_name, DEFAULT_WS_URLS[self.exchange]).rstrip("/")

    def websocket_url(self) -> str:
        if self.exchange == "binance":
            return f"{self.ws_base_url}/{self.instrument.lower()}@bookTicker"
        return self.ws_base_url

    def on_open(self, ws: websocket.WebSocketApp) -> None:
        print(f"connected to {self.exchange} for {self.instrument}", file=sys.stderr)
        if self.exchange == "okx":
            ws.send(
                json.dumps(
                    {
                        "id": str(int(time.time() * 1000)),
                        "op": "subscribe",
                        "args": [{"channel": "tickers", "instId": self.instrument}],
                    }
                )
            )
        elif self.exchange == "crypto_com":
            # Crypto.com recommends waiting one second after connecting before
            # sending the first request to avoid a pro-rated rate-limit error.
            time.sleep(1)
            now = int(time.time() * 1000)
            ws.send(
                json.dumps(
                    {
                        "id": now,
                        "method": "subscribe",
                        "params": {"channels": [f"ticker.{self.instrument}"]},
                        "nonce": now,
                    }
                )
            )

    def extract_quote(
        self,
        ws: websocket.WebSocketApp,
        payload: dict[str, Any],
    ) -> tuple[str, str] | None:
        if self.exchange == "binance":
            payload = payload.get("data", payload)
            bid, ask = payload.get("b"), payload.get("a")
        elif self.exchange == "okx":
            rows = payload.get("data", [])
            if not rows:
                return None
            bid, ask = rows[0].get("bidPx"), rows[0].get("askPx")
        else:
            if payload.get("method") == "public/heartbeat":
                ws.send(
                    json.dumps(
                        {
                            "id": payload.get("id"),
                            "method": "public/respond-heartbeat",
                        }
                    )
                )
                return None
            rows = payload.get("result", {}).get("data", [])
            if not rows:
                return None
            bid, ask = rows[0].get("b"), rows[0].get("k")

        if not bid or not ask:
            return None
        try:
            if Decimal(str(bid)) <= 0 or Decimal(str(ask)) <= 0:
                return None
        except InvalidOperation:
            return None
        return str(bid), str(ask)

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        try:
            payload = json.loads(message)
            quote = self.extract_quote(ws, payload)
            if quote is None:
                return
            bid, ask = quote
            timestamp_ms = int(time.time() * 1000)
            self.redis.hset(
                self.key,
                mapping={
                    "exchange": self.exchange,
                    "instrument": self.instrument,
                    "bid": bid,
                    "ask": ask,
                    "timestamp_ms": timestamp_ms,
                },
            )
            self.redis.expire(self.key, self.cache_ttl)
            if self.print_updates or not self.seen_quote:
                print(
                    f"cached {self.key}: bid={bid} ask={ask} timestamp_ms={timestamp_ms}",
                    file=sys.stderr,
                )
            self.seen_quote = True
        except (ValueError, TypeError, redis.RedisError) as exc:
            print(f"message handling failed: {exc}", file=sys.stderr)

    @staticmethod
    def on_error(_ws: websocket.WebSocketApp, error: Any) -> None:
        print(f"WebSocket error: {error}", file=sys.stderr)

    @staticmethod
    def on_close(
        _ws: websocket.WebSocketApp,
        status_code: int | None,
        message: str | None,
    ) -> None:
        print(
            f"WebSocket closed (status={status_code}, message={message})",
            file=sys.stderr,
        )

    def run(self) -> None:
        self.redis.ping()
        print(f"Redis key: {self.key}", file=sys.stderr)
        while True:
            app = websocket.WebSocketApp(
                self.websocket_url(),
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            app.run_forever(ping_interval=20, ping_timeout=10)
            print(
                f"reconnecting in {self.reconnect_delay:g} seconds...",
                file=sys.stderr,
            )
            time.sleep(self.reconnect_delay)


def main() -> None:
    args = parse_args()
    try:
        QuoteCollector(args).run()
    except redis.RedisError as exc:
        raise SystemExit(f"Redis connection failed: {exc}") from exc
    except KeyboardInterrupt:
        print("stopped", file=sys.stderr)


if __name__ == "__main__":
    main()
