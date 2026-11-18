#!/usr/bin/env python3
"""Place a Binance Spot order with the signed REST API."""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

from market_data import (
    DEFAULT_REDIS_URL,
    MarketDataError,
    add_dynamic_price_arguments,
    resolve_dynamic_price,
    validate_price_arguments,
)


DEFAULT_BASE_URL = "https://api.binance.com"

load_dotenv()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def sign(payload: str, secret_key: str) -> str:
    return hmac.new(
        secret_key.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Place a Binance Spot order")
    parser.add_argument("symbol", help="Trading symbol, for example BTCUSDT")
    parser.add_argument("side", type=str.upper, choices=("BUY", "SELL"))
    parser.add_argument("quantity", help="Base-asset quantity, kept as an exact decimal string")
    parser.add_argument(
        "--type",
        dest="order_type",
        type=str.upper,
        choices=("LIMIT", "MARKET"),
        default="LIMIT",
        help="Order type (default: LIMIT)",
    )
    parser.add_argument("--price", help="Required for a LIMIT order")
    add_dynamic_price_arguments(parser)
    parser.add_argument(
        "--time-in-force",
        choices=("GTC", "IOC", "FOK"),
        default="GTC",
        help="LIMIT order time in force (default: GTC)",
    )
    parser.add_argument(
        "--post-only",
        action="store_true",
        help="Submit a maker-only LIMIT_MAKER order",
    )
    parser.add_argument("--client-order-id", help="Optional client order ID")
    parser.add_argument(
        "--recv-window",
        type=int,
        default=5000,
        help="Request validity window in milliseconds (default: 5000)",
    )
    parser.add_argument(
        "--test-order",
        action="store_true",
        help="Validate the order through /api/v3/order/test without sending it to the matching engine",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    args = parser.parse_args()

    validate_price_arguments(parser, args, requires_price=args.order_type == "LIMIT")
    if args.post_only and args.order_type != "LIMIT":
        parser.error("--post-only can only be used with a LIMIT order")
    if not 1 <= args.recv_window <= 60000:
        parser.error("--recv-window must be between 1 and 60000 milliseconds")
    return args


def response_payload(response: requests.Response) -> object:
    try:
        return response.json()
    except ValueError:
        return {"raw_response": response.text}


def main() -> None:
    args = parse_args()
    api_key = required_env("BINANCE_API_KEY")
    secret_key = required_env("BINANCE_API_SECRET")
    base_url = os.getenv("BINANCE_BASE_URL", DEFAULT_BASE_URL).rstrip("/")

    order_price = args.price
    if args.price_mode:
        try:
            decision = resolve_dynamic_price(
                exchange="binance",
                instrument=args.symbol,
                order_side=args.side,
                price_mode=args.price_mode,
                book_side=args.book_side,
                price_offset=args.price_offset,
                spread_multiplier=args.spread_multiplier,
                base_url=base_url,
                redis_url=os.getenv("REDIS_URL", DEFAULT_REDIS_URL),
                max_price_age=args.max_price_age,
                timeout=args.timeout,
            )
        except MarketDataError as exc:
            raise SystemExit(f"Cannot calculate Binance order price: {exc}") from exc
        order_price = decision.price
        print(
            f"price source={decision.source} bid={decision.bid} ask={decision.ask} "
            f"spread={decision.spread} reference={decision.book_side}:"
            f"{decision.reference_price} spread_multiplier={decision.spread_multiplier} "
            f"spread_adjustment={decision.spread_adjustment} offset={decision.offset} "
            f"order_price={decision.price}",
            file=sys.stderr,
        )

    params: dict[str, str | int] = {
        "symbol": args.symbol.upper(),
        "side": args.side,
        "type": "LIMIT_MAKER" if args.post_only else args.order_type,
        "quantity": args.quantity,
        "recvWindow": args.recv_window,
        "timestamp": int(time.time() * 1000),
    }
    if args.order_type == "LIMIT":
        params["price"] = order_price
        # LIMIT_MAKER is Binance's post-only type and does not take timeInForce.
        if not args.post_only:
            params["timeInForce"] = args.time_in_force
    if args.client_order_id:
        params["newClientOrderId"] = args.client_order_id

    # Binance requires the already-percent-encoded payload to be signed.
    payload = urlencode(params, encoding="utf-8")
    request_body = f"{payload}&signature={sign(payload, secret_key)}"
    endpoint = "/api/v3/order/test" if args.test_order else "/api/v3/order"
    headers = {
        "X-MBX-APIKEY": api_key,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    request_started = time.perf_counter()
    try:
        response = requests.post(
            f"{base_url}{endpoint}",
            headers=headers,
            data=request_body,
            timeout=args.timeout,
        )
    except requests.RequestException as exc:
        latency_ms = (time.perf_counter() - request_started) * 1000
        raise SystemExit(
            f"Binance request failed after {latency_ms:.3f} ms: {exc}"
        ) from exc

    latency_ms = (time.perf_counter() - request_started) * 1000
    print(f"order_latency_ms={latency_ms:.3f}", file=sys.stderr)

    result = response_payload(response)
    print(json.dumps(result, indent=2))
    if not response.ok:
        raise SystemExit(f"Binance returned HTTP {response.status_code}")


if __name__ == "__main__":
    main()
