#!/usr/bin/env python3
"""Place an OKX order with the signed REST API."""

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

from market_data import (
    DEFAULT_REDIS_URL,
    MarketDataError,
    add_dynamic_price_arguments,
    resolve_dynamic_price,
    validate_price_arguments,
)


DEFAULT_BASE_URL = "https://openapi.okx.com"
ORDER_PATH = "/api/v5/trade/order"

load_dotenv()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sign(timestamp: str, method: str, path: str, body: str, secret_key: str) -> str:
    prehash = f"{timestamp}{method.upper()}{path}{body}"
    digest = hmac.new(
        secret_key.encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("ascii")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Place an OKX order")
    parser.add_argument("instrument", help="Instrument ID, for example BTC-USDT")
    parser.add_argument("side", type=str.lower, choices=("buy", "sell"))
    parser.add_argument("size", help="Order size, kept as an exact decimal string")
    parser.add_argument(
        "--type",
        dest="order_type",
        type=str.lower,
        choices=("limit", "market", "post_only", "fok", "ioc"),
        default="limit",
        help="Order type (default: limit)",
    )
    parser.add_argument("--price", help="Required for limit, post_only, fok, and ioc orders")
    add_dynamic_price_arguments(parser)
    parser.add_argument(
        "--post-only",
        action="store_true",
        help="Submit a maker-only order (equivalent to --type post_only)",
    )
    parser.add_argument(
        "--trade-mode",
        choices=("cash", "cross", "isolated", "spot_isolated"),
        default="cash",
        help="Trading/margin mode (default: cash)",
    )
    parser.add_argument("--client-order-id", help="Optional client order ID")
    parser.add_argument("--position-side", choices=("long", "short", "net"))
    parser.add_argument("--reduce-only", action="store_true")
    parser.add_argument(
        "--target-currency",
        choices=("base_ccy", "quote_ccy"),
        help="Unit used by size for Spot market orders",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Send to OKX Demo Trading (requires demo API credentials)",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    args = parser.parse_args()

    validate_price_arguments(parser, args, requires_price=args.order_type != "market")
    if args.post_only and args.order_type not in ("limit", "post_only"):
        parser.error("--post-only can only be used with a limit order")
    return args


def response_payload(response: requests.Response) -> object:
    try:
        return response.json()
    except ValueError:
        return {"raw_response": response.text}


def main() -> None:
    args = parse_args()
    api_key = required_env("OKX_API_KEY")
    secret_key = required_env("OKX_API_SECRET")
    passphrase = required_env("OKX_API_PASSPHRASE")
    base_url = os.getenv("OKX_BASE_URL", DEFAULT_BASE_URL).rstrip("/")

    order_price = args.price
    if args.price_mode:
        try:
            decision = resolve_dynamic_price(
                exchange="okx",
                instrument=args.instrument,
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
            raise SystemExit(f"Cannot calculate OKX order price: {exc}") from exc
        order_price = decision.price
        print(
            f"price source={decision.source} bid={decision.bid} ask={decision.ask} "
            f"spread={decision.spread} reference={decision.book_side}:"
            f"{decision.reference_price} spread_multiplier={decision.spread_multiplier} "
            f"epsilon={decision.epsilon} side_adjustment={decision.spread_adjustment} "
            f"offset={decision.offset} "
            f"order_price={decision.price}",
            file=sys.stderr,
        )

    order: dict[str, str | bool] = {
        "instId": args.instrument.upper(),
        "tdMode": args.trade_mode,
        "side": args.side,
        "ordType": "post_only" if args.post_only else args.order_type,
        "sz": args.size,
    }
    if order_price:
        order["px"] = order_price
    if args.client_order_id:
        order["clOrdId"] = args.client_order_id
    if args.position_side:
        order["posSide"] = args.position_side
    if args.reduce_only:
        order["reduceOnly"] = True
    if args.target_currency:
        order["tgtCcy"] = args.target_currency

    # The exact serialized body must be included in the signature.
    body = json.dumps(order, separators=(",", ":"), ensure_ascii=False)
    timestamp = utc_timestamp()
    headers = {
        "Content-Type": "application/json",
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": sign(timestamp, "POST", ORDER_PATH, body, secret_key),
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
    }
    if args.demo:
        headers["x-simulated-trading"] = "1"

    request_started = time.perf_counter()
    try:
        response = requests.post(
            f"{base_url}{ORDER_PATH}",
            headers=headers,
            data=body.encode("utf-8"),
            timeout=args.timeout,
        )
    except requests.RequestException as exc:
        latency_ms = (time.perf_counter() - request_started) * 1000
        raise SystemExit(f"OKX request failed after {latency_ms:.3f} ms: {exc}") from exc

    latency_ms = (time.perf_counter() - request_started) * 1000
    print(f"order_latency_ms={latency_ms:.3f}", file=sys.stderr)

    result = response_payload(response)
    print(json.dumps(result, indent=2))
    if not response.ok:
        raise SystemExit(f"OKX returned HTTP {response.status_code}")
    if isinstance(result, dict) and result.get("code") != "0":
        raise SystemExit(f"OKX rejected the order: {result.get('msg', 'unknown error')}")


if __name__ == "__main__":
    main()
