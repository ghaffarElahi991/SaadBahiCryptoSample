#!/usr/bin/env python3
"""Place a Crypto.com Exchange order with the signed REST API."""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import uuid
from typing import Any

import requests
from dotenv import load_dotenv

from market_data import (
    DEFAULT_REDIS_URL,
    MarketDataError,
    add_dynamic_price_arguments,
    resolve_dynamic_price,
    validate_price_arguments,
)


DEFAULT_BASE_URL = "https://api.crypto.com/exchange/v1"
METHOD = "private/create-order"

load_dotenv()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def value_to_string(value: Any) -> str:
    """Serialize nested values using Crypto.com's signature rules."""
    if isinstance(value, dict):
        return "".join(key + value_to_string(value[key]) for key in sorted(value))
    if isinstance(value, list):
        return "".join(value_to_string(item) for item in value)
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def sign(
    method: str,
    request_id: int,
    api_key: str,
    params: dict[str, Any],
    nonce: int,
    secret_key: str,
) -> str:
    payload = f"{method}{request_id}{api_key}{value_to_string(params)}{nonce}"
    return hmac.new(
        secret_key.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Place a Crypto.com Exchange order")
    parser.add_argument("instrument", help="Instrument name, for example BTC_USDT")
    parser.add_argument("side", type=str.upper, choices=("BUY", "SELL"))
    parser.add_argument("quantity", help="Order quantity, kept as an exact decimal string")
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
        choices=("GOOD_TILL_CANCEL", "IMMEDIATE_OR_CANCEL", "FILL_OR_KILL"),
        help="Optional order time in force",
    )
    parser.add_argument(
        "--post-only",
        action="store_true",
        help="Submit a maker-only LIMIT order",
    )
    parser.add_argument("--client-order-id", help="Optional client order ID")
    parser.add_argument("--spot-margin", choices=("SPOT", "MARGIN"))
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    args = parser.parse_args()

    validate_price_arguments(parser, args, requires_price=args.order_type == "LIMIT")
    if args.post_only and args.order_type != "LIMIT":
        parser.error("--post-only can only be used with a LIMIT order")
    if args.post_only and args.time_in_force not in (None, "GOOD_TILL_CANCEL"):
        parser.error("--post-only requires GOOD_TILL_CANCEL time in force")
    return args


def response_payload(response: requests.Response) -> object:
    try:
        return response.json()
    except ValueError:
        return {"raw_response": response.text}


def main() -> None:
    args = parse_args()
    api_key = required_env("CRYPTO_COM_API_KEY")
    secret_key = required_env("CRYPTO_COM_API_SECRET")
    base_url = os.getenv("CRYPTO_COM_BASE_URL", DEFAULT_BASE_URL).rstrip("/")

    order_price = args.price
    if args.price_mode:
        try:
            decision = resolve_dynamic_price(
                exchange="crypto_com",
                instrument=args.instrument,
                order_side=args.side,
                price_mode=args.price_mode,
                book_side=args.book_side,
                price_offset=args.price_offset,
                base_url=base_url,
                redis_url=os.getenv("REDIS_URL", DEFAULT_REDIS_URL),
                max_price_age=args.max_price_age,
                timeout=args.timeout,
            )
        except MarketDataError as exc:
            raise SystemExit(f"Cannot calculate Crypto.com order price: {exc}") from exc
        order_price = decision.price
        print(
            f"price source={decision.source} {decision.book_side}={decision.reference_price} "
            f"offset={decision.offset} order_price={decision.price}",
            file=sys.stderr,
        )

    params: dict[str, Any] = {
        "instrument_name": args.instrument.upper(),
        "side": args.side,
        "type": args.order_type,
        "quantity": args.quantity,
        "client_oid": args.client_order_id or f"py-{uuid.uuid4().hex}",
    }
    if order_price:
        params["price"] = order_price
    if args.time_in_force:
        params["time_in_force"] = args.time_in_force
    elif args.post_only:
        params["time_in_force"] = "GOOD_TILL_CANCEL"
    if args.post_only:
        params["exec_inst"] = ["POST_ONLY"]
    if args.spot_margin:
        params["spot_margin"] = args.spot_margin

    nonce = int(time.time() * 1000)
    request_id = nonce
    request_body = {
        "id": request_id,
        "method": METHOD,
        "api_key": api_key,
        "params": params,
        "nonce": nonce,
        "sig": sign(METHOD, request_id, api_key, params, nonce, secret_key),
    }

    request_started = time.perf_counter()
    try:
        response = requests.post(
            f"{base_url}/{METHOD}",
            headers={"Content-Type": "application/json"},
            json=request_body,
            timeout=args.timeout,
        )
    except requests.RequestException as exc:
        latency_ms = (time.perf_counter() - request_started) * 1000
        raise SystemExit(
            f"Crypto.com request failed after {latency_ms:.3f} ms: {exc}"
        ) from exc

    latency_ms = (time.perf_counter() - request_started) * 1000
    print(f"order_latency_ms={latency_ms:.3f}", file=sys.stderr)

    result = response_payload(response)
    print(json.dumps(result, indent=2))
    if not response.ok:
        raise SystemExit(f"Crypto.com returned HTTP {response.status_code}")
    if isinstance(result, dict) and result.get("code") != 0:
        raise SystemExit(
            f"Crypto.com rejected the order: {result.get('message', 'unknown error')}"
        )


if __name__ == "__main__":
    main()
