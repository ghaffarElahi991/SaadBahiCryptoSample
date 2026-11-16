#!/usr/bin/env python3
"""Read Binance Spot orders and balances without changing account state."""

import argparse
import hashlib
import hmac
import json
import os
import time
from typing import Any
from urllib.parse import urlencode

from dotenv import load_dotenv
import requests


DEFAULT_BASE_URL = "https://api.binance.com"

load_dotenv()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="View Binance Spot order status, open orders, history, and balances"
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        default=5000,
        help="Request validity window in milliseconds (default: 5000)",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout in seconds")
    commands = parser.add_subparsers(dest="command", required=True)

    status = commands.add_parser("status", help="Show one order")
    status.add_argument("symbol", help="Trading symbol, for example BTCUSDT")
    status.add_argument("order_id", help="Binance order ID")

    open_orders = commands.add_parser("open", help="Show current open orders")
    open_orders.add_argument(
        "symbol",
        nargs="?",
        default="BTCUSDT",
        help="Trading symbol (default: BTCUSDT)",
    )

    history = commands.add_parser("history", help="Show order history")
    history.add_argument(
        "symbol",
        nargs="?",
        default="BTCUSDT",
        help="Trading symbol (default: BTCUSDT)",
    )
    history.add_argument("--limit", type=int, default=20, help="Number of orders (default: 20)")

    balance = commands.add_parser("balance", help="Show one asset balance")
    balance.add_argument("asset", nargs="?", default="BTC", help="Asset (default: BTC)")

    args = parser.parse_args()
    if not 1 <= args.recv_window <= 60000:
        parser.error("--recv-window must be between 1 and 60000 milliseconds")
    if hasattr(args, "limit") and not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")
    return args


def signature(payload: str, secret_key: str) -> str:
    return hmac.new(
        secret_key.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def signed_get(
    *,
    base_url: str,
    path: str,
    params: dict[str, Any],
    api_key: str,
    secret_key: str,
    recv_window: int,
    timeout: float,
) -> Any:
    signed_params = dict(params)
    signed_params["recvWindow"] = recv_window
    signed_params["timestamp"] = int(time.time() * 1000)
    payload = urlencode(signed_params, encoding="utf-8")
    query = f"{payload}&signature={signature(payload, secret_key)}"

    try:
        response = requests.get(
            f"{base_url.rstrip('/')}{path}?{query}",
            headers={"X-MBX-APIKEY": api_key},
            timeout=timeout,
        )
    except requests.RequestException as exc:
        raise SystemExit(f"Binance request failed: {exc}") from exc

    try:
        result = response.json()
    except ValueError as exc:
        raise SystemExit(
            f"Binance returned HTTP {response.status_code} with invalid JSON"
        ) from exc
    if not response.ok:
        print(json.dumps(result, indent=2))
        raise SystemExit(f"Binance returned HTTP {response.status_code}")
    return result


def main() -> None:
    args = parse_args()
    api_key = required_env("BINANCE_API_KEY")
    secret_key = required_env("BINANCE_API_SECRET")
    base_url = os.getenv("BINANCE_BASE_URL", DEFAULT_BASE_URL)

    if args.command == "status":
        path = "/api/v3/order"
        params = {"symbol": args.symbol.upper(), "orderId": args.order_id}
    elif args.command == "open":
        path = "/api/v3/openOrders"
        params = {"symbol": args.symbol.upper()}
    elif args.command == "history":
        path = "/api/v3/allOrders"
        params = {"symbol": args.symbol.upper(), "limit": args.limit}
    else:
        path = "/api/v3/account"
        params = {}

    result = signed_get(
        base_url=base_url,
        path=path,
        params=params,
        api_key=api_key,
        secret_key=secret_key,
        recv_window=args.recv_window,
        timeout=args.timeout,
    )

    if args.command == "balance":
        asset = args.asset.upper()
        balances = result.get("balances", []) if isinstance(result, dict) else []
        result = next(
            (item for item in balances if item.get("asset") == asset),
            {"asset": asset, "free": "0", "locked": "0"},
        )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
