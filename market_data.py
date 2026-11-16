"""Shared best-bid/ask price resolution for the order scripts."""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import time
from typing import Any

import requests


DEFAULT_REDIS_URL = "redis://localhost:6379/0"


class MarketDataError(RuntimeError):
    """Raised when a usable market quote cannot be obtained."""


@dataclass(frozen=True)
class Quote:
    bid: str
    ask: str
    timestamp_ms: int


@dataclass(frozen=True)
class PriceDecision:
    price: str
    reference_price: str
    book_side: str
    offset: str
    source: str


def add_dynamic_price_arguments(parser: Any) -> None:
    parser.add_argument(
        "--price-mode",
        choices=("rest", "websocket"),
        help="Get bid/ask from REST or from the Redis-backed WebSocket cache",
    )
    parser.add_argument(
        "--book-side",
        choices=("ask", "bid"),
        help="Reference ask or bid (default: bid for buys, ask for sells)",
    )
    parser.add_argument(
        "--price-offset",
        default="0",
        help="Signed amount added to the reference price, for example 0.10 or -0.10",
    )
    parser.add_argument(
        "--max-price-age",
        type=float,
        default=2.0,
        help="Maximum WebSocket cache age in seconds (default: 2)",
    )


def validate_price_arguments(parser: Any, args: Any, requires_price: bool) -> None:
    try:
        offset = Decimal(args.price_offset)
    except InvalidOperation:
        parser.error("--price-offset must be a valid decimal number")
    if not offset.is_finite():
        parser.error("--price-offset must be finite")
    if args.max_price_age <= 0:
        parser.error("--max-price-age must be greater than zero")

    if requires_price:
        if args.price and args.price_mode:
            parser.error("use either --price or --price-mode, not both")
        if not args.price and not args.price_mode:
            parser.error("a limit price requires --price or --price-mode")
    elif args.price or args.price_mode:
        parser.error("market orders do not accept --price or --price-mode")

    if not args.price_mode and args.book_side:
        parser.error("--book-side requires --price-mode")
    if not args.price_mode and offset != 0:
        parser.error("--price-offset requires --price-mode")


def quote_cache_key(exchange: str, instrument: str) -> str:
    return f"market:quote:{exchange}:{instrument.upper()}"


def _positive_decimal(value: Any, label: str) -> str:
    if value is None or value == "":
        raise MarketDataError(f"quote does not contain a {label} price")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation as exc:
        raise MarketDataError(f"invalid {label} price: {value!r}") from exc
    if not parsed.is_finite() or parsed <= 0:
        raise MarketDataError(f"invalid {label} price: {value!r}")
    return str(value)


def _request_json(url: str, params: dict[str, str], timeout: float) -> Any:
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise MarketDataError(f"market-data request failed: {exc}") from exc
    except ValueError as exc:
        raise MarketDataError("market-data endpoint returned invalid JSON") from exc


def get_rest_quote(
    exchange: str,
    instrument: str,
    base_url: str,
    timeout: float,
) -> Quote:
    instrument = instrument.upper()
    base_url = base_url.rstrip("/")

    if exchange == "binance":
        payload = _request_json(
            f"{base_url}/api/v3/ticker/bookTicker",
            {"symbol": instrument},
            timeout,
        )
        bid = payload.get("bidPrice") if isinstance(payload, dict) else None
        ask = payload.get("askPrice") if isinstance(payload, dict) else None
    elif exchange == "okx":
        payload = _request_json(
            f"{base_url}/api/v5/market/ticker",
            {"instId": instrument},
            timeout,
        )
        rows = payload.get("data", []) if isinstance(payload, dict) else []
        row = rows[0] if rows else {}
        bid = row.get("bidPx")
        ask = row.get("askPx")
    elif exchange == "crypto_com":
        payload = _request_json(
            f"{base_url}/public/get-tickers",
            {"instrument_name": instrument},
            timeout,
        )
        if not isinstance(payload, dict) or payload.get("code") != 0:
            message = payload.get("message", "unknown error") if isinstance(payload, dict) else "invalid response"
            raise MarketDataError(f"Crypto.com ticker request failed: {message}")
        rows = payload.get("result", {}).get("data", [])
        row = next((item for item in rows if item.get("i") == instrument), rows[0] if rows else {})
        bid = row.get("b")
        ask = row.get("k")
    else:
        raise MarketDataError(f"unsupported exchange: {exchange}")

    return Quote(
        bid=_positive_decimal(bid, "bid"),
        ask=_positive_decimal(ask, "ask"),
        timestamp_ms=int(time.time() * 1000),
    )


def get_websocket_quote(
    exchange: str,
    instrument: str,
    redis_url: str,
    max_age_seconds: float,
    timeout: float,
) -> Quote:
    try:
        import redis
    except ImportError as exc:
        raise MarketDataError(
            "Redis support is not installed; run: pip install -r requirment.txt"
        ) from exc

    key = quote_cache_key(exchange, instrument)
    try:
        client = redis.Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=timeout,
            socket_timeout=timeout,
        )
        cached = client.hgetall(key)
    except redis.RedisError as exc:
        raise MarketDataError(f"Redis request failed: {exc}") from exc

    if not cached:
        raise MarketDataError(
            f"no WebSocket quote found at {key}; start websocket_cache.py first"
        )
    try:
        timestamp_ms = int(cached["timestamp_ms"])
    except (KeyError, TypeError, ValueError) as exc:
        raise MarketDataError(f"invalid WebSocket quote stored at {key}") from exc

    age_seconds = max(0.0, (int(time.time() * 1000) - timestamp_ms) / 1000)
    if age_seconds > max_age_seconds:
        raise MarketDataError(
            f"WebSocket quote is stale ({age_seconds:.3f}s old; maximum {max_age_seconds:.3f}s)"
        )
    return Quote(
        bid=_positive_decimal(cached.get("bid"), "bid"),
        ask=_positive_decimal(cached.get("ask"), "ask"),
        timestamp_ms=timestamp_ms,
    )


def resolve_dynamic_price(
    *,
    exchange: str,
    instrument: str,
    order_side: str,
    price_mode: str,
    book_side: str | None,
    price_offset: str,
    base_url: str,
    redis_url: str,
    max_price_age: float,
    timeout: float,
) -> PriceDecision:
    if price_mode == "rest":
        quote = get_rest_quote(exchange, instrument, base_url, timeout)
    elif price_mode == "websocket":
        quote = get_websocket_quote(
            exchange,
            instrument,
            redis_url,
            max_price_age,
            timeout,
        )
    else:
        raise MarketDataError(f"unsupported price mode: {price_mode}")

    selected_side = book_side or ("bid" if order_side.lower() == "buy" else "ask")
    reference = getattr(quote, selected_side)
    calculated = Decimal(reference) + Decimal(price_offset)
    if not calculated.is_finite() or calculated <= 0:
        raise MarketDataError(
            f"calculated order price must be positive; got {reference} + {price_offset}"
        )
    return PriceDecision(
        price=format(calculated, "f"),
        reference_price=reference,
        book_side=selected_side,
        offset=price_offset,
        source=price_mode,
    )
