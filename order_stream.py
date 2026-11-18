#!/usr/bin/env python3
"""Monitor private order updates and calculate order fill times."""

import argparse
import base64
from collections import defaultdict
from decimal import Decimal, InvalidOperation
import hashlib
import hmac
import json
import os
import sys
import threading
import time
from typing import Any, TextIO
from urllib.parse import urlencode
import uuid

from dotenv import load_dotenv
import requests
import websocket


BINANCE_USER_WS_URL = "wss://ws-api.binance.com:443/ws-api/v3"
OKX_PRIVATE_WS_URL = "wss://ws.okx.com/ws/v5/private"
OKX_DEMO_PRIVATE_WS_URL = "wss://wspap.okx.com/ws/v5/private"
CRYPTO_COM_USER_WS_URL = "wss://stream.crypto.com/exchange/v1/user"

TERMINAL_STATES = {
    "FILLED",
    "CANCELED",
    "CANCELLED",
    "EXPIRED",
    "REJECTED",
    "MMP_CANCELED",
}

load_dotenv()


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream private order updates and report fill timing"
    )
    parser.add_argument("exchange", choices=("binance", "okx", "crypto_com"))
    parser.add_argument(
        "instrument",
        nargs="?",
        help="Optional symbol filter, e.g. SOLUSDT, SOL-USDT, or SOL_USDT",
    )
    parser.add_argument(
        "--account-type",
        choices=("spot", "cross-margin"),
        default="spot",
        help="Binance account stream to monitor (default: spot)",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Use OKX Demo Trading private WebSocket",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="Print recognized raw exchange events before normalized events",
    )
    parser.add_argument(
        "--log-file",
        help="Append normalized order and fill events as JSON Lines",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=2.0,
        help="Seconds before reconnecting (default: 2)",
    )
    args = parser.parse_args()
    if args.demo and args.exchange != "okx":
        parser.error("--demo is only supported for OKX")
    if args.account_type != "spot" and args.exchange != "binance":
        parser.error("--account-type cross-margin is only supported for Binance")
    if args.reconnect_delay < 0:
        parser.error("--reconnect-delay cannot be negative")
    return args


def hmac_hex(payload: str, secret: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def hmac_base64(payload: str, secret: str) -> str:
    digest = hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return base64.b64encode(digest).decode("ascii")


def decimal_divide(numerator: Any, denominator: Any) -> str | None:
    try:
        top = Decimal(str(numerator))
        bottom = Decimal(str(denominator))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if bottom == 0:
        return None
    return format(top / bottom, "f")


def int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def elapsed_ms(start_ms: int | None, end_ms: int | None) -> int | None:
    if start_ms is None or end_ms is None or end_ms < start_ms:
        return None
    return end_ms - start_ms


def is_positive_decimal(value: Any) -> bool:
    try:
        return Decimal(str(value)) > 0
    except (InvalidOperation, TypeError, ValueError):
        return False


class OrderStreamMonitor:
    def __init__(self, args: argparse.Namespace) -> None:
        self.exchange = args.exchange
        self.instrument = args.instrument.upper() if args.instrument else None
        self.account_type = getattr(args, "account_type", "spot")
        self.demo = args.demo
        self.raw = args.raw
        self.reconnect_delay = args.reconnect_delay
        self.first_seen: dict[tuple[str, str], float] = {}
        self.binance_fees: dict[tuple[str, str], dict[str, Decimal]] = defaultdict(
            lambda: defaultdict(Decimal)
        )
        self.authenticated = False
        self.margin_token_expiration_ms: int | None = None
        self.log_handle: TextIO | None = (
            open(args.log_file, "a", encoding="utf-8", buffering=1)
            if args.log_file
            else None
        )

        if self.exchange == "binance":
            self.api_key = required_env("BINANCE_API_KEY")
            self.secret_key = required_env("BINANCE_API_SECRET")
            self.passphrase = None
        elif self.exchange == "okx":
            self.api_key = required_env("OKX_API_KEY")
            self.secret_key = required_env("OKX_API_SECRET")
            self.passphrase = required_env("OKX_API_PASSPHRASE")
        else:
            self.api_key = required_env("CRYPTO_COM_API_KEY")
            self.secret_key = required_env("CRYPTO_COM_API_SECRET")
            self.passphrase = None

    def close(self) -> None:
        if self.log_handle:
            self.log_handle.close()

    def websocket_url(self) -> str:
        if self.exchange == "binance":
            return os.getenv("BINANCE_USER_WS_URL", BINANCE_USER_WS_URL)
        if self.exchange == "okx":
            if self.demo:
                return os.getenv(
                    "OKX_DEMO_PRIVATE_WS_URL", OKX_DEMO_PRIVATE_WS_URL
                )
            return os.getenv("OKX_PRIVATE_WS_URL", OKX_PRIVATE_WS_URL)
        return os.getenv("CRYPTO_COM_USER_WS_URL", CRYPTO_COM_USER_WS_URL)

    def emit(self, event_type: str, details: dict[str, Any]) -> None:
        event = {
            "event_type": event_type,
            "received_at_ms": int(time.time() * 1000),
            **details,
        }
        line = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
        print(line, flush=True)
        if self.log_handle:
            self.log_handle.write(line + "\n")

    def print_raw(self, payload: Any) -> None:
        if self.raw:
            print(
                "RAW " + json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
                file=sys.stderr,
                flush=True,
            )

    def observed_elapsed(self, order_id: str, status: str) -> float:
        key = (self.exchange, order_id)
        now = time.perf_counter()
        started = self.first_seen.setdefault(key, now)
        duration = (now - started) * 1000
        if status in TERMINAL_STATES:
            self.first_seen.pop(key, None)
        return round(duration, 3)

    def on_open(self, ws: websocket.WebSocketApp) -> None:
        self.authenticated = False
        print(
            f"connected to {self.exchange} private order stream"
            + (
                f" ({self.account_type})"
                if self.exchange == "binance"
                else ""
            )
            + (f" for {self.instrument}" if self.instrument else " for all instruments"),
            file=sys.stderr,
            flush=True,
        )
        try:
            if self.exchange == "binance":
                self.send_binance_subscription(ws)
            elif self.exchange == "okx":
                self.send_okx_login(ws)
            else:
                # Crypto.com recommends waiting before the first request after connect.
                time.sleep(1)
                self.send_crypto_auth(ws)
        except RuntimeError as exc:
            print(f"private stream setup failed: {exc}", file=sys.stderr, flush=True)
            ws.close()

    def send_binance_subscription(self, ws: websocket.WebSocketApp) -> None:
        if self.account_type == "cross-margin":
            listen_token = self.create_binance_margin_listen_token()
            ws.send(
                json.dumps(
                    {
                        "id": str(uuid.uuid4()),
                        "method": "userDataStream.subscribe.listenToken",
                        "params": {"listenToken": listen_token},
                    },
                    separators=(",", ":"),
                )
            )
            return

        params: dict[str, Any] = {
            "apiKey": self.api_key,
            "recvWindow": 5000,
            "timestamp": int(time.time() * 1000),
        }
        signing_payload = urlencode(sorted(params.items()), encoding="utf-8")
        params["signature"] = hmac_hex(signing_payload, self.secret_key)
        ws.send(
            json.dumps(
                {
                    "id": str(uuid.uuid4()),
                    "method": "userDataStream.subscribe.signature",
                    "params": params,
                },
                separators=(",", ":"),
            )
        )

    def create_binance_margin_listen_token(self) -> str:
        params: dict[str, Any] = {
            "isIsolated": "false",
            "validity": 86_400_000,
            "recvWindow": 5000,
            "timestamp": int(time.time() * 1000),
        }
        payload = urlencode(params, encoding="utf-8")
        request_body = f"{payload}&signature={hmac_hex(payload, self.secret_key)}"
        base_url = os.getenv("BINANCE_BASE_URL", "https://api.binance.com").rstrip(
            "/"
        )
        try:
            response = requests.post(
                f"{base_url}/sapi/v1/userListenToken",
                headers={
                    "X-MBX-APIKEY": self.api_key,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data=request_body,
                timeout=10,
            )
        except requests.RequestException as exc:
            raise RuntimeError(
                f"could not create Binance Cross Margin listen token: {exc}"
            ) from exc

        try:
            result = response.json()
        except ValueError as exc:
            raise RuntimeError(
                "Binance Cross Margin listen-token endpoint returned invalid JSON"
            ) from exc
        if not response.ok:
            code = result.get("code") if isinstance(result, dict) else None
            message = result.get("msg") if isinstance(result, dict) else None
            raise RuntimeError(
                "Binance Cross Margin listen-token request failed "
                f"(HTTP {response.status_code}, code={code}, message={message})"
            )
        token = result.get("token") if isinstance(result, dict) else None
        if not token:
            raise RuntimeError(
                "Binance Cross Margin listen-token response did not contain a token"
            )
        self.margin_token_expiration_ms = int_or_none(result.get("expirationTime"))
        return str(token)

    def send_okx_login(self, ws: websocket.WebSocketApp) -> None:
        timestamp = str(time.time())
        signature = hmac_base64(
            f"{timestamp}GET/users/self/verify", self.secret_key
        )
        ws.send(
            json.dumps(
                {
                    "op": "login",
                    "args": [
                        {
                            "apiKey": self.api_key,
                            "passphrase": self.passphrase,
                            "timestamp": timestamp,
                            "sign": signature,
                        }
                    ],
                },
                separators=(",", ":"),
            )
        )

    def send_okx_subscription(self, ws: websocket.WebSocketApp) -> None:
        channel: dict[str, str] = {"channel": "orders", "instType": "SPOT"}
        if self.instrument:
            channel["instId"] = self.instrument
        ws.send(
            json.dumps(
                {"id": str(uuid.uuid4()), "op": "subscribe", "args": [channel]},
                separators=(",", ":"),
            )
        )

    def send_crypto_auth(self, ws: websocket.WebSocketApp) -> None:
        request_id = int(time.time() * 1000)
        nonce = request_id
        method = "public/auth"
        signing_payload = f"{method}{request_id}{self.api_key}{nonce}"
        ws.send(
            json.dumps(
                {
                    "id": request_id,
                    "method": method,
                    "api_key": self.api_key,
                    "sig": hmac_hex(signing_payload, self.secret_key),
                    "nonce": nonce,
                },
                separators=(",", ":"),
            )
        )

    def send_crypto_subscription(self, ws: websocket.WebSocketApp) -> None:
        suffix = f".{self.instrument}" if self.instrument else ""
        request_id = int(time.time() * 1000)
        ws.send(
            json.dumps(
                {
                    "id": request_id,
                    "method": "subscribe",
                    "params": {
                        "channels": [f"user.order{suffix}", f"user.trade{suffix}"]
                    },
                    "nonce": request_id,
                },
                separators=(",", ":"),
            )
        )

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        if message == "pong":
            return
        try:
            payload = json.loads(message)
        except ValueError:
            print(f"invalid WebSocket JSON: {message}", file=sys.stderr, flush=True)
            return

        try:
            if self.exchange == "binance":
                self.handle_binance(payload)
            elif self.exchange == "okx":
                self.handle_okx(ws, payload)
            else:
                self.handle_crypto(ws, payload)
        except RuntimeError as exc:
            print(f"private stream request failed: {exc}", file=sys.stderr, flush=True)
            self.print_raw(payload)
            ws.close()
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            print(f"order event parsing failed: {exc}", file=sys.stderr, flush=True)
            self.print_raw(payload)

    def handle_binance(self, payload: dict[str, Any]) -> None:
        if "status" in payload and "id" in payload:
            if payload.get("status") != 200:
                error = payload.get("error")
                raise RuntimeError(
                    "Binance subscription failed "
                    f"(status={payload.get('status')}, error={error})"
                )
            self.authenticated = True
            print(
                f"subscribed to Binance {self.account_type} user data stream",
                file=sys.stderr,
                flush=True,
            )
            return

        record = payload.get("event", payload)
        if record.get("e") == "eventStreamTerminated":
            raise RuntimeError(
                f"Binance {self.account_type} user data stream expired"
            )
        if record.get("e") != "executionReport":
            return
        if self.instrument and record.get("s") != self.instrument:
            return
        self.print_raw(record)

        order_id = str(record.get("i", ""))
        status = str(record.get("X", "UNKNOWN")).upper()
        execution_type = str(record.get("x", ""))
        created_ms = int_or_none(record.get("O"))
        updated_ms = int_or_none(record.get("T"))
        event_elapsed = elapsed_ms(created_ms, updated_ms)
        cumulative_qty = record.get("z", "0")
        cumulative_quote = record.get("Z", "0")
        fee_asset = record.get("N")
        fee = record.get("n", "0")
        fee_key = (self.exchange, order_id)
        if execution_type == "TRADE" and fee_asset and Decimal(str(fee)) != 0:
            self.binance_fees[fee_key][str(fee_asset)] += Decimal(str(fee))

        details = {
            "exchange": "binance",
            "account_type": self.account_type,
            "symbol": record.get("s"),
            "order_id": order_id,
            "client_order_id": record.get("c"),
            "side": record.get("S"),
            "order_type": record.get("o"),
            "execution_type": execution_type,
            "status": status,
            "order_quantity": record.get("q"),
            "limit_price": record.get("p"),
            "last_fill_quantity": record.get("l"),
            "cumulative_fill_quantity": cumulative_qty,
            "last_fill_price": record.get("L"),
            "average_fill_price": decimal_divide(cumulative_quote, cumulative_qty),
            "cumulative_quote_quantity": cumulative_quote,
            "last_fill_fee": fee,
            "last_fill_fee_asset": fee_asset,
            "cumulative_fees": {
                asset: format(amount, "f")
                for asset, amount in self.binance_fees[fee_key].items()
            },
            "liquidity": (
                "MAKER" if record.get("m") else "TAKER"
                if execution_type == "TRADE"
                else None
            ),
            "trade_id": record.get("t"),
            "created_at_ms": created_ms,
            "updated_at_ms": updated_ms,
            "working_at_ms": int_or_none(record.get("W")),
            "exchange_elapsed_ms": event_elapsed,
            "fill_elapsed_ms": event_elapsed if status == "FILLED" else None,
            "stream_observed_elapsed_ms": self.observed_elapsed(order_id, status),
            "reject_reason": record.get("r"),
        }
        self.emit("order_update", details)
        if status in TERMINAL_STATES:
            self.binance_fees.pop(fee_key, None)

    def handle_okx(
        self, ws: websocket.WebSocketApp, payload: dict[str, Any]
    ) -> None:
        event = payload.get("event")
        if event == "login":
            if payload.get("code") != "0":
                raise RuntimeError(f"OKX login failed: {payload}")
            self.authenticated = True
            print("authenticated with OKX", file=sys.stderr, flush=True)
            self.send_okx_subscription(ws)
            return
        if event == "subscribe":
            print("subscribed to OKX orders channel", file=sys.stderr, flush=True)
            return
        if event == "error":
            raise RuntimeError(f"OKX WebSocket error: {payload}")

        for record in payload.get("data", []):
            if self.instrument and record.get("instId") != self.instrument:
                continue
            self.print_raw(record)
            order_id = str(record.get("ordId", ""))
            status = str(record.get("state", "UNKNOWN")).upper()
            created_ms = int_or_none(record.get("cTime"))
            fill_ms = int_or_none(record.get("fillTime"))
            updated_ms = int_or_none(record.get("uTime"))
            effective_update_ms = fill_ms or updated_ms
            event_elapsed = elapsed_ms(created_ms, effective_update_ms)
            exec_type = record.get("execType")
            self.emit(
                "order_update",
                {
                    "exchange": "okx",
                    "symbol": record.get("instId"),
                    "order_id": order_id,
                    "client_order_id": record.get("clOrdId"),
                    "side": record.get("side"),
                    "order_type": record.get("ordType"),
                    "status": status,
                    "order_quantity": record.get("sz"),
                    "limit_price": record.get("px"),
                    "last_fill_quantity": record.get("fillSz"),
                    "cumulative_fill_quantity": record.get("accFillSz"),
                    "last_fill_price": record.get("fillPx"),
                    "average_fill_price": record.get("avgPx"),
                    "last_fill_fee": record.get("fillFee"),
                    "last_fill_fee_asset": record.get("fillFeeCcy"),
                    "cumulative_fee": record.get("fee"),
                    "cumulative_fee_asset": record.get("feeCcy"),
                    "liquidity": (
                        "MAKER" if exec_type == "M" else "TAKER"
                        if exec_type == "T"
                        else None
                    ),
                    "trade_id": record.get("tradeId"),
                    "created_at_ms": created_ms,
                    "filled_at_ms": fill_ms,
                    "updated_at_ms": updated_ms,
                    "exchange_elapsed_ms": event_elapsed,
                    "fill_elapsed_ms": (
                        event_elapsed if status == "FILLED" else None
                    ),
                    "stream_observed_elapsed_ms": self.observed_elapsed(
                        order_id, status
                    ),
                    "cancel_source": record.get("cancelSource"),
                    "message": record.get("msg"),
                },
            )

    def handle_crypto(
        self, ws: websocket.WebSocketApp, payload: dict[str, Any]
    ) -> None:
        method = payload.get("method")
        if method == "public/heartbeat":
            ws.send(
                json.dumps(
                    {
                        "id": payload.get("id"),
                        "method": "public/respond-heartbeat",
                    },
                    separators=(",", ":"),
                )
            )
            return
        if method == "public/auth":
            if payload.get("code") != 0:
                raise RuntimeError(f"Crypto.com authentication failed: {payload}")
            self.authenticated = True
            print("authenticated with Crypto.com", file=sys.stderr, flush=True)
            self.send_crypto_subscription(ws)
            return
        if payload.get("code") not in (None, 0):
            raise RuntimeError(f"Crypto.com WebSocket error: {payload}")

        result = payload.get("result") or {}
        subscription = str(result.get("subscription", result.get("channel", "")))
        rows = result.get("data", [])
        if "user.trade" in subscription:
            for record in rows:
                if self.instrument and record.get("instrument_name") != self.instrument:
                    continue
                self.print_raw(record)
                self.emit(
                    "fill",
                    {
                        "exchange": "crypto_com",
                        "symbol": record.get("instrument_name"),
                        "order_id": str(record.get("order_id", "")),
                        "client_order_id": record.get("client_oid"),
                        "trade_id": record.get("trade_id"),
                        "trade_match_id": record.get("trade_match_id"),
                        "side": record.get("side"),
                        "fill_quantity": record.get("traded_quantity"),
                        "fill_price": record.get("traded_price"),
                        "fee": record.get("fees"),
                        "fee_asset": record.get("fee_instrument_name"),
                        "liquidity": record.get("taker_side"),
                        "filled_at_ms": int_or_none(record.get("create_time")),
                        "filled_at_ns": record.get("create_time_ns"),
                    },
                )
            return

        if "user.order" not in subscription:
            return
        for record in rows:
            if self.instrument and record.get("instrument_name") != self.instrument:
                continue
            self.print_raw(record)
            order_id = str(record.get("order_id", ""))
            exchange_status = str(record.get("status", "UNKNOWN")).upper()
            cumulative_qty = record.get("cumulative_quantity")
            status = (
                "PARTIALLY_FILLED"
                if exchange_status == "ACTIVE" and is_positive_decimal(cumulative_qty)
                else exchange_status
            )
            created_ms = int_or_none(record.get("create_time"))
            if created_ms is None:
                created_ns = int_or_none(record.get("create_time_ns"))
                created_ms = created_ns // 1_000_000 if created_ns else None
            updated_ms = int_or_none(record.get("update_time"))
            event_elapsed = elapsed_ms(created_ms, updated_ms)
            self.emit(
                "order_update",
                {
                    "exchange": "crypto_com",
                    "symbol": record.get("instrument_name"),
                    "order_id": order_id,
                    "client_order_id": record.get("client_oid"),
                    "side": record.get("side"),
                    "order_type": record.get("order_type"),
                    "time_in_force": record.get("time_in_force"),
                    "execution_instructions": record.get("exec_inst"),
                    "status": status,
                    "exchange_status": exchange_status,
                    "order_quantity": record.get("quantity"),
                    "limit_price": record.get("limit_price"),
                    "cumulative_fill_quantity": cumulative_qty,
                    "cumulative_fill_value": record.get("cumulative_value"),
                    "average_fill_price": record.get("avg_price"),
                    "cumulative_fee": record.get("cumulative_fee"),
                    "cumulative_fee_asset": record.get("fee_instrument_name"),
                    "created_at_ms": created_ms,
                    "created_at_ns": record.get("create_time_ns"),
                    "updated_at_ms": updated_ms,
                    "exchange_elapsed_ms": event_elapsed,
                    "fill_elapsed_ms": (
                        event_elapsed if status == "FILLED" else None
                    ),
                    "stream_observed_elapsed_ms": self.observed_elapsed(
                        order_id, status
                    ),
                },
            )

    @staticmethod
    def on_error(_ws: websocket.WebSocketApp, error: Any) -> None:
        print(f"WebSocket error: {error}", file=sys.stderr, flush=True)

    @staticmethod
    def on_close(
        _ws: websocket.WebSocketApp,
        status_code: int | None,
        message: str | None,
    ) -> None:
        print(
            f"WebSocket closed (status={status_code}, message={message})",
            file=sys.stderr,
            flush=True,
        )

    def start_okx_heartbeat(self, ws: websocket.WebSocketApp) -> None:
        def heartbeat() -> None:
            while ws.sock and ws.sock.connected:
                time.sleep(20)
                try:
                    ws.send("ping")
                except websocket.WebSocketException:
                    return

        threading.Thread(target=heartbeat, daemon=True).start()

    def run(self) -> None:
        while True:
            app = websocket.WebSocketApp(
                self.websocket_url(),
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            if self.exchange == "okx":
                original_open = app.on_open

                def okx_open(ws: websocket.WebSocketApp) -> None:
                    if original_open:
                        original_open(ws)
                    self.start_okx_heartbeat(ws)

                app.on_open = okx_open
            app.run_forever(ping_interval=20, ping_timeout=10)
            print(
                f"reconnecting in {self.reconnect_delay:g} seconds...",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(self.reconnect_delay)


def main() -> None:
    args = parse_args()
    monitor = OrderStreamMonitor(args)
    try:
        monitor.run()
    except KeyboardInterrupt:
        print("stopped", file=sys.stderr)
    finally:
        monitor.close()


if __name__ == "__main__":
    main()
