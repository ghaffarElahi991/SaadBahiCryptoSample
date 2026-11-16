# Multi-Exchange Post-Only Order Scripts

Python command-line tools for placing Spot orders on Binance, OKX, and Crypto.com. Orders can use a fixed price, fetch the latest bid/ask over REST, or read continuously updated WebSocket prices from Redis.

> **Warning:** Commands without `--test-order` or `--demo` can place real orders and use real funds. Confirm the symbol, side, quantity, price source, and account balance before running them.

## Features

- Binance, OKX, and Crypto.com authenticated order submission
- Post-only/maker-only orders on all three exchanges
- Fixed, REST, and Redis-backed WebSocket pricing
- Selectable ask or bid reference with a signed price offset
- Order request latency printed in milliseconds
- Automatic rejection of stale WebSocket prices
- Binance order status, open-order, history, and balance queries
- Credentials loaded from `.env`

## Project files

| File | Purpose |
| --- | --- |
| `binance.py` | Place Binance Spot orders |
| `okx.py` | Place OKX orders |
| `crypto_com.py` | Place Crypto.com Exchange orders |
| `websocket_cache.py` | Stream best bid/ask prices into Redis |
| `market_data.py` | Shared REST, Redis, and price-calculation logic |
| `binance_orders.py` | Read Binance orders and balances |
| `.env` | API credentials and Redis URL |
| `requirment.txt` | Python dependencies |

## Setup

Create and activate the project virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the dependencies:

```bash
python -m pip install -r requirment.txt
```

The IDE interpreter is:

```text
<project-directory>/.venv/bin/python
```

## Credentials

Put the API credentials in `.env`:

```dotenv
# Binance
BINANCE_API_KEY=
BINANCE_API_SECRET=

# OKX
OKX_API_KEY=
OKX_API_SECRET=
OKX_API_PASSPHRASE=

# Crypto.com Exchange
CRYPTO_COM_API_KEY=
CRYPTO_COM_API_SECRET=

# WebSocket quote cache
REDIS_URL=redis://localhost:6379/0
```

Keep `.env` private. It is excluded by `.gitignore` and should have restricted permissions:

```bash
chmod 600 .env
```

API keys need trading permission to place orders. Withdrawal permission is not required and should remain disabled.

## Price modes

### Fixed price

Provide the exact limit price with `--price`:

```bash
python binance.py BTCUSDT BUY 0.0002 \
  --post-only \
  --price 60000
```

### REST price

`--price-mode rest` fetches the latest best bid and ask immediately before sending the order:

```bash
python binance.py BTCUSDT BUY 0.0002 \
  --post-only \
  --price-mode rest \
  --book-side bid \
  --price-offset 0 \
  --test-order
```

### WebSocket price

`--price-mode websocket` reads a fresh quote saved in Redis by `websocket_cache.py`. The collector must already be running.

```bash
python binance.py BTCUSDT BUY 0.0002 \
  --post-only \
  --price-mode websocket \
  --book-side bid \
  --price-offset 0 \
  --test-order
```

The default maximum cached-price age is two seconds. Change it when needed:

```bash
--max-price-age 5
```

## Ask, bid, and price offsets

The calculated order price is:

```text
selected ask or bid + price offset
```

- Buys default to the bid.
- Sells default to the ask.
- `--book-side bid` or `--book-side ask` overrides the default.
- A positive offset increases the price.
- A negative offset decreases the price.

Examples:

```bash
# Add 0.10 to the bid
--book-side bid --price-offset 0.10

# Subtract 0.10 from the ask
--book-side ask --price-offset=-0.10
```

A post-only order that would immediately trade as a taker can be rejected or canceled by the exchange.

## Running the WebSocket price cache

Redis must be running first:

```bash
redis-cli ping
```

A working server returns:

```text
PONG
```

Run the collector in terminal 1 and leave it open. Use `--print-updates` to print every updated price while also saving it to Redis.

### Binance

```bash
python websocket_cache.py binance BTCUSDT --print-updates
```

### OKX

```bash
python websocket_cache.py okx BTC-USDT --print-updates
```

### Crypto.com

```bash
python websocket_cache.py crypto_com BTC_USDT --print-updates
```

Stop a collector with `Ctrl+C`. It automatically reconnects after a connection failure.

To run all three collectors in one terminal:

```bash
python websocket_cache.py binance BTCUSDT --print-updates &
python websocket_cache.py okx BTC-USDT --print-updates &
python websocket_cache.py crypto_com BTC_USDT --print-updates &
wait
```

Inspect cached Binance data from another terminal:

```bash
redis-cli HGETALL market:quote:binance:BTCUSDT
```

Watch the value change:

```bash
watch -n 0.5 'redis-cli HGETALL market:quote:binance:BTCUSDT'
```

Redis keys use this format:

```text
market:quote:<exchange>:<instrument>
```

The collector expires its key after 30 seconds without an update. The order scripts reject data older than two seconds by default.

## Binance commands

### Test a post-only buy using REST

Binance `/order/test` validates the order without placing it. A successful response is `{}`.

```bash
python binance.py BTCUSDT BUY 0.0002 \
  --post-only \
  --price-mode rest \
  --book-side bid \
  --price-offset 0 \
  --test-order
```

### Live post-only buy using WebSocket pricing

```bash
python binance.py BTCUSDT BUY 0.0002 \
  --post-only \
  --price-mode websocket \
  --book-side bid \
  --price-offset 0
```

### Test a post-only sell

Use an amount no larger than the free BTC balance:

```bash
python binance.py BTCUSDT SELL 0.00019 \
  --post-only \
  --price-mode websocket \
  --book-side ask \
  --price-offset 0 \
  --test-order
```

Remove `--test-order` to submit that sell as a live order.

### Market order

Market orders cannot be post-only and do not accept a price mode:

```bash
python binance.py BTCUSDT BUY 0.0002 --type MARKET
```

### Binance order and balance queries

Check one order:

```bash
python binance_orders.py status BTCUSDT ORDER_ID
```

Example:

```bash
python binance_orders.py status BTCUSDT 68637324532
```

Show current open orders:

```bash
python binance_orders.py open BTCUSDT
```

Show recent order history:

```bash
python binance_orders.py history BTCUSDT --limit 20
```

Show the free and locked BTC balance:

```bash
python binance_orders.py balance BTC
```

Trading fees can make the free balance smaller than the original purchased quantity. Always use the `free` balance and an exchange-valid quantity step when selling.

## OKX commands

OKX symbols use hyphens, such as `BTC-USDT`, and sides are lowercase.

### Post-only buy using REST pricing

```bash
python okx.py BTC-USDT buy 0.0002 \
  --post-only \
  --price-mode rest \
  --book-side bid \
  --price-offset 0
```

### Post-only sell using WebSocket pricing

```bash
python okx.py BTC-USDT sell 0.0002 \
  --post-only \
  --price-mode websocket \
  --book-side ask \
  --price-offset 0
```

### Demo Trading

Use API credentials created inside OKX Demo Trading:

```bash
python okx.py BTC-USDT buy 0.0002 \
  --post-only \
  --price-mode rest \
  --demo
```

### OKX market buy using quote currency

This example uses `10` units of the quote currency:

```bash
python okx.py BTC-USDT buy 10 \
  --type market \
  --target-currency quote_ccy
```

Additional OKX options include:

```text
--trade-mode cash|cross|isolated|spot_isolated
--position-side long|short|net
--reduce-only
--client-order-id ID
```

## Crypto.com commands

Crypto.com symbols use underscores, such as `BTC_USDT`, and sides are uppercase.

### Post-only buy using REST pricing

```bash
python crypto_com.py BTC_USDT BUY 0.0002 \
  --post-only \
  --price-mode rest \
  --book-side bid \
  --price-offset 0
```

### Post-only sell using WebSocket pricing

```bash
python crypto_com.py BTC_USDT SELL 0.0002 \
  --post-only \
  --price-mode websocket \
  --book-side ask \
  --price-offset 0
```

### Fixed-price post-only order

```bash
python crypto_com.py BTC_USDT BUY 0.0002 \
  --post-only \
  --price 60000
```

Additional Crypto.com options include:

```text
--time-in-force GOOD_TILL_CANCEL|IMMEDIATE_OR_CANCEL|FILL_OR_KILL
--spot-margin SPOT|MARGIN
--client-order-id ID
```

## Latency output

Every order script prints client-side request latency:

```text
price source=websocket bid=64699.10000000 offset=0 order_price=64699.10000000
order_latency_ms=362.307
{}
```

`order_latency_ms` measures the authenticated HTTP order request from immediately before sending until the response arrives. It does not measure time until the order fills.

## Useful help commands

```bash
python binance.py --help
python okx.py --help
python crypto_com.py --help
python websocket_cache.py --help
python binance_orders.py --help
```

## Common errors

### `Account has insufficient balance for requested action`

Check the free asset balance. Fees may have been charged in the purchased asset:

```bash
python binance_orders.py balance BTC
```

### `no WebSocket quote found`

Start the matching collector and wait until it prints its first cached bid and ask.

### `WebSocket quote is stale`

Confirm the collector is still connected and Redis timestamps are changing. The default maximum age is two seconds.

### Redis connection failure

Confirm that `REDIS_URL` in `.env` is correct and that this returns `PONG`:

```bash
redis-cli -u redis://localhost:6379/0 ping
```

### Post-only order does not fill immediately

This is expected. A post-only order must rest on the order book as a maker. It can remain open until another participant trades against it.

### Check an order before selling

An order-creation acknowledgment does not prove that the buy filled. Confirm that its status is `FILLED`, then check the free balance before selling:

```bash
python binance_orders.py status BTCUSDT ORDER_ID
python binance_orders.py balance BTC
```
# SaadBahiCryptoSample
