# Verified public interfaces

Verified against official Polymarket documentation on 2026-08-05. These are the only remote
interfaces used at runtime.

## Gamma market discovery

- Base: `https://gamma-api.polymarket.com`
- Direct event lookup: `GET /events/slug/{slug}`
- Fallback discovery: `GET /events/keyset` with public, read-only filters
- Retry: bounded exponential backoff on HTTP 429 and 5xx responses (up to 3 retries)

Official references:

- [Discover markets](https://docs.polymarket.com/market-data/discover-markets)
- [Get event by slug](https://docs.polymarket.com/api-reference/events/get-event-by-slug)
- [List events with keyset pagination](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination)

Automatic discovery tries the observed current-interval slug convention for the current, preceding,
and following five-minute boundaries. This is only a fast candidate generator. If no candidate
validates, the recorder queries the official keyset event listing for open Bitcoin Up/Down events
ending near the current time, then applies the same full validation.

## Public CLOB market channel

- Endpoint: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Subscription:

```json
{
  "assets_ids": ["<up-token>", "<down-token>"],
  "type": "market",
  "custom_feature_enabled": true
}
```

Official reference:
[Market channel](https://docs.polymarket.com/market-data/websocket/market-channel).

The recorder retains documented `book`, `price_change`, `tick_size_change`, `last_trade_price`,
`best_bid_ask`, `new_market`, and `market_resolved` frames. Unknown frames are also stored if they
have the required `event_type` boundary.

## Settlement-aligned BTC reference stream

- Endpoint: `wss://ws-live-data.polymarket.com`
- Public RTDS topic: `crypto_prices_chainlink`
- Symbol filter: `btc/usd`
- Message type: `*`
- Keepalive: text `PING` every five seconds

Subscription:

```json
{
  "action": "subscribe",
  "subscriptions": [
    {
      "topic": "crypto_prices_chainlink",
      "type": "*",
      "filters": "{\"symbol\":\"btc/usd\"}"
    }
  ]
}
```

Official reference: [Real-Time Data Socket](https://docs.polymarket.com/market-data/websocket/rtds).

No Binance or secondary exchange stream is substituted for the Chainlink source.

## Explicitly unused interfaces

The recorder does not call the user channel, authenticated CLOB routes, trading REST routes, relayer
routes, wallet APIs, private profile/position routes, or any unofficial scraper.
