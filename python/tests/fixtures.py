"""Test helpers: build synthetic schema-v2 run directories."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def base_market() -> dict:
    return {
        "schemaVersion": 2,
        "eventId": "event-100",
        "marketId": "market-200",
        "conditionId": "0xcondition",
        "slug": "btc-updown-5m-1785909600",
        "question": "Bitcoin Up or Down - August 5, 6:00AM-6:05AM ET",
        "opensAt": "2026-08-05T06:00:00.000Z",
        "closesAt": "2026-08-05T06:05:00.000Z",
        "resolutionSource": "https://data.chain.link/streams/btc-usd",
        "referenceSymbol": "btc/usd",
        "referenceTopic": "crypto_prices_chainlink",
        "upTokenId": "111111",
        "downTokenId": "222222",
        "tickSize": "0.01",
        "active": True,
        "closed": False,
        "rawMetadata": {
            "id": "event-100",
            "slug": "btc-updown-5m-1785909600",
            "active": True,
            "closed": False,
        },
        "rawGammaText": '{"id":"event-100","slug":"btc-updown-5m-1785909600","active":true,"closed":false}',
    }


def base_manifest(market: dict | None = None, **overrides: Any) -> dict:
    m = market or base_market()
    manifest = {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "startedAt": "2026-08-05T06:02:00.000Z",
        "endedAt": "2026-08-05T06:02:01.000Z",
        "recorderVersion": "0.2.0",
        "gitCommit": None,
        "market": m,
        "commandArguments": {
            "durationSeconds": 1,
            "outputDir": "fixtures",
            "marketSlug": m["slug"],
            "logLevel": "error",
        },
        "outputFiles": ["manifest.json", "market.json", "events.jsonl", "health.json"],
        "completionStatus": "COMPLETED",
        "finalEventCount": None,
        "failureReason": None,
        "captureStatus": "COMPLETE",
        "usableForReplay": True,
        "qualityFailures": [],
        "clobStaleAfterMs": 15000,
        "referenceStaleAfterMs": 10000,
    }
    manifest.update(overrides)
    return manifest


def base_health(books: dict | None = None, **overrides: Any) -> dict:
    if books is None:
        books = {
            "111111": {
                "tokenId": "111111",
                "state": "SYNCHRONIZED",
                "integrity": "NORMAL",
                "crossedIncidentCount": 0,
                "synchronizationEpoch": 0,
                "snapshotConnectionGeneration": 1,
                "bestBid": ".48",
                "bestAsk": ".52",
                "bidDepthCount": 1,
                "askDepthCount": 1,
                "lastSuccessfulUpdateTime": 1785909720106,
                "healthy": True,
            },
            "222222": {
                "tokenId": "222222",
                "state": "SYNCHRONIZED",
                "integrity": "NORMAL",
                "crossedIncidentCount": 0,
                "synchronizationEpoch": 0,
                "snapshotConnectionGeneration": 1,
                "bestBid": ".47",
                "bestAsk": ".53",
                "bidDepthCount": 1,
                "askDepthCount": 1,
                "lastSuccessfulUpdateTime": 1785909720116,
                "healthy": True,
            },
        }
    health = {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "startedAt": "2026-08-05T06:02:00.000Z",
        "endedAt": "2026-08-05T06:02:01.000Z",
        "totalEventsReceived": 4,
        "eventCountsBySource": {
            "POLYMARKET_MARKET": 1,
            "POLYMARKET_CLOB": 2,
            "POLYMARKET_REFERENCE_PRICE": 1,
        },
        "eventCountsByType": {
            "POLYMARKET_MARKET:MARKET_DISCOVERED": 1,
            "POLYMARKET_CLOB:book": 2,
            "POLYMARKET_REFERENCE_PRICE:REFERENCE_PRICE": 1,
        },
        "malformedEventCount": 0,
        "duplicateEventCount": 0,
        "reconnectCount": 0,
        "maximumSourceToReceiveLatencyMs": 5,
        "maximumReceiveToProcessLatencyMs": 1,
        "staleFeedIncidents": 0,
        "bothTokenStreamsObserved": True,
        "referencePriceStreamObserved": True,
        "finalBookHealth": books,
        "openGapsByFeed": {},
        "dataGapCount": 0,
        "closedGapCount": 0,
        "totalGapDurationMs": 0,
        "maximumGapDurationMs": 0,
        "captureStatus": "COMPLETE",
        "usableForReplay": True,
        "qualityFailures": [],
        "clobStaleAfterMs": 15000,
        "referenceStaleAfterMs": 10000,
        "lastReferenceReceivedAt": 1785909720125,
        "lastReferenceConnectionGeneration": 1,
        "lastReferenceSourceTimestamp": 1785909720120,
        "finalReferenceAgeMs": 1,
        "finalReferenceSourceAgeMs": 6,
        "referenceFresh": True,
        "referenceClockSkewMs": 5000,
        "referenceTimestampAnomalyCount": 0,
        "referenceTimestampMissingCount": 0,
        "referenceStaleAtReceiptCount": 0,
        "referenceFutureSourceSkewCount": 0,
    }
    health.update(overrides)
    return health


def market_discovered(seq: int = 1) -> dict:
    return {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "sequence": seq,
        "source": "POLYMARKET_MARKET",
        "eventType": "MARKET_DISCOVERED",
        "marketId": "market-200",
        "conditionId": "0xcondition",
        "sourceMarketId": None,
        "tokenId": None,
        "observedTokenIds": None,
        "sourceTimestamp": None,
        "topLevelSourceTimestamp": None,
        "payloadSourceTimestamp": None,
        "receivedAt": 1785909720000,
        "processedAt": 1785909720001,
        "connectionGeneration": None,
        "frameId": None,
        "messageIndex": None,
        "messageCount": None,
        "isDuplicate": False,
        "dedupeState": "UNIQUE_APPLIED",
        "duplicateOfSequence": None,
        "dedupeReason": None,
        "payload": {
            "id": "event-100",
            "slug": "btc-updown-5m-1785909600",
            "active": True,
            "closed": False,
        },
        "rawText": '{"id":"event-100","slug":"btc-updown-5m-1785909600","active":true,"closed":false}',
        "rawMessageText": None,
    }


def book_event(
    seq: int,
    token_id: str,
    bids: list,
    asks: list,
    ts: int,
    gen: int = 1,
    frame: str | None = None,
    processed_at: int | None = None,
) -> dict:
    payload = {
        "event_type": "book",
        "asset_id": token_id,
        "market": "0xcondition",
        "bids": bids,
        "asks": asks,
        "timestamp": str(ts),
    }
    raw = json.dumps(payload)
    return {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "sequence": seq,
        "source": "POLYMARKET_CLOB",
        "eventType": "book",
        "marketId": "market-200",
        "conditionId": "0xcondition",
        "sourceMarketId": "0xcondition",
        "tokenId": token_id,
        "observedTokenIds": None,
        "sourceTimestamp": ts,
        "topLevelSourceTimestamp": None,
        "payloadSourceTimestamp": None,
        "receivedAt": ts + 5,
        "processedAt": processed_at if processed_at is not None else ts + 6,
        "connectionGeneration": gen,
        "frameId": frame or f"frame-{seq}",
        "messageIndex": 0,
        "messageCount": 1,
        "isDuplicate": False,
        "dedupeState": "UNIQUE_APPLIED",
        "duplicateOfSequence": None,
        "dedupeReason": None,
        "payload": payload,
        "rawText": raw,
        "rawMessageText": raw,
    }


def price_change_event(
    seq: int, token_id: str, changes: list, ts: int, gen: int = 1, processed_at: int | None = None
) -> dict:
    payload = {
        "event_type": "price_change",
        "asset_id": token_id,
        "market": "0xcondition",
        "price_changes": changes,
        "timestamp": str(ts),
    }
    raw = json.dumps(payload)
    return {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "sequence": seq,
        "source": "POLYMARKET_CLOB",
        "eventType": "price_change",
        "marketId": "market-200",
        "conditionId": "0xcondition",
        "sourceMarketId": "0xcondition",
        "tokenId": token_id,
        "observedTokenIds": None,
        "sourceTimestamp": ts,
        "topLevelSourceTimestamp": None,
        "payloadSourceTimestamp": None,
        "receivedAt": ts + 5,
        "processedAt": processed_at if processed_at is not None else ts + 6,
        "connectionGeneration": gen,
        "frameId": f"frame-{seq}",
        "messageIndex": 0,
        "messageCount": 1,
        "isDuplicate": False,
        "dedupeState": "UNIQUE_APPLIED",
        "duplicateOfSequence": None,
        "dedupeReason": None,
        "payload": payload,
        "rawText": raw,
        "rawMessageText": raw,
    }


def reference_event(
    seq: int,
    value: str = "67234.5000000000000001",
    ts: int = 1785909720120,
    payload_ts: int | None = None,
    processed_at: int | None = None,
) -> dict:
    pts = payload_ts if payload_ts is not None else ts
    payload = {
        "topic": "crypto_prices_chainlink",
        "type": "update",
        "timestamp": ts,
        "payload": {"symbol": "btc/usd", "timestamp": pts, "value": value},
    }
    raw = json.dumps(payload)
    return {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "sequence": seq,
        "source": "POLYMARKET_REFERENCE_PRICE",
        "eventType": "REFERENCE_PRICE",
        "marketId": "market-200",
        "conditionId": "0xcondition",
        "sourceMarketId": None,
        "tokenId": None,
        "observedTokenIds": None,
        "sourceTimestamp": ts,
        "topLevelSourceTimestamp": ts,
        "payloadSourceTimestamp": pts,
        "receivedAt": ts + 5,
        "processedAt": processed_at if processed_at is not None else ts + 6,
        "connectionGeneration": 1,
        "frameId": f"frame-{seq}",
        "messageIndex": 0,
        "messageCount": 1,
        "isDuplicate": False,
        "dedupeState": "UNIQUE_APPLIED",
        "duplicateOfSequence": None,
        "dedupeReason": None,
        "payload": payload,
        "rawText": raw,
        "rawMessageText": raw,
    }


def write_run(
    directory: Path,
    manifest: dict,
    health: dict,
    events: list[dict],
    market: dict | None = None,
    write_market_file: bool = True,
) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (directory / "health.json").write_text(json.dumps(health), encoding="utf-8")
    if write_market_file and market is not None:
        (directory / "market.json").write_text(
            json.dumps({"schemaVersion": 2, "market": market}), encoding="utf-8"
        )
    with (directory / "events.jsonl").open("w", encoding="utf-8") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")
    return directory


def sample_events() -> list[dict]:
    return [
        market_discovered(1),
        book_event(
            2,
            "111111",
            [{"price": ".48", "size": "30"}],
            [{"price": ".52", "size": "25"}],
            1785909720100,
        ),
        book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720110,
        ),
        reference_event(4),
    ]


def write_sample_run(directory: Path) -> Path:
    m = base_market()
    manifest = base_manifest(m, finalEventCount=4)
    health = base_health()
    return write_run(directory, manifest, health, sample_events(), market=m)


def trade_event(
    seq: int,
    token_id: str,
    price: str,
    size: str,
    aggressor: str,
    ts: int,
    gen: int = 1,
) -> dict:
    payload = {
        "event_type": "price_change",
        "asset_id": token_id,
        "market": "0xcondition",
        "trades": [{"price": price, "size": size, "side": aggressor}],
        "timestamp": str(ts),
    }
    raw = json.dumps(payload)
    return {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "sequence": seq,
        "source": "POLYMARKET_CLOB",
        "eventType": "trade",
        "marketId": "market-200",
        "conditionId": "0xcondition",
        "sourceMarketId": "0xcondition",
        "tokenId": token_id,
        "observedTokenIds": None,
        "sourceTimestamp": ts,
        "topLevelSourceTimestamp": None,
        "payloadSourceTimestamp": None,
        "receivedAt": ts + 5,
        "processedAt": ts + 6,
        "connectionGeneration": gen,
        "frameId": f"frame-{seq}",
        "messageIndex": 0,
        "messageCount": 1,
        "isDuplicate": False,
        "dedupeState": "UNIQUE_APPLIED",
        "duplicateOfSequence": None,
        "dedupeReason": None,
        "payload": payload,
        "rawText": raw,
        "rawMessageText": raw,
    }


def lifecycle_event(
    seq: int,
    kind: str,
    ts: int,
    gen: int = 1,
    detail: str = "",
) -> dict:
    return {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "sequence": seq,
        "source": "POLYMARKET_CLOB",
        "eventType": kind,
        "marketId": "market-200",
        "conditionId": "0xcondition",
        "sourceMarketId": "0xcondition",
        "tokenId": None,
        "observedTokenIds": None,
        "sourceTimestamp": ts,
        "topLevelSourceTimestamp": None,
        "payloadSourceTimestamp": None,
        "receivedAt": ts + 5,
        "processedAt": ts + 6,
        "connectionGeneration": gen,
        "frameId": f"frame-{seq}",
        "messageIndex": 0,
        "messageCount": 1,
        "isDuplicate": False,
        "dedupeState": "UNIQUE_APPLIED",
        "duplicateOfSequence": None,
        "dedupeReason": detail,
        "payload": {"event_type": kind, "detail": detail},
        "rawText": json.dumps({"event_type": kind, "detail": detail}),
        "rawMessageText": json.dumps({"event_type": kind, "detail": detail}),
    }


def multi_token_price_change_event(
    seq: int,
    changes: list[dict],
    ts: int,
    gen: int = 1,
) -> dict:
    """A price_change event with multiple token changes and null envelope tokenId."""
    payload = {
        "event_type": "price_change",
        "asset_id": None,
        "market": "0xcondition",
        "price_changes": changes,
        "timestamp": str(ts),
    }
    raw = json.dumps(payload)
    return {
        "schemaVersion": 2,
        "runId": "fixture-run-001",
        "sequence": seq,
        "source": "POLYMARKET_CLOB",
        "eventType": "price_change",
        "marketId": "market-200",
        "conditionId": "0xcondition",
        "sourceMarketId": "0xcondition",
        "tokenId": None,
        "observedTokenIds": None,
        "sourceTimestamp": ts,
        "topLevelSourceTimestamp": None,
        "payloadSourceTimestamp": None,
        "receivedAt": ts + 5,
        "processedAt": ts + 6,
        "connectionGeneration": gen,
        "frameId": f"frame-{seq}",
        "messageIndex": 0,
        "messageCount": 1,
        "isDuplicate": False,
        "dedupeState": "UNIQUE_APPLIED",
        "duplicateOfSequence": None,
        "dedupeReason": None,
        "payload": payload,
        "rawText": raw,
        "rawMessageText": raw,
    }


def realistic_events() -> list[dict]:
    """A realistic event stream with snapshots, deltas, trades, lifecycle, multi-token changes.

    Includes:
    - Market discovery
    - Initial snapshots for both tokens (same source timestamp = same-time conflict)
    - A price_change delta on UP token
    - A trade on DOWN token
    - A multi-token price_change (envelope tokenId=null, per-change asset_id)
    - A lifecycle RECONNECT event
    - A second reference price observation
    - Final snapshots reflecting the changes
    """
    base_ts = 1785909720100
    return [
        market_discovered(1),
        # Both token snapshots at the same source timestamp (same-time conflict).
        book_event(
            2, "111111", [{"price": ".48", "size": "30"}], [{"price": ".52", "size": "25"}], base_ts
        ),
        book_event(
            3, "222222", [{"price": ".47", "size": "20"}], [{"price": ".53", "size": "22"}], base_ts
        ),
        # Price change delta on UP token.
        price_change_event(
            4, "111111", [{"price": ".49", "size": "10", "side": "BUY"}], base_ts + 100
        ),
        # Trade on DOWN token.
        trade_event(5, "222222", ".48", "5", "BUY", base_ts + 200),
        # Multi-token price_change: envelope tokenId=null, each change has its own asset_id.
        multi_token_price_change_event(
            6,
            [
                {"price": ".50", "size": "15", "side": "BUY", "asset_id": "111111"},
                {"price": ".49", "size": "12", "side": "BUY", "asset_id": "222222"},
            ],
            base_ts + 300,
        ),
        # Lifecycle: reconnect scheduled event (real recorder event name).
        lifecycle_event(
            7,
            "RECONNECT_SCHEDULED",
            base_ts + 400,
            gen=2,
            detail="WebSocket closed, reconnect scheduled",
        ),
        # Second reference price observation.
        reference_event(8, value="67235.1", ts=base_ts + 500),
        # Updated snapshots after deltas.
        book_event(
            9,
            "111111",
            [{"price": ".50", "size": "15"}],
            [{"price": ".51", "size": "20"}],
            base_ts + 600,
        ),
        book_event(
            10,
            "222222",
            [{"price": ".49", "size": "12"}],
            [{"price": ".54", "size": "18"}],
            base_ts + 700,
        ),
    ]


def realistic_health() -> dict:
    """Health summary matching the realistic_events stream."""
    books = {
        "111111": {
            "tokenId": "111111",
            "state": "SYNCHRONIZED",
            "integrity": "NORMAL",
            "crossedIncidentCount": 0,
            "synchronizationEpoch": 0,
            "snapshotConnectionGeneration": 2,
            "bestBid": ".50",
            "bestAsk": ".51",
            "bidDepthCount": 1,
            "askDepthCount": 1,
            "lastSuccessfulUpdateTime": 1785909720806,
            "healthy": True,
        },
        "222222": {
            "tokenId": "222222",
            "state": "SYNCHRONIZED",
            "integrity": "NORMAL",
            "crossedIncidentCount": 0,
            "synchronizationEpoch": 0,
            "snapshotConnectionGeneration": 2,
            "bestBid": ".49",
            "bestAsk": ".54",
            "bidDepthCount": 1,
            "askDepthCount": 1,
            "lastSuccessfulUpdateTime": 1785909720806,
            "healthy": True,
        },
    }
    health = base_health(books=books)
    health["totalEventsReceived"] = 10
    health["eventCountsBySource"] = {
        "POLYMARKET_MARKET": 1,
        "POLYMARKET_CLOB": 8,
        "POLYMARKET_REFERENCE_PRICE": 1,
    }
    health["eventCountsByType"] = {
        "POLYMARKET_MARKET:MARKET_DISCOVERED": 1,
        "POLYMARKET_CLOB:book": 4,
        "POLYMARKET_CLOB:price_change": 2,
        "POLYMARKET_CLOB:trade": 1,
        "POLYMARKET_CLOB:RECONNECT_SCHEDULED": 1,
        "POLYMARKET_REFERENCE_PRICE:REFERENCE_PRICE": 1,
    }
    health["reconnectCount"] = 1
    health["lastReferenceReceivedAt"] = 1785909720605
    health["lastReferenceSourceTimestamp"] = 1785909720600
    health["finalReferenceAgeMs"] = 401
    health["finalReferenceSourceAgeMs"] = 406
    return health


def write_realistic_run(directory: Path) -> Path:
    m = base_market()
    manifest = base_manifest(m, finalEventCount=10)
    health = realistic_health()
    return write_run(directory, manifest, health, realistic_events(), market=m)


def regressing_timestamp_events() -> list[dict]:
    """Events where source timestamps deliberately regress to prove recorder-order delivery.

    The BacktestNode must deliver events in (sourceSequence, childIndex) order,
    not source-timestamp order. This fixture has:
    - seq 2: snapshot at ts=200 (late arrival)
    - seq 3: snapshot at ts=100 (earlier timestamp, but later sequence)
    - seq 4: delta at ts=150 (between the two timestamps)
    - seq 5: reference at ts=180
    If the engine sorts by ts_event, the order would be 3,4,5,2 — wrong.
    If it sorts by sequence, the order is 2,3,4,5 — correct.
    """
    return [
        market_discovered(1),
        # seq 2: UP snapshot at ts=200 (late timestamp), processedAt=106 (monotonic)
        book_event(
            2,
            "111111",
            [{"price": ".48", "size": "30"}],
            [{"price": ".52", "size": "25"}],
            1785909720200,
            processed_at=1785909720106,
        ),
        # seq 3: DOWN snapshot at ts=100 (earlier timestamp, but later sequence), processedAt=116
        book_event(
            3,
            "222222",
            [{"price": ".47", "size": "20"}],
            [{"price": ".53", "size": "22"}],
            1785909720100,
            processed_at=1785909720116,
        ),
        # seq 4: UP delta at ts=150 (between the two snapshot timestamps), processedAt=126
        price_change_event(
            4,
            "111111",
            [{"price": ".49", "size": "10", "side": "BUY"}],
            1785909720150,
            processed_at=1785909720126,
        ),
        # seq 5: reference at ts=180, processedAt=136
        reference_event(5, value="67234.5", ts=1785909720180, processed_at=1785909720136),
    ]


def regressing_timestamp_health() -> dict:
    books = {
        "111111": {
            "tokenId": "111111",
            "state": "SYNCHRONIZED",
            "integrity": "NORMAL",
            "crossedIncidentCount": 0,
            "synchronizationEpoch": 0,
            "snapshotConnectionGeneration": 1,
            "bestBid": ".49",
            "bestAsk": ".52",
            "bidDepthCount": 2,
            "askDepthCount": 1,
            "lastSuccessfulUpdateTime": 1785909720156,
            "healthy": True,
        },
        "222222": {
            "tokenId": "222222",
            "state": "SYNCHRONIZED",
            "integrity": "NORMAL",
            "crossedIncidentCount": 0,
            "synchronizationEpoch": 0,
            "snapshotConnectionGeneration": 1,
            "bestBid": ".47",
            "bestAsk": ".53",
            "bidDepthCount": 1,
            "askDepthCount": 1,
            "lastSuccessfulUpdateTime": 1785909720106,
            "healthy": True,
        },
    }
    health = base_health(books=books)
    health["totalEventsReceived"] = 5
    health["eventCountsBySource"] = {
        "POLYMARKET_MARKET": 1,
        "POLYMARKET_CLOB": 3,
        "POLYMARKET_REFERENCE_PRICE": 1,
    }
    health["eventCountsByType"] = {
        "POLYMARKET_MARKET:MARKET_DISCOVERED": 1,
        "POLYMARKET_CLOB:book": 2,
        "POLYMARKET_CLOB:price_change": 1,
        "POLYMARKET_REFERENCE_PRICE:REFERENCE_PRICE": 1,
    }
    health["lastReferenceReceivedAt"] = 1785909720185
    health["lastReferenceSourceTimestamp"] = 1785909720180
    health["finalReferenceAgeMs"] = 1
    health["finalReferenceSourceAgeMs"] = 6
    return health


def write_regressing_timestamp_run(directory: Path) -> Path:
    m = base_market()
    manifest = base_manifest(m, finalEventCount=5)
    health = regressing_timestamp_health()
    return write_run(directory, manifest, health, regressing_timestamp_events(), market=m)
