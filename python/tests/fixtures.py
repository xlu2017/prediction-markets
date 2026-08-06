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
    seq: int, token_id: str, bids: list, asks: list, ts: int, gen: int = 1, frame: str | None = None
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
        "processedAt": ts + 6,
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


def price_change_event(seq: int, token_id: str, changes: list, ts: int, gen: int = 1) -> dict:
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


def reference_event(
    seq: int,
    value: str = "67234.5000000000000001",
    ts: int = 1785909720120,
    payload_ts: int | None = None,
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
        "processedAt": ts + 6,
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
