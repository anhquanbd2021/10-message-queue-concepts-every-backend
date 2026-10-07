# Queue Guarantees Lab — companion demo

Interactive lab for the article *The 10 Queue Concepts Split in Two — and
the Second Half Is What Breaks in Production*. A zero-dependency toy
broker — partitioned append-only log, per-group committed offsets,
delivery and attempt counters — that makes the five guarantee concepts
visible instead of theoretical.

Zero dependencies — Node 20+ only. The log, consumer-group, dedupe and
scenario modules are plain ES modules shared by the browser UI, the CLI
report, and the test suite.

## Four labs

| Lab | What it proves |
|---|---|
| **Delivery** | Crash lands between the handler's work and the offset commit → the tail is redelivered (at-least-once). A naive handler applies the crashed record's effect twice (11 charges for 10 events); an idempotent consumer dedupes on `partition:offset` and applies 10. |
| **DLQ** | A poison record fails `maxAttempts` deliveries, gets parked, and the records behind it proceed. `drainDlq` hands the parked record back — a DLQ nobody drains is a graveyard. |
| **Backpressure** | Producer 5/tick vs consumer 3/tick: queue depth climbs 2/tick, crosses the alert line at tick 7, hits capacity and blocks the producer. Stall the producer and the backlog still takes ~7 ticks to drain. |
| **Fan-out** | `billing` and `analytics` keep independent offsets — each sees every record. Crash billing mid-read: redelivery hits billing alone. Rewind billing and it replays history while analytics is untouched. Partition lanes show the ordering contract: same key, same lane, in order. |

## Run it

```text
npm start        # serve the lab on :3000
npm test         # log, delivery, DLQ, backpressure, fan-out, server
npm run report   # side-by-side report on the article's claims
npm run check    # both
```

## Examples

- `examples/payments.json` — 10 charge events for the crash/redelivery lab.
- `examples/jobs.json` — 8 jobs, `job-4` carries `CORRUPT-PAYLOAD`.
- `examples/scenarios.json` — parameters for every lab (crash point, max
  attempts, rates, capacity, ordering fixtures).

## Honest limits

- This is an in-memory **model of the mechanics** — not a broker. No
  network, no real protocol, no persistence, no competing consumers.
- One partition per lab topic keeps the stories readable; real topics
  shard across partitions (the ordering lab shows why).
- The "crash" is deterministic — injected between work and commit at a
  chosen record. Real crashes land anywhere, which is why the dedupe
  store has to exist for *every* record, not just suspicious ones.
- Backpressure here models one overflow policy (block the producer).
  Real systems also drop, spill to disk, or scale consumers — the depth
  math is the same, the blast radius isn't.
- The dedupe store is a `Set` in memory — real consumers need a durable
  store keyed on a business key with a retention window longer than the
  redelivery horizon.

This is an educational demo, not production infrastructure.
