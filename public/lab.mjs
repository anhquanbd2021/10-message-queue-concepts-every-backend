// lab.mjs — scenario runners. Each function turns one guarantee concept
// into a measurable outcome so the article's claims are assertions, not
// metaphors.

import { createBroker, createTopic, produce, keyOrder } from './log.mjs';
import {
  createGroup, poll, commit, recordFailure, crash, drainDlq, rewind, lag,
  redelivered, resumeOffset,
} from './group.mjs';
import { createDedupeStore, processNaive, processIdempotent } from './dedupe.mjs';

// ── At-least-once + idempotent ──────────────────────────────────────────
// Produce `records` into one partition, consume, and crash exactly once:
// the crash lands AFTER the handler's effect but BEFORE the commit on the
// crashAfter-th record. Restart and resume from the committed bookmark —
// the tail is redelivered, and the crash record is delivered twice.
// With a naive handler its effect applies twice (double charge); with a
// dedupe store the second apply is skipped.
export const PAYMENTS_DEFAULT = Array.from({ length: 10 }, (_, i) => ({
  key: `pay-${1000 + i}`, value: `charge-$${(i + 1) * 10}`,
}));

export function runCrash({ records = PAYMENTS_DEFAULT, crashAfter = 5, idempotent = true } = {}) {
  const events = records.length;
  const broker = createBroker();
  createTopic(broker, 'payments', { partitions: 1 });
  for (const e of records) produce(broker, 'payments', e);
  const group = createGroup(broker, 'payments', { name: 'billing' });
  const seen = createDedupeStore();
  const effects = [];
  let crashes = 0;
  let processed = 0;
  let polls = 0;

  while (polls < 50) {
    polls += 1;
    const batch = poll(group, { maxRecords: events });
    if (batch.length === 0) break;
    for (const r of batch) {
      // The crash lands between the work and the commit on this record:
      // the effect happened; the bookmark didn't move.
      if (crashes === 0 && processed === crashAfter) {
        const apply = idempotent
          ? processIdempotent(seen, r, effects)
          : processNaive(r, effects);
        if (apply !== 'applied') throw new Error('crash record must apply');
        crash(group);
        crashes += 1;
        break; // die here — the rest of the batch was never committed
      }
      if (idempotent) processIdempotent(seen, r, effects);
      else processNaive(r, effects);
      commit(group, r); // commit AFTER the work — the manual contract
      processed += 1;
    }
  }

  const lost = events - group.topic.partitions[0].records.filter(
    (r) => resumeOffset(group, r.partition) > r.offset,
  ).length;
  return {
    idempotent,
    events,
    delivered: [...group.deliveries.values()].reduce((a, b) => a + b, 0),
    redelivered: redelivered(group),
    applied: effects.length,
    processed,
    lost,
    crashes,
    effectLog: effects,
  };
}

// ── Dead-letter queue ───────────────────────────────────────────────────
// One poison record fails on every attempt; everything behind it is fine.
// Below maxAttempts the record is retried (redelivered); at maxAttempts it
// is parked and the consumer moves on — the partition behind it unblocks.
export const JOBS_DEFAULT = Array.from({ length: 8 }, (_, i) => ({
  key: `job-${i}`, value: i === 4 ? 'CORRUPT-PAYLOAD' : `job-${i}`,
}));

export function runPoison({ records = JOBS_DEFAULT, maxAttempts = 3 } = {}) {
  const events = records.length;
  const broker = createBroker();
  createTopic(broker, 'jobs', { partitions: 1 });
  for (const e of records) produce(broker, 'jobs', e);
  const poisonOffset = records.findIndex((e) => e.value === 'CORRUPT-PAYLOAD');
  const group = createGroup(broker, 'jobs', { name: 'workers', maxAttempts });
  const isPoison = (r) => r.value === 'CORRUPT-PAYLOAD';
  const done = new Set();
  let polls = 0;
  let verdicts = [];

  while (polls < 100) {
    polls += 1;
    const batch = poll(group, { maxRecords: 1 }); // one at a time so the retry story is visible
    if (batch.length === 0) break;
    const r = batch[0];
    if (isPoison(r)) {
      const verdict = recordFailure(group, r);
      verdicts.push(verdict);
      continue; // uncommitted on 'retry' -> redelivered; parked on 'dlq'
    }
    commit(group, r);
    done.add(r.offset);
  }

  const parked = drainDlq(group);
  return {
    events,
    poisonOffset,
    poisonAttempts: verdicts.length,
    verdicts,
    processed: done.size,
    dlqSize: parked.length,
    parked,
    done: [...done],
  };
}

// ── Backpressure ────────────────────────────────────────────────────────
// A producer outruns a consumer and the queue absorbs the difference —
// until it can't. depth(t) grows by (producerRate - consumerRate) each
// tick; past `alertAt` the alert should already have fired; past
// `capacity` the producer is BLOCKED (this model's overflow policy) and
// messages are turned away. stallTick optionally drops the producer to 0
// mid-run so the consumer drains the backlog — showing recovery takes
// backlog/rate ticks, not zero.
export function runBackpressure({
  producerRate = 5, consumerRate = 3, ticks = 12, capacity = 20,
  alertAt = 15, stallTick = null,
} = {}) {
  let depthNow = 0;
  let produced = 0;
  let consumed = 0;
  let blocked = 0;
  let firstAlertTick = null;
  const series = [];

  for (let t = 0; t < ticks; t += 1) {
    const rate = stallTick !== null && t >= stallTick ? 0 : producerRate;
    const admitted = Math.min(rate, capacity - depthNow);
    blocked += rate - admitted;
    depthNow += admitted;
    produced += admitted;
    const taken = Math.min(consumerRate, depthNow);
    depthNow -= taken;
    consumed += taken;
    series.push(depthNow);
    if (firstAlertTick === null && depthNow >= alertAt) firstAlertTick = t;
  }

  return {
    producerRate, consumerRate, ticks, capacity, alertAt,
    series, peak: Math.max(...series, 0), endDepth: depthNow,
    produced, consumed, blocked, firstAlertTick,
    overflowed: blocked > 0,
  };
}

// ── Fan-out ─────────────────────────────────────────────────────────────
// Two groups on one topic: each keeps its own offsets, so each sees every
// record. Crash 'billing' mid-read: its uncommitted tail is redelivered on
// restart; 'analytics' is untouched. Rewind billing and it replays history
// alone — fan-out means independent readers, not shared progress.
export function runFanout({ records = Array.from({ length: 6 }, (_, i) => ({ key: `order-${i}`, value: `order-${i}` })), crashAfterCommits = 3 } = {}) {
  const events = records.length;
  const broker = createBroker();
  createTopic(broker, 'orders', { partitions: 1 });
  for (const e of records) produce(broker, 'orders', e);
  const billing = createGroup(broker, 'orders', { name: 'billing' });
  const analytics = createGroup(broker, 'orders', { name: 'analytics' });

  const readFully = (group, crashAt = Infinity) => {
    const applied = new Set();
    let done = 0;
    let polls = 0;
    while (polls < 50) {
      polls += 1;
      const batch = poll(group, { maxRecords: events });
      if (batch.length === 0) break;
      for (const r of batch) {
        if (done >= crashAt) { crash(group); return applied.size; }
        applied.add(r.offset);
        commit(group, r);
        done += 1;
      }
    }
    return applied.size;
  };

  // Analytics reads everything cleanly. Billing crashes mid-read.
  const analyticsSeen = readFully(analytics);
  readFully(billing, crashAfterCommits); // crash mid-read
  readFully(billing);               // restart resumes from committed offset
  const analyticsDeliveries = [...analytics.deliveries.values()];
  const billingDelivered = [...billing.deliveries.values()].reduce((a, b) => a + b, 0);
  const billingRedeliveredAfterCrash = redelivered(billing);

  // Rewind billing alone: it replays all 6; analytics is unaffected.
  rewind(billing, 0);
  const replayed = readFully(billing);

  return {
    events,
    crashAfterCommits,
    analyticsSeen,
    analyticsDeliveries: analyticsDeliveries.length,
    analyticsMaxDelivery: Math.max(...analyticsDeliveries),
    billingDelivered,
    billingRedelivered: billingRedeliveredAfterCrash,
    billingReplayed: replayed,
    analyticsLagAfterBilling: lag(analytics),
  };
}

// ── Ordering ────────────────────────────────────────────────────────────
// Interleaved keyed events across partitions: every key stays ordered in
// its own lane while the global sequence interleaves. Keyless produces
// scatter — the contract dies with the key.
export function runOrdering({ keyedEvents = [], keylessCount = 0, partitions = 3 } = {}) {
  const broker = createBroker();
  const topic = createTopic(broker, 'mixed', { partitions });
  for (const e of keyedEvents) produce(broker, 'mixed', e);
  for (let i = 0; i < keylessCount; i += 1) produce(broker, 'mixed', { value: `keyless-${i}` });
  const contract = keyOrder(broker, 'mixed');
  const keylessSpread = keylessCount
    ? new Set(topic.partitions.flatMap((p) => p.records.filter((r) => r.key === null).map((r) => r.partition))).size
    : 0;
  return {
    partitions: topic.partitions.map((p) => ({
      id: p.id,
      records: p.records.map((r) => ({ offset: r.offset, key: r.key, value: r.value })),
    })),
    contract: contract ? Object.fromEntries(contract) : null,
    contractHolds: contract !== null,
    keylessSpread,
  };
}
