// Side-by-side CLI report: runs every claim the article makes end-to-end
// and asserts it — exit 1 on any broken claim so `npm run check` gates a
// deploy on the story being true.

import {
  runCrash, runPoison, runBackpressure, runFanout, runOrdering,
  PAYMENTS_DEFAULT, JOBS_DEFAULT,
} from '../public/lab.mjs';
import { PAYMENTS, JOBS, SCENARIOS } from '../public/examples.mjs';

const failures = [];
const check = (label, cond, detail) => {
  console.log(`  ${cond ? 'PASS' : 'FAIL'}  ${label} ${detail ? `(${detail})` : ''}`);
  if (!cond) failures.push(label);
};

/* 1 — At-least-once + idempotent: crash between work and commit --------- */
console.log(`\n== Delivery: ${PAYMENTS.length} payments, crash after ${SCENARIOS.crash.crashAfter} commits ==`);
{
  const { crashAfter } = SCENARIOS.crash;
  const results = {};
  for (const idempotent of [false, true]) {
    results[idempotent] = runCrash({ records: PAYMENTS, crashAfter, idempotent });
  }
  const row = (label, r) => console.log(
    `  ${label.padEnd(11)} delivered=${r.delivered} redelivered=${r.redelivered} ` +
    `applied=${r.applied} lost=${r.lost}`,
  );
  row('naive', results.false);
  row('idempotent', results.true);
  check('the crash redelivers the uncommitted tail — at-least-once',
    results.false.redelivered === PAYMENTS.length - crashAfter,
    `${results.false.redelivered} redeliveries`);
  check('nothing is lost — duplicates are the price, not loss',
    results.false.lost === 0 && results.true.lost === 0);
  check('naive handler applies the crashed record twice',
    results.false.applied === PAYMENTS.length + 1,
    `${results.false.applied} effects for ${PAYMENTS.length} events — one double-charge`);
  check('idempotent consumer applies each effect exactly once',
    results.true.applied === PAYMENTS.length,
    `${results.true.applied} effects`);
}

/* 2 — Dead-letter queue: poison parked, tail unblocked ------------------ */
console.log(`\n== DLQ: ${JOBS.length} jobs, job-4 is poison, maxAttempts=${SCENARIOS.poison.maxAttempts} ==`);
{
  const r = runPoison({ records: JOBS, maxAttempts: SCENARIOS.poison.maxAttempts });
  console.log(`  verdicts: ${r.verdicts.join(' → ')}`);
  console.log(`  processed=${r.processed} parked=${r.dlqSize} poisonOffset=${r.poisonOffset}`);
  check(`poison burns exactly ${SCENARIOS.poison.maxAttempts} attempts, then parks`,
    r.poisonAttempts === SCENARIOS.poison.maxAttempts &&
    r.verdicts.join(',') === 'retry,retry,dlq');
  check('every healthy record still processes — no head-of-line blocking',
    r.processed === r.events - 1);
  check('draining the DLQ returns the poison record for replay/audit',
    r.dlqSize === 1 && r.parked[0].record.value === 'CORRUPT-PAYLOAD');
}

/* 3 — Backpressure: producer outruns consumer --------------------------- */
{
  const s = SCENARIOS.backpressure;
  console.log(`\n== Backpressure: producer ${s.producerRate}/tick, consumer ${s.consumerRate}/tick, capacity ${s.capacity} ==`);
  const surge = runBackpressure({ ...s, stallTick: null });
  console.log(`  depth/tick: ${surge.series.join(' ')}`);
  check(`depth grows ${s.producerRate - s.consumerRate}/tick while the gap lasts`,
    surge.series[1] - surge.series[0] === s.producerRate - s.consumerRate);
  check(`alert fires at tick 7 (depth ≥ ${s.alertAt})`,
    surge.firstAlertTick === 7, `tick ${surge.firstAlertTick}`);
  check('capacity forces the overflow policy — producer blocked',
    surge.overflowed && surge.blocked > 0, `${surge.blocked} messages turned away`);

  const stall = runBackpressure(s);
  console.log(`  stall at tick ${s.stallTick}: depth ${stall.series.join(' ')}`);
  check('stopping the producer still leaves a backlog to drain',
    stall.endDepth > 0 && stall.blocked === 0,
    `${stall.endDepth} still queued after ${s.ticks} ticks`);
}

/* 4 — Fan-out: two groups, independent offsets -------------------------- */
console.log(`\n== Fan-out: billing + analytics on one topic ==`);
{
  const r = runFanout({ crashAfterCommits: SCENARIOS.fanout.crashAfterCommits });
  console.log(`  analytics: seen=${r.analyticsSeen} maxDelivery=${r.analyticsMaxDelivery}`);
  console.log(`  billing:   delivered=${r.billingDelivered} redelivered=${r.billingRedelivered} replayed=${r.billingReplayed}`);
  check('every group sees every record — fan-out is independent reads',
    r.analyticsSeen === r.events);
  check('analytics is untouched by billing\'s crash (max 1 delivery each)',
    r.analyticsMaxDelivery === 1);
  check('billing redelivers only its uncommitted tail after the crash',
    r.billingRedelivered === r.events - r.crashAfterCommits,
    `${r.billingRedelivered} redeliveries`);
  check('rewind replays one group\'s history without touching the other',
    r.billingReplayed === r.events && r.analyticsLagAfterBilling === 0);
}

/* 5 — Ordering: per-key contract, never global -------------------------- */
console.log(`\n== Ordering: ${SCENARIOS.ordering.keyedEvents.length} keyed events across ${SCENARIOS.ordering.partitions} partitions ==`);
{
  const s = SCENARIOS.ordering;
  const r = runOrdering({ keyedEvents: s.keyedEvents, keylessCount: s.keylessCount, partitions: s.partitions });
  const c3 = r.contract['customer-c3'];
  console.log(`  customer-c3 → partition ${c3.partition}, offsets ${c3.offsets.join(',')}`);
  console.log(`  keyless events spread across ${r.keylessSpread} partition(s)`);
  check('the contract holds: same key, one partition, produce order',
    r.contractHolds && c3.offsets.length === 4);
  check('customer-c3 reads created→paid→shipped→delivered in one lane',
    r.partitions[c3.partition].records
      .filter((x) => x.key === 'customer-c3')
      .map((x) => x.value).join('|') ===
      'order-1001 created|order-1001 paid|order-1001 shipped|order-1001 delivered');
  check('keyless events scatter — no key, no contract',
    r.keylessSpread > 1, `${r.keylessSpread} partitions`);
}

console.log(failures.length
  ? `\n${failures.length} claim(s) FAILED`
  : '\nAll claims verified.');
process.exit(failures.length ? 1 : 0);
