import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runCrash } from '../public/lab.mjs';
import { PAYMENTS, SCENARIOS } from '../public/examples.mjs';
import { dedupeKey, createDedupeStore, processIdempotent, processNaive } from '../public/dedupe.mjs';

const { crashAfter } = SCENARIOS.crash;

test('a crash between work and commit redelivers the uncommitted tail', () => {
  const r = runCrash({ records: PAYMENTS, crashAfter, idempotent: true });
  assert.equal(r.redelivered, PAYMENTS.length - crashAfter); // 5
  assert.equal(r.lost, 0); // at-least-once: duplicates, never loss
  assert.equal(r.delivered, PAYMENTS.length + r.redelivered);
});

test('naive handler double-applies the crashed record — one extra charge', () => {
  const r = runCrash({ records: PAYMENTS, crashAfter, idempotent: false });
  assert.equal(r.applied, PAYMENTS.length + 1); // 11 effects for 10 events
});

test('idempotent consumer applies every effect exactly once', () => {
  const r = runCrash({ records: PAYMENTS, crashAfter, idempotent: true });
  assert.equal(r.applied, PAYMENTS.length);
});

test('dedupe key is the delivery identity, not the payload', () => {
  const a = { partition: 0, offset: 3, value: 'charge-$40' };
  const b = { partition: 0, offset: 4, value: 'charge-$40' }; // same payload, different delivery
  assert.notEqual(dedupeKey(a), dedupeKey(b)); // legitimate repeats must not collapse
});

test('processIdempotent skips a second apply for the same key', () => {
  const seen = createDedupeStore();
  const effects = [];
  const r = { partition: 0, offset: 0, value: 'x' };
  assert.equal(processIdempotent(seen, r, effects), 'applied');
  assert.equal(processIdempotent(seen, r, effects), 'duplicate-skipped');
  assert.equal(effects.length, 1);
  // naive applies regardless
  processNaive(r, effects);
  processNaive(r, effects);
  assert.equal(effects.length, 3);
});

test('no crash means no redelivery', () => {
  const r = runCrash({ records: PAYMENTS, crashAfter: Infinity, idempotent: true });
  assert.equal(r.redelivered, 0);
  assert.equal(r.applied, PAYMENTS.length);
});
