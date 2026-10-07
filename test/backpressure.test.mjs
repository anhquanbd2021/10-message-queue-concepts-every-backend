import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runBackpressure } from '../public/lab.mjs';
import { SCENARIOS } from '../public/examples.mjs';
import { createBroker, createTopic, produce } from '../public/log.mjs';
import { createGroup, lag } from '../public/group.mjs';

const s = SCENARIOS.backpressure;

test('depth grows by (producerRate - consumerRate) each tick', () => {
  const r = runBackpressure({ ...s, stallTick: null });
  assert.deepEqual(r.series.slice(0, 4), [2, 4, 6, 8]);
  assert.equal(r.peak, s.capacity - s.consumerRate); // pinned against the wall
});

test('the alert fires while the queue can still buffer', () => {
  const r = runBackpressure({ ...s, stallTick: null });
  assert.equal(r.firstAlertTick, 7); // depth 16 >= alertAt 15
  assert.ok(r.series[r.firstAlertTick] < s.capacity); // before overflow
});

test('at capacity the producer is blocked — the chosen overflow policy', () => {
  const r = runBackpressure({ ...s, stallTick: null });
  assert.ok(r.overflowed);
  assert.equal(r.blocked, 7);
  assert.equal(r.produced + r.blocked, s.producerRate * s.ticks);
});

test('a stalled producer still leaves a backlog to drain', () => {
  const r = runBackpressure(s); // stallTick 8
  assert.equal(r.blocked, 0);
  assert.equal(r.endDepth, 4); // 16 deep at the stall, 3/tick drain, 4 ticks
});

test('matched rates keep depth at zero — the happy path', () => {
  const r = runBackpressure({ producerRate: 3, consumerRate: 3, ticks: 8, capacity: 20, alertAt: 15 });
  assert.deepEqual(r.series, [0, 0, 0, 0, 0, 0, 0, 0]);
  assert.equal(r.firstAlertTick, null);
});

test('lag() is the same depth the alert watches', () => {
  const broker = createBroker();
  createTopic(broker, 't', { partitions: 1 });
  for (let i = 0; i < 10; i += 1) produce(broker, 't', { key: `k${i}`, value: `v${i}` });
  const group = createGroup(broker, 't', { name: 'g' });
  assert.equal(lag(group), 10);
});
