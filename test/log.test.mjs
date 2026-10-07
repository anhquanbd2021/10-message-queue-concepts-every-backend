import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  createBroker, createTopic, produce, partitionFor, depth, keyOrder, fnv1a,
} from '../public/log.mjs';
import { SCENARIOS } from '../public/examples.mjs';

test('produce assigns sequential offsets per partition', () => {
  const broker = createBroker();
  createTopic(broker, 't', { partitions: 1 });
  const a = produce(broker, 't', { key: 'k', value: 'a' });
  const b = produce(broker, 't', { key: 'k', value: 'b' });
  assert.equal(a.offset, 0);
  assert.equal(b.offset, 1);
  assert.equal(depth(broker.topics.get('t')), 2);
});

test('same key always routes to the same partition', () => {
  const broker = createBroker();
  createTopic(broker, 't', { partitions: 4 });
  const p = partitionFor(broker, 't', 'customer-c3');
  for (let i = 0; i < 5; i += 1) produce(broker, 't', { key: 'customer-c3', value: `e${i}` });
  const lane = broker.topics.get('t').partitions[p];
  assert.equal(lane.records.length, 5);
  assert.deepEqual(lane.records.map((r) => r.value), ['e0', 'e1', 'e2', 'e3', 'e4']);
});

test('keyless produces round-robin — spread, not ordered', () => {
  const broker = createBroker();
  createTopic(broker, 't', { partitions: 3 });
  const landed = new Set();
  for (let i = 0; i < 6; i += 1) {
    const r = produce(broker, 't', { value: `k${i}` });
    landed.add(r.partition);
  }
  assert.equal(landed.size, 3); // scattered across every lane
});

test('keyOrder reports the per-key contract', () => {
  const broker = createBroker();
  createTopic(broker, 'mixed', { partitions: 3 });
  for (const e of SCENARIOS.ordering.keyedEvents) produce(broker, 'mixed', e);
  const contract = keyOrder(broker, 'mixed');
  assert.ok(contract);
  assert.equal(contract.get('customer-c3').offsets.length, 4);
});

test('fnv1a is stable and deterministic', () => {
  assert.equal(fnv1a('customer-c3'), fnv1a('customer-c3'));
  assert.notEqual(fnv1a('customer-c3'), fnv1a('customer-c7'));
});
