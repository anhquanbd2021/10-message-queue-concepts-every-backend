import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runFanout, runOrdering } from '../public/lab.mjs';
import { SCENARIOS } from '../public/examples.mjs';
import { createBroker, createTopic, produce } from '../public/log.mjs';
import { createGroup, poll, commit, resumeOffset } from '../public/group.mjs';

test('each consumer group sees every record — independent reads', () => {
  const r = runFanout({ crashAfterCommits: 0 }); // nobody crashes: cleanest form
  assert.equal(r.analyticsSeen, r.events);
  assert.equal(r.analyticsMaxDelivery, 1);
});

test('a crash in one group never touches the other group\'s offsets', () => {
  const r = runFanout({ crashAfterCommits: SCENARIOS.fanout.crashAfterCommits });
  assert.equal(r.analyticsMaxDelivery, 1);          // analytics: delivered once
  assert.equal(r.billingRedelivered, 3);            // billing: tail redelivered
  assert.equal(r.analyticsLagAfterBilling, 0);      // analytics fully caught up
});

test('rewind replays one group\'s history alone', () => {
  const r = runFanout({ crashAfterCommits: SCENARIOS.fanout.crashAfterCommits });
  assert.equal(r.billingReplayed, r.events);        // billing sees all 6 again
  assert.equal(r.analyticsLagAfterBilling, 0);      // analytics unaffected
});

test('groups hold separate committed offsets over the same partition', () => {
  const broker = createBroker();
  createTopic(broker, 't', { partitions: 1 });
  for (let i = 0; i < 5; i += 1) produce(broker, 't', { key: `k${i}`, value: `v${i}` });
  const a = createGroup(broker, 't', { name: 'a' });
  const b = createGroup(broker, 't', { name: 'b' });
  const first = poll(a, { maxRecords: 2 });
  for (const r of first) commit(a, r);
  assert.equal(resumeOffset(a, 0), 2);
  assert.equal(resumeOffset(b, 0), 0); // b hasn't read — its own bookmark
  assert.equal(poll(b, { maxRecords: 10 }).length, 5); // b still gets everything
});

test('ordering: keyed events keep per-key order; keyless scatter', () => {
  const s = SCENARIOS.ordering;
  const r = runOrdering({ keyedEvents: s.keyedEvents, keylessCount: s.keylessCount, partitions: s.partitions });
  assert.equal(r.contractHolds, true);
  const c3 = r.contract['customer-c3'];
  assert.deepEqual(
    r.partitions[c3.partition].records.filter((x) => x.key === 'customer-c3').map((x) => x.value),
    ['order-1001 created', 'order-1001 paid', 'order-1001 shipped', 'order-1001 delivered'],
  );
  assert.ok(r.keylessSpread > 1); // no key -> no lane -> no order
});
