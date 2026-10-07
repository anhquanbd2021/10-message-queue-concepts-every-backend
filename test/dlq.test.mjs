import { test } from 'node:test';
import assert from 'node:assert/strict';
import { runPoison } from '../public/lab.mjs';
import { JOBS, SCENARIOS } from '../public/examples.mjs';
import { createBroker, createTopic, produce } from '../public/log.mjs';
import { createGroup, poll, recordFailure, drainDlq, commit } from '../public/group.mjs';

const { maxAttempts } = SCENARIOS.poison;

test('a poison record burns maxAttempts deliveries, then parks', () => {
  const r = runPoison({ records: JOBS, maxAttempts });
  assert.equal(r.poisonAttempts, maxAttempts);
  assert.deepEqual(r.verdicts, ['retry', 'retry', 'dlq']);
  assert.equal(r.dlqSize, 1);
  assert.equal(r.parked[0].record.value, 'CORRUPT-PAYLOAD');
  assert.equal(r.parked[0].attempts, maxAttempts);
});

test('parking unblocks the partition — the tail still processes', () => {
  const r = runPoison({ records: JOBS, maxAttempts });
  assert.equal(r.processed, JOBS.length - 1);
  assert.deepEqual(r.done, [0, 1, 2, 3, 5, 6, 7]); // everything but the poison
});

test('a retried record is redelivered before it parks', () => {
  const broker = createBroker();
  createTopic(broker, 't', { partitions: 1 });
  produce(broker, 't', { key: 'j', value: 'bad' });
  const group = createGroup(broker, 't', { maxAttempts: 3 });
  assert.equal(recordFailure(group, poll(group)[0]), 'retry');
  // uncommitted -> next poll delivers it again
  const again = poll(group)[0];
  assert.equal(again.offset, 0);
  assert.equal(recordFailure(group, again), 'retry');
  assert.equal(recordFailure(group, poll(group)[0]), 'dlq');
  assert.equal(group.dlq.length, 1);
});

test('draining returns the parked records and clears the lot', () => {
  const broker = createBroker();
  createTopic(broker, 't', { partitions: 1 });
  produce(broker, 't', { key: 'j', value: 'bad' });
  produce(broker, 't', { key: 'j2', value: 'good' });
  const group = createGroup(broker, 't', { maxAttempts: 1 });
  recordFailure(group, poll(group)[0]); // parks immediately at maxAttempts=1
  commit(group, poll(group)[0]);        // the good record processes
  const drained = drainDlq(group);
  assert.equal(drained.length, 1);
  assert.equal(group.dlq.length, 0); // a drained DLQ is empty
});
