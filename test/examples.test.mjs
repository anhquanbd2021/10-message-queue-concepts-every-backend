// Keeps public/examples.mjs mirrored to examples/*.json — the UI, CLI and
// tests must tell the same story as the shipped fixtures.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { PAYMENTS, JOBS, SCENARIOS } from '../public/examples.mjs';
import { PAYMENTS_DEFAULT, JOBS_DEFAULT } from '../public/lab.mjs';

const load = (name) => JSON.parse(
  readFileSync(fileURLToPath(new URL(`../examples/${name}`, import.meta.url)), 'utf8'),
);

test('payments fixture mirrors examples/payments.json and the lab default', () => {
  assert.deepEqual(PAYMENTS, load('payments.json'));
  assert.deepEqual(PAYMENTS, PAYMENTS_DEFAULT);
});

test('jobs fixture mirrors examples/jobs.json and the lab default', () => {
  assert.deepEqual(JOBS, load('jobs.json'));
  assert.deepEqual(JOBS, JOBS_DEFAULT);
  assert.equal(JOBS[4].value, 'CORRUPT-PAYLOAD'); // the poison lives at offset 4
});

test('scenario parameters mirror examples/scenarios.json', () => {
  const disk = load('scenarios.json');
  for (const key of Object.keys(SCENARIOS)) {
    assert.deepEqual(SCENARIOS[key], disk[key], key);
  }
});
