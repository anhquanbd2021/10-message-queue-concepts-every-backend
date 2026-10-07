import { test, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { startProduction } from '../app/server.js';

let base; let close;

before(async () => {
  const started = await startProduction({ port: 0 });
  base = `http://127.0.0.1:${started.server.address().port}`;
  close = started.close;
});
after(async () => { await close(); });

test('health and version endpoints respond', async () => {
  const health = await fetch(`${base}/health`);
  assert.equal(health.status, 200);
  assert.equal(await health.text(), 'ok');
  const version = await fetch(`${base}/version`);
  const meta = await version.json();
  assert.equal(meta.name, 'queue-guarantees-lab');
  assert.ok(meta.version);
});

test('allowlisted assets serve; everything else 404s', async () => {
  for (const path of ['/', '/guide.html', '/styles.css', '/app.js', '/lab.mjs']) {
    const res = await fetch(`${base}${path}`);
    assert.equal(res.status, 200, path);
  }
  for (const path of ['/package.json', '/app/server.js', '/nope', '/examples/jobs.json']) {
    const res = await fetch(`${base}${path}`);
    assert.equal(res.status, 404, path);
  }
});
