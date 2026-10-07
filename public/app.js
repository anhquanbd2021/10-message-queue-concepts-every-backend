import { createBroker, createTopic, produce, keyOrder } from './log.mjs';
import {
  createGroup, poll, commit, crash, rewind, lag, redelivered, resumeOffset,
} from './group.mjs';
import { runCrash, runPoison, runBackpressure } from './lab.mjs';
import { PAYMENTS, JOBS, SCENARIOS } from './examples.mjs';

const $ = (id) => document.getElementById(id);
const badge = (el, text, cls) => { el.textContent = text; el.className = `badge ${cls}`; };

/* Delivery lab ------------------------------------------------------------ */
$('dl-crash').addEventListener('input', () => {
  $('dl-crash-out').textContent = $('dl-crash').value;
});
const runDelivery = (idempotent) => {
  const crashAfter = Number($('dl-crash').value);
  const r = runCrash({ records: PAYMENTS, crashAfter, idempotent });
  $('dl-delivered').textContent = r.delivered;
  $('dl-redel').textContent = r.redelivered;
  $('dl-applied').textContent = r.applied;
  $('dl-lost').textContent = r.lost;
  // Count applies per key from the effect log to flag the double-charge.
  const appliedBy = new Map();
  for (const e of r.effectLog) appliedBy.set(e.key, (appliedBy.get(e.key) ?? 0) + 1);
  $('dl-log').innerHTML = PAYMENTS.map((p, i) => {
    const key = `0:${i}`;
    const applies = appliedBy.get(key) ?? 0;
    const redel = i >= crashAfter;
    const cls = applies > 1 ? 'fail' : redel ? 'warn' : 'pass';
    const note = applies > 1
      ? `applied ${applies}× — the double-charge`
      : i === crashAfter
        ? 'crashed between work and commit → redelivered, deduped'
        : redel ? 'redelivered after restart' : 'processed and committed';
    return `<li class="result ${cls}">${p.key} (${p.value}) — ${note}</li>`;
  }).join('');
  badge($('dl-badge'), idempotent
    ? `idempotent: ${r.redelivered} redeliveries, ${r.applied} effects — deduped`
    : `naive: ${r.applied} effects for ${r.events} events — one double-charge`,
    idempotent ? 'pass' : 'fail');
};
$('dl-naive').addEventListener('click', () => runDelivery(false));
$('dl-idem').addEventListener('click', () => runDelivery(true));

/* Dead-letter lab --------------------------------------------------------- */
let dqParked = [];
$('dq-run').addEventListener('click', () => {
  const r = runPoison({ records: JOBS, maxAttempts: SCENARIOS.poison.maxAttempts });
  dqParked = r.parked;
  $('dq-log').innerHTML = JOBS.map((j, i) => {
    if (i === r.poisonOffset) {
      return `<li class="result fail">${j.key} — CORRUPT-PAYLOAD: attempt 1 fail → retry, attempt 2 fail → retry, attempt 3 fail → parked in DLQ</li>`;
    }
    const past = i > r.poisonOffset;
    return `<li class="result pass">${j.key} — processed${past ? ' (after the poison parked)' : ''}</li>`;
  }).join('');
  $('dq-dlq').textContent = `DLQ: ${r.dlqSize} parked — job-4 burned ${r.poisonAttempts} attempts`;
  $('dq-drain').disabled = false;
  badge($('dq-badge'), `poison parked after ${r.poisonAttempts} attempts — ${r.processed}/${r.events} jobs still processed`, 'warn');
});
$('dq-drain').addEventListener('click', () => {
  $('dq-dlq').textContent = `DLQ: drained — ${dqParked.map((p) => p.record.key).join(', ')} handed to replay/audit`;
  $('dq-drain').disabled = true;
  badge($('dq-badge'), 'DLQ drained — parked records replay or get audited, never vanish', 'pass');
});

/* Backpressure lab -------------------------------------------------------- */
const syncBp = () => {
  $('bp-prod-out').textContent = $('bp-prod').value;
  $('bp-cons-out').textContent = $('bp-cons').value;
  const v = Number($('bp-stall').value);
  $('bp-stall-out').textContent = v >= 12 ? 'never' : v;
};
for (const id of ['bp-prod', 'bp-cons', 'bp-stall']) $(id).addEventListener('input', syncBp);
$('bp-run').addEventListener('click', () => {
  const s = SCENARIOS.backpressure;
  const stall = Number($('bp-stall').value);
  const r = runBackpressure({
    producerRate: Number($('bp-prod').value),
    consumerRate: Number($('bp-cons').value),
    ticks: s.ticks, capacity: s.capacity, alertAt: s.alertAt,
    stallTick: stall >= s.ticks ? null : stall,
  });
  // Inline SVG, not inline styles — the page CSP blocks style attributes.
  const W = 560; const H = 150; const GAP = 6;
  const bw = (W - GAP * (r.series.length - 1)) / r.series.length;
  const alertY = H - (s.alertAt / s.capacity) * H;
  const rects = r.series.map((d, t) => {
    const bh = (d / s.capacity) * H;
    const fill = d >= s.capacity - s.consumerRate ? '#c0504d' : d >= s.alertAt ? '#EBB042' : '#48A080';
    return `<rect x="${(t * (bw + GAP)).toFixed(1)}" y="${(H - bh).toFixed(1)}" width="${bw.toFixed(1)}" height="${Math.max(2, bh).toFixed(1)}" rx="3" fill="${fill}"/>` +
      `<text x="${(t * (bw + GAP) + bw / 2).toFixed(1)}" y="${Math.max(12, H - bh - 6).toFixed(1)}" text-anchor="middle" font-size="11" fill="#8aa0ae">${d}</text>`;
  }).join('');
  $('bp-chart').innerHTML =
    `<svg viewBox="0 0 ${W} ${H + 8}" width="100%" role="img" aria-label="Queue depth per tick">` +
    rects +
    `<line x1="0" x2="${W}" y1="${alertY}" y2="${alertY}" stroke="#EBB042" stroke-width="2" stroke-dasharray="6 4"/>` +
    `<text x="4" y="${alertY - 6}" font-size="12" font-weight="700" fill="#EBB042">alert ${s.alertAt}</text>` +
    `<text x="${W - 4}" y="${H + 8}" text-anchor="end" font-size="10" fill="#8aa0ae">capacity ${s.capacity}</text></svg>`;
  $('bp-peak').textContent = r.peak;
  $('bp-alert').textContent = r.firstAlertTick ?? '—';
  $('bp-blocked').textContent = r.blocked;
  badge($('bp-badge'), r.overflowed
    ? `overflowed — producer blocked ${r.blocked}×, peak depth ${r.peak}`
    : r.firstAlertTick !== null
      ? `alerted at tick ${r.firstAlertTick} — end depth ${r.endDepth}`
      : 'consumer kept up — depth never crossed the line',
    r.overflowed ? 'fail' : r.firstAlertTick !== null ? 'warn' : 'pass');
});

/* Fan-out lab ------------------------------------------------------------- */
let fo = null;
const foReset = () => {
  const broker = createBroker();
  const s = SCENARIOS.ordering;
  createTopic(broker, 'orders', { partitions: s.partitions });
  for (const e of s.keyedEvents) produce(broker, 'orders', e);
  for (let i = 0; i < s.keylessCount; i += 1) produce(broker, 'orders', { value: `keyless-${i}` });
  fo = {
    broker,
    topic: broker.topics.get('orders'),
    billing: createGroup(broker, 'orders', { name: 'billing' }),
    analytics: createGroup(broker, 'orders', { name: 'analytics' }),
  };
  renderFo('Ready — 13 records across 3 partitions, both groups at offset 0');
};

const readFully = (group, crashAt = Infinity) => {
  let done = 0;
  let crashed = false;
  for (let i = 0; i < 20; i += 1) {
    const batch = poll(group, { maxRecords: 50 });
    if (batch.length === 0) break;
    for (const r of batch) {
      if (done >= crashAt) { crash(group); crashed = true; break; }
      commit(group, r);
      done += 1;
    }
    if (crashed) break;
  }
  return crashed;
};

const renderFo = (note) => {
  const groupCard = (g) => {
    const committed = [...fo.topic.partitions].map((p) => resumeOffset(g, p.id)).join(',');
    return `<li class="lane${g.crashed ? ' idle' : ''}">
      <div class="lane-head"><span>${g.name}</span>
      <span class="${g.crashed ? 'idle-tag' : 'muted'}">${g.crashed ? 'CRASHED — resume pending' : `committed offsets ${committed} · lag ${lag(g)}`}</span></div>
    </li>`;
  };
  $('fo-groups').innerHTML = groupCard(fo.billing) + groupCard(fo.analytics) +
    `<li class="lane"><div class="lane-head"><span>billing redeliveries</span><span class="muted">${redelivered(fo.billing)} · analytics ${redelivered(fo.analytics)}</span></div></li>`;
  const contract = keyOrder(fo.broker, 'orders');
  $('fo-lanes').innerHTML = fo.topic.partitions.map((p) => `
    <li class="lane">
      <div class="lane-head"><span>partition ${p.id}</span><span class="muted">${p.records.length} records</span></div>
      <div class="recs">${p.records.map((r) =>
        `<span class="rec">o${r.offset} · ${r.key ?? 'no key'}<small> ${r.value.split(' ')[0]}</small></span>`).join('') || '<span class="muted">empty</span>'}</div>
    </li>`).join('');
  badge($('fo-badge'), contract ? `${note} — per-key contract holds` : note, 'pass');
};

$('fo-run').addEventListener('click', () => {
  readFully(fo.billing);
  readFully(fo.analytics);
  renderFo('both groups read every record — independent offsets, same data');
});
$('fo-crash').addEventListener('click', () => {
  foResetState();
  readFully(fo.analytics);
  readFully(fo.billing, 3);         // crash after 3 commits
  readFully(fo.billing);            // restart: resumes from committed offsets
  renderFo(`billing crashed after 3 commits → ${redelivered(fo.billing)} redeliveries; analytics untouched`);
});
$('fo-rewind').addEventListener('click', () => {
  for (const p of fo.topic.partitions) rewind(fo.billing, p.id, 0);
  readFully(fo.billing);
  renderFo('billing rewound and replayed history — analytics never noticed');
});
const foResetState = () => {
  // Rebuild the world so the crash story starts clean each click.
  const keep = fo;
  foReset();
  if (keep) badge($('fo-badge'), 'Ready', '');
};
foReset();
