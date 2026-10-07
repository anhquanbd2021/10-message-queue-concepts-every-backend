// dedupe.mjs — the idempotent consumer: the actual fix for the duplicates
// at-least-once hands you. The handler keeps a dedupe store of keys it has
// already applied an effect for; a redelivery finds its key in the store
// and skips the effect instead of applying it twice. At-least-once
// delivery + idempotent effects = an exactly-once OUTCOME — which is the
// only kind of exactly-once that exists without a transaction spanning
// queue and database.

// The dedupe key must identify the DELIVERY, not the payload. Two
// legitimate identical orders must not collapse into one — so the key is
// the record's position, and a real system would prefer a business key
// the producer assigns (payment id) for the same reason.
export function dedupeKey(record) {
  return `${record.partition}:${record.offset}`;
}

// Naive handler: applies the effect on every delivery. A redelivered
// record is a second charge.
export function processNaive(record, effects) {
  effects.push({ key: dedupeKey(record), value: record.value });
  return 'applied';
}

// Idempotent handler: checks the store first. A redelivery is seen,
// acknowledged — and its effect is skipped.
export function processIdempotent(seen, record, effects) {
  const key = dedupeKey(record);
  if (seen.has(key)) return 'duplicate-skipped';
  seen.add(key);
  effects.push({ key, value: record.value });
  return 'applied';
}

export function createDedupeStore() {
  return new Set();
}
