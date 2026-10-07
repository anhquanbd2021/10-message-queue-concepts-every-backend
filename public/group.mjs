// group.mjs — the read side: a consumer group with its own durable
// bookmarks. `committed` maps partition -> next offset to deliver — the
// offset commit from the cheat sheet, here as the guarantee it actually
// is: what a restarted consumer resumes from. poll() delivers records
// from the committed position and counts deliveries — a record delivered
// twice is the visible price of at-least-once. recordFailure() counts
// attempts: below maxAttempts the record stays uncommitted (redelivered —
// a retry); at maxAttempts it is parked in the DLQ and committed past, so
// one poison message can't block the partition behind it.

export function createGroup(broker, topicName, { name = 'group-1', maxAttempts = 3 } = {}) {
  const topic = broker.topics.get(topicName);
  const group = {
    broker,
    topic,
    name,
    maxAttempts,
    committed: new Map(),     // partition -> next offset to deliver (durable)
    deliveries: new Map(),    // "p:o" -> times delivered (duplicates show up here)
    attempts: new Map(),      // "p:o" -> processing attempts
    dlq: [],                  // parked records + the attempts they burned
    crashed: false,
  };
  topic.groups.set(name, group);
  return group;
}

const rid = (r) => `${r.partition}:${r.offset}`;

export function resumeOffset(group, partitionId) {
  return group.committed.get(partitionId) ?? 0;
}

// Deliver up to maxRecords uncommitted records across the group's
// partitions. Nothing is committed here — delivery and the bookmark are
// different events, and that gap is where at-least-once lives.
export function poll(group, { maxRecords = 50 } = {}) {
  const records = [];
  for (const partition of group.topic.partitions) {
    const from = resumeOffset(group, partition.id);
    for (let i = from; i < partition.records.length && records.length < maxRecords; i += 1) {
      const r = partition.records[i];
      records.push(r);
      noteDelivery(group, r);
    }
  }
  return records;
}

function noteDelivery(group, record) {
  const key = rid(record);
  group.deliveries.set(key, (group.deliveries.get(key) ?? 0) + 1);
}

export function deliveryCount(group, record) {
  return group.deliveries.get(rid(record)) ?? 0;
}

// Records delivered more than once — the duplicates at-least-once charges.
export function redelivered(group) {
  return [...group.deliveries.values()].filter((n) => n > 1).length;
}

// Commit one processed record. Only moves the bookmark forward.
export function commit(group, record) {
  const p = record.partition;
  group.committed.set(p, Math.max(resumeOffset(group, p), record.offset + 1));
}

// A processing failure: bump the attempt counter. Below maxAttempts the
// record stays uncommitted — the next poll redelivers it (the retry). At
// maxAttempts it is parked: pushed to the DLQ and committed past so the
// poison stops blocking the records behind it.
export function recordFailure(group, record) {
  const key = rid(record);
  const n = (group.attempts.get(key) ?? 0) + 1;
  group.attempts.set(key, n);
  if (n >= group.maxAttempts) {
    group.dlq.push({ record, attempts: n });
    commit(group, record); // park = acknowledge and move on
    return 'dlq';
  }
  return 'retry';
}

export function attemptCount(group, record) {
  return group.attempts.get(rid(record)) ?? 0;
}

// Crash + rejoin: the process dies; only the committed offsets survive.
// In this model committed state IS the durable state, so a crash is a
// marker — the redelivery happens automatically on the next poll, because
// uncommitted records are still ahead of the bookmark.
export function crash(group) {
  group.crashed = true;
  return group;
}

// Drain the DLQ: hand the parked records to whoever replays or audits
// them. A DLQ nobody drains is a graveyard, so draining returns a copy
// and clears the lot.
export function drainDlq(group) {
  const parked = group.dlq.slice();
  group.dlq.length = 0;
  return parked;
}

// Rewind: reset the bookmark so the group replays history. Per-group —
// rewinding 'billing' does nothing to 'analytics', which is the whole
// point of fan-out.
export function rewind(group, partitionId, offset = 0) {
  group.committed.set(partitionId, offset);
}

// Lag: records produced but not yet committed for this group — the number
// a backpressure alert actually watches.
export function lag(group) {
  return group.topic.partitions.reduce(
    (n, p) => n + Math.max(0, p.records.length - resumeOffset(group, p.id)),
    0,
  );
}
