// log.mjs — the partitioned append-only log the whole lab stands on.
// A topic is a set of partitions; produce() routes a record to exactly one
// partition by hashing its key (keyless records round-robin). Each record
// gets an offset — its position in that partition's log. This models
// mechanics, not a real broker: no network, no replication, no persistence.

// FNV-1a 32-bit — same role Kafka's murmur2 plays: stable key -> int.
export function fnv1a(key) {
  let h = 0x811c9dc5;
  for (const ch of String(key)) {
    h ^= ch.codePointAt(0);
    h = Math.imul(h, 0x01000193) >>> 0;
  }
  return h >>> 0;
}

export function createBroker() {
  return { topics: new Map() };
}

export function createTopic(broker, name, { partitions = 3 } = {}) {
  const topic = {
    name,
    rr: 0, // round-robin cursor for keyless produces
    partitions: Array.from({ length: partitions }, (_, p) => ({
      id: p,
      records: [], // { offset, partition, key, value }
    })),
    groups: new Map(), // name -> consumer group (group.mjs registers here)
  };
  broker.topics.set(name, topic);
  return topic;
}

// The routing decision: same key always lands in the same partition.
export function partitionFor(broker, topicName, key) {
  const topic = broker.topics.get(topicName);
  if (key === null || key === undefined) {
    const p = topic.rr % topic.partitions.length;
    topic.rr += 1;
    return p;
  }
  return fnv1a(key) % topic.partitions.length;
}

export function produce(broker, topicName, { key = null, value }) {
  const topic = broker.topics.get(topicName);
  const p = partitionFor(broker, topicName, key);
  const partition = topic.partitions[p];
  const record = { offset: partition.records.length, partition: p, key, value };
  partition.records.push(record);
  return record;
}

// How many records sit in the topic across all partitions — the depth a
// backpressure alert would watch before any consumer exists.
export function depth(topic) {
  return topic.partitions.reduce((n, p) => n + p.records.length, 0);
}

// The ordering contract: records sharing a key appear in ONE partition, in
// produce order. Returns { partition, offsets } per key — or null if the
// contract is broken (same key in two partitions, or out-of-order offsets).
export function keyOrder(broker, topicName) {
  const topic = broker.topics.get(topicName);
  const byKey = new Map();
  for (const p of topic.partitions) {
    for (const r of p.records) {
      if (r.key === null || r.key === undefined) continue;
      if (!byKey.has(r.key)) byKey.set(r.key, { partition: p.id, offsets: [] });
      const entry = byKey.get(r.key);
      if (entry.partition !== p.id) return null; // key scattered -> contract broken
      entry.offsets.push(r.offset);
    }
  }
  for (const { offsets } of byKey.values()) {
    if (!offsets.every((o, i) => i === 0 || o > offsets[i - 1])) return null;
  }
  return byKey;
}
