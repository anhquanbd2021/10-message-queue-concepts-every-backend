// Fixtures mirrored from examples/*.json — kept in sync by test so the UI,
// CLI and tests all tell the same story.

export const PAYMENTS = [
  { key: 'pay-1000', value: 'charge-$10' },
  { key: 'pay-1001', value: 'charge-$20' },
  { key: 'pay-1002', value: 'charge-$30' },
  { key: 'pay-1003', value: 'charge-$40' },
  { key: 'pay-1004', value: 'charge-$50' },
  { key: 'pay-1005', value: 'charge-$60' },
  { key: 'pay-1006', value: 'charge-$70' },
  { key: 'pay-1007', value: 'charge-$80' },
  { key: 'pay-1008', value: 'charge-$90' },
  { key: 'pay-1009', value: 'charge-$100' },
];

export const JOBS = [
  { key: 'job-0', value: 'job-0' },
  { key: 'job-1', value: 'job-1' },
  { key: 'job-2', value: 'job-2' },
  { key: 'job-3', value: 'job-3' },
  { key: 'job-4', value: 'CORRUPT-PAYLOAD' },
  { key: 'job-5', value: 'job-5' },
  { key: 'job-6', value: 'job-6' },
  { key: 'job-7', value: 'job-7' },
];

export const SCENARIOS = {
  crash: { crashAfter: 5 },
  poison: { maxAttempts: 3 },
  backpressure: {
    producerRate: 5,
    consumerRate: 3,
    ticks: 12,
    capacity: 20,
    alertAt: 15,
    stallTick: 8,
  },
  fanout: { crashAfterCommits: 3 },
  ordering: {
    partitions: 3,
    keylessCount: 4,
    keyedEvents: [
      { key: 'customer-c3', value: 'order-1001 created' },
      { key: 'customer-c7', value: 'order-1002 created' },
      { key: 'customer-c3', value: 'order-1001 paid' },
      { key: 'customer-c1', value: 'order-1003 created' },
      { key: 'customer-c7', value: 'order-1002 paid' },
      { key: 'customer-c3', value: 'order-1001 shipped' },
      { key: 'customer-c1', value: 'order-1003 paid' },
      { key: 'customer-c7', value: 'order-1002 shipped' },
      { key: 'customer-c3', value: 'order-1001 delivered' },
    ],
  },
};
