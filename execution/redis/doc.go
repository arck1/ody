// Package redis provides a Redis-backed execution.Store.
//
// The Store keeps two kinds of data:
//
//   - source-of-truth records: serialized executions, pipeline hashes, and append-only event lists;
//   - derived indexes: sorted sets by task, status, pipeline, availability, and lease deadline.
//
// A per-task sorted set is also the delivery queue. Pending and retrying executions are scored by
// AvailableAt. Running executions remain in that set with LeaseUntil as their score, which makes an
// abandoned delivery visible for another claim after its lease expires. Terminal transitions
// remove the execution from the queue but retain its record and events for monitoring.
//
// Mutations use Redis WATCH together with MULTI/EXEC. WATCH protects the serialized execution or
// pipeline record; the transaction updates that record and every related index atomically. A
// concurrent writer causes a bounded retry instead of a lost update. Queue and lease comparisons
// use Redis TIME so workers do not need synchronized local clocks.
//
// Every key starts with one configurable prefix. The default prefix includes {execution}, forcing
// all keys into one Redis Cluster hash slot because each transaction touches several related keys.
// Custom prefixes used with Redis Cluster must preserve an equivalent common hash tag.
package redis
