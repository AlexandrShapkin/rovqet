# MVP reproducibility and benchmark profiles

## Protocol

Use two processes at minimum: broker and independent client/oracle. Persist original send/ACK/delivery records outside broker's data directory. Record CPU model, kernel, RAM, TCP buffer sizes, network and exact Git commit. Keep load-generator overhead below saturation; invalid generator-limited runs are not broker results.

## Required Q0 smoke

- 1 publisher → 1 subscriber, 256 bytes and 1,000 ordered sends; measure actual delivery count.
- 1 publisher → 1,000 subscribers, 256 bytes; count 1,000 physical deliveries per message where all remain connected.
- 100 publishers → 10 subscribers, independent sessions and topic partitions; no cross-partition order claims.
- 1 intentionally blocked subscriber in 1→N: others still deliver; blocked subscriber bounded or dropped as configured.
- Bursty load > capacity: compare offered, admitted, delivered, rejected and dropped; no OOM.
- 10,000 idle actual TCP sessions early smoke (if available machine permits); record kernel socket bytes and FDs, not fake virtual clients.

## Required Q1 smoke

- Append 1,000 unique records, receive COMMITTED_LOCAL after fsync, kill broker, restart on same disk, exact set comparison.
- Drop a PUB_ACK response and retry same producer epoch/sequence: one committed record and same offset.
- Same sequence different digest → SEQUENCE_CONFLICT; gap → SEQUENCE_GAP; stale producer epoch → PRODUCER_FENCED.
- Consumer sends ACK for offset 2 while 1 pending: cursor remains before 1; ACK 1 commits → cursor advances through 2.
- Crash before and after ACK barrier; restore cursor without skip; duplicate ACK can return saved result.
- Explicitly simulate *permanent owner/disk loss* with separate disposable test data root: old incarnation rejects replay and new incarnation requires reset.

## Required Q2 smoke

- Three nodes on independent durable directories, fixed RF=3. Record node/host distinction.
- After COMMITTED_QUORUM, kill leader; remaining 2/3 elect, recover committed record and dedup outcome.
- Kill follower while leader and second follower remain; commits possible only if durable majority.
- Isolate leader alone; it **must not** produce new COMMITTED_QUORUM.
- Restart divergent follower; install snapshot/catch up, never expose old uncommitted tail.
- Publish incompatible Q1 into Q2 partition and reverse: UNSUPPORTED_GUARANTEE before append/offset.

## Preliminary comparison plan

- Q0 profile: NATS Core versus Rovqet, equal payload, actual connection count, offered load and physical deliveries. If Rovqet lab plaintext vs NATS TLS differ, clearly annotate and do not claim VAL-01 advantage.
- Q1/Q2: NATS JetStream configurations matched as closely as possible for durable commit, replication and ACK semantics; produce semantic-delta report, do not force false equivalence.
- Metrics: true session count, p50/p95/p99/p99.9, CPU, RSS+kernel socket bytes, egress, fsync, Raft commit, rejects/drops, delivered message set, recovery time.
- At minimum three independent exploratory repetitions with raw artifacts. Full normative H1 uses 10min warmup/30min steady/five runs and appropriate fault repeats, later.

## Command conventions (commands implemented by tasks)

~~~text
cargo test -p rovqet-protocol
cargo test -p rovqet-wal
cargo test -p rovqet-durable
cargo test -p rovqet-consensus
cargo run -p rovqet-testkit -- q0-load --publishers 1 --subscribers 1000 --messages 20 --rate 20 --seed 42 --output /tmp/q0-new-run
cargo run -p rovqet-testkit -- q1-restart --seed 42
cargo run -p rovqet-testkit -- q2-failover --seed 42
~~~

The Q0 load command is implemented; see [benchmark instructions](../../benchmarks/README.md)
for profiles, resource sampling and JSONL replay. Q1/Q2 commands remain planned
interfaces and are not implemented. Current exploratory evidence is in the
[Q0 report](../../benchmarks/evidence/q0-f9221b3/REPORT.md); it is not H1 qualification.

## Honest output schema

~~~json
{
  "git_sha": "actual commit",
  "scenario": "q1-restart",
  "environment": "hardware and OS fingerprint",
  "seed": 42,
  "offered": 1000,
  "admitted": 1000,
  "committed": 1000,
  "delivered": 1000,
  "rejected": 0,
  "lost_acknowledged": 0,
  "p99_ms": null,
  "notes": "sample, not full H1 certification"
}
~~~

Never write a measured number without actual execution; null/NOT_RUN is preferable to invention.
