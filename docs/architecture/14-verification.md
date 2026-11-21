# 14 — Verification, reproducible evidence and test oracle

## Two acceptance tracks

**Research MVP:** deterministic smoke + relevant integrity tests + measured density/latency against a baseline. Fast enough for each component task. Results state their coverage and do not claim full product certification.

**Product v1:** TEST-001…TEST-017, REL-01…REL-07, PERF-D1/D2/D3/R1/P1/M1/C1, VAL-01, normalized H1 rig, multi-run statistical evidence, security/soak/production packaging from normative requirements.

## Independent oracle design

Test harness on a separate process/host records append-only JSONL, independent of broker durable state.

~~~json
{"event":"publish_sent","run":"r1","message_id":"...","producer":"...","epoch":1,"sequence":0,"qos":"Q1","partition":"...","digest":"...","time_mono_ns":123}
{"event":"publish_ack","run":"r1","message_id":"...","outcome":"COMMITTED_LOCAL","incarnation":"...","offset":0,"time_mono_ns":456}
{"event":"delivery","run":"r1","message_id":"...","group":"g","generation":3,"lease":"...","offset":0}
{"event":"consumer_ack","run":"r1","group":"g","offset":0,"outcome":"ACK_COMMITTED"}
{"event":"fault","run":"r1","kind":"process_kill","node":"n1","at":"after_fsync"}
{"event":"recovered_record","run":"r1","node":"n1","message_id":"...","digest":"..."}
~~~

Use real monotonic timing for same-host and synchronized-clock methodology for E2E across hosts. Never average percentiles. The oracle computes set-difference of committed IDs vs recovered records, divergent digest, unintended duplicate committed record, offset reincarnation, Q2 no-quorum ACK, cursor skips and replay gaps. An uncertain client outcome is not automatically classified as lost.

## Test fixture naming

\`unit::<crate>::<contract>\`, \`integration::q0::<scenario>\`, \`recovery::q1::<barrier>\`, \`cluster::q2::<fault>\`, \`group::<scenario>\`, \`bench::<profile>\`. Each unit of implementation includes at least one negative test: not only the happy path.

## Mandatory research gates

| Gate | Goal | Proving fixture | Failure action |
|---|---|---|---|
| D0 | architecture complete enough for coding | doc checker, index and trace review | repair contract once, not recreate architecture per crate |
| A | Q0 end-to-end | 1→N, slow subscriber, overload, 10k session smoke | repair routing/queue |
| B | Q1 durable local | crash before/after fsync + independent oracle | no Q1 claim until fixed |
| C | Q2 quorum | 3 nodes, leader loss, quorum loss, committed prefix | no Q2 claim until fixed |
| D | concept comparison | preregistered equivalent NATS profile, raw latency/resources | report negative result honestly |

Gate success does not require approval/review process; it means the engineer/agent has actually executed the behavioral fixture and kept the artifacts.

## Requirement crosswalk for MVP

| Requirement | Architecture chapter | Initial proof |
|---|---|---|
| FR-CON-001/003 | 03, 10 | 10k sessions, bounded memory |
| FR-TOP-002/003/008 | 05, 10 | 1:N, 1:1000 fan-out, slow client |
| FR-QOS-001/003/006/007 | 04, 06, 07 | wrong class rejected, ACK outcomes |
| FR-QOS-004 | 06, 07 | same sequence same digest/conflict/gap |
| FR-STO-001/003/005 | 06 | crash matrix + WAL recovered oracle |
| FR-GRP-003/006 | 08 | stale ACK, sparse closure, cursor |
| FR-CLU-002/004/005/011 | 07, 09 | fenced owner, Q2 quorum loss |
| NFR-RES-001/006 | 10 | offered/admitted vs memory sweep |
| INV-01/03/07/08/13/16 | 06–09, 11 | failure matrix |

Production evidence expands this mapping to every normative requirement, not just MVP.

## Measurements and baseline validity

Measure offered → admitted → committed → delivered rates and bytes separately. Count physical fan-out; reject/drop counts accompany p99. For Q0 compare to NATS Core at matching TLS/profile (or explicitly note local plaintext mismatch and prevent VAL-01 claim). For Q1/Q2 compare appropriately configured JetStream modes, equal replication/durability boundary where possible; if a mode is not semantically equivalent, report a limited observation, not an 'advantage'.

Record CPU, memory RSS, kernel socket memory, NIC, disk writes/fsync, pending queues, recovery time, p95/p99/p99.9 and total concurrent authenticated sessions. Keep raw histograms, seeds, exact command, hardware and source SHA. Early MVP results are exploratory; H1/VAL-01 acceptance occurs after product rig exists.

## CI philosophy

PR-free by default. On docs change: run doc checker and optionally diagram syntax check. On Rust change: formatting, cargo check and tests touching changed crate; on WAL/ACK/Raft: targeted crash/model tests. Do not run a 72-hour soak or generate release artifacts on every commit. Trigger large benchmark or compatibility suite only at corresponding milestone, manual dispatch or explicit performance-impacting change.

## B-07 Q1 durability and crash-recovery campaign

`scripts/test_q1_durability.py` executes the B gate with real external SDK processes. Each
campaign has its own `scripts/q1_oracle.py` journal: append-only JSONL written and fsynced
by the parent process outside the broker data directory before any deliberate termination.
The journal records publication attempts and received `COMMITTED_LOCAL` results, delivery
tokens, ACK attempts and received `ACK_COMMITTED` results, generation changes, faults and
restart observations. Broker WAL files are only supplementary; the independent journal is
authoritative for externally observed outcomes.

Reconciliation compares exact identities, not counts: each externally acknowledged
publication must be recovered with the same message ID, payload SHA-256, incarnation and
offset; repeated producer tokens must not create a second committed record; a conflicting
payload for a committed token must be rejected; sparse ACKs may not move the cursor past an
unattempted or unacknowledged application record; acknowledged offsets must not be
redelivered. A delivered record whose ACK reply was lost is classified as uncertain progress,
never as a skip or a durability failure.

Verdicts are `PASS`, `FAIL`, `INVALID`, `NOT_RUN`. Missing conclusions, truncated lines,
non-contiguous journal sequence numbers or missing post-recovery observations are `INVALID`;
an interrupted fixture can never yield a pass. `scripts/test_q1_oracle.py` feeds deliberately
corrupted evidence (missing records, changed digests, duplicate publications, wrong
offsets/incarnations, missing ACK state, cursor gaps, stale-generation commits, truncated
journals) and requires rejection.

The executed matrix covers publication crashes before append, after append/before sync and
after sync/before reply; consumer ACK crashes at the same barriers plus cursor-checkpoint
append/sync; generation-claim crash before the fence is durable; sparse out-of-order ACKs;
generation replacement; all seven WAL fault points across segment rotation; corrupt and
missing committed storage; Q0 isolation under a blocked Q1 storage barrier; and repeated
restart identity stability. Measurements include end-to-end publication/ACK latency, direct
WAL append/fsync latency, broker peak RSS and CPU jiffies, recovery readiness time and
recovered record counts. Q1 internal queue, lease and sparse-set occupancy have no public
metric endpoint and are reported `UNAVAILABLE`. Small samples are exploratory: p99.9 is
reported with its sample count and is not a production SLO claim.

A defect found by this campaign has been fixed: B-05 made application offsets sparse, but
the major-1 codec still rejected odd `READ_RECORD`/`READ_END` offsets, so replay after
interleaved consumer metadata returned `RESOURCE_EXHAUSTED`. The codec now accepts sparse
positions; the golden vectors are unchanged and a protocol regression pinned the behavior.

## B-06 retry and terminal-disposition campaign

`scripts/test_q1_terminal.py` drives real external SDK consumers through NACK, bounded
backoff, exhaustion, quarantine and a real DLQ partition. The parent journal records NACK
requests, retry scheduling, deliveries, terminal commits, DLQ publications and recovered
cursors. `q1_oracle.reconcile_terminal` rejects a DLQ terminal without a prior destination
publication, a missing or duplicated DLQ record, a payload-digest mismatch, a terminal for
an uncommitted source offset, cursor movement across an unexplained source record, and any
incomplete journal.

Deterministic barriers cover crash after the PENDING record append/before sync, after the
PENDING sync, after the DLQ commit/before the source terminal, and the committed terminal
append/sync windows. Recovery must produce exactly one DLQ record, complete the source
terminal only after its own barrier, and retain the original publication in every case.
Actor tests additionally cover bounded retry/terminal state, corrupt terminal metadata,
unavailable DLQ, sparse terminal closure and real destination readability and idempotence.

## C-02 fencing verification (executed part)

`crates/durable/tests/fencing.rs` and `scripts/test_cluster_routing.py` verify the ownership
fencing protocol at the durable commit boundary:

- a standalone partition is not fenced and keeps its previous behaviour;
- a partition whose committed authority names another node refuses publications, consumer
  deliveries, acknowledgements and NACKs;
- an expired ownership lease fails closed instead of committing;
- a newer authority fences the previous owner, and an older epoch can never replace it;
- across two real broker processes a publication to the non-owner is forwarded to the
  owner, the owner performs the durable commit and returns its own offset, and the same
  token retry returns the same offset without a second record;
- across two real broker processes an expired ownership lease stops the old owner from
  committing further records while its committed history stays readable.

The metadata Raft group, committed-metadata driven ownership transitions and the staged Q1
migration are exercised by the safety matrix below. The recorded stage of C-02 itself is
kept in `docs/roadmap/tasks.json`, not in this chapter.

## C-02 safety matrix (executed part)

The safety matrix executes the ownership handover of
[09-cluster-control.md](09-cluster-control.md) against a deterministic fault model: every
window is an exact operation boundary of the production code, not a simulation of one. The
window is reached, the process is killed or isolated there, and an independent journal of
externally observed results decides whether the transition stayed safe. The ownership,
staging, fencing and activation results of the campaign are recorded in
`docs/roadmap/tasks.json` and under `benchmarks/evidence/`; this section documents the
contract the matrix is required to prove.

### Deterministic seam model

A named barrier is an exact operation boundary of the production path. It exists only in a
build with the `fault-injection` feature: the process announces the barrier to the parent and
waits, and the parent kills it there. A normal build compiles the seam away, so no
release artifact can be paused or terminated at a barrier.

The handover barriers are:

- `chunk_before_send`, `chunk_after_write` (the staged bytes are written but not
  synchronized), `chunk_after_append` (the staged bytes are synchronized) and
  `chunk_after_sync` (the durable chunk receipt is journaled);
- `before_catch_up_commit`, the boundary between the received inventory and its committed
  preliminary evidence;
- `before_fence` and `after_fence`, before and after the durable fence of the old owner;
- `before_final_transfer`, the final post-fence delta;
- `before_activate_commit`, the committed activation;
- the promotion barriers of the paired promotion: the dependent inventory is promoted, the
  durable readiness is recorded, and the source inventory is promoted;
- the metadata barriers `after_migration_proposal` (a transition reached the leader but is
  not committed) and `after_migration_commit` (the transition is committed and applied, the
  confirmation is not delivered).

The metadata barrier fires only on the committed leader. A follower never holds a barrier, so
an isolated or slow follower cannot stall the group, and a transition that never reached the
leader has no window to exercise.

### Durable storage and transport controls

Two environment families control the durable storage of one isolated broker process.
`ROVQET_Q1_TEST_DIR`, `_STAGE`, `_SEQUENCE` and `_WAL_FAULT` select a disposable marker
directory, the exact durability stage, a one-shot sequence number and a WAL fault point for
the source partition; `ROVQET_Q1_DLQ_TEST_DIR`, `_STAGE`, `_SEQUENCE` and `_WAL_FAULT` are
the equivalent for the dependent DLQ partition. The control pauses at the selected stage,
writes a `reached-<stage>` marker and waits for the release marker, so the parent knows
exactly when the window is open and when to terminate the process.

The transport fault is directional and lives in `ROVQET_FAULT_BLOCK_FILE`: a marker file with
a comma separated list of peer node ids whose outgoing path is blackholed. The file is read
on every call, so the parent can isolate and restore a live process without restarting it.
Only the outgoing direction is affected; the client facing listener keeps serving, which is
what makes the asymmetric isolation case meaningful.

### Windows the matrix must cover

- the initial transfer interrupted before the target holds a committed inventory;
- a chunk written before its synchronization;
- a chunk synchronized with the acknowledgement lost;
- the finalize interrupted before the committed transition;
- the final post-fence delta interrupted;
- the first durable commit of the successor interrupted around the commit/response boundary;
- the metadata leader crashed with a transition pending, both before the commitment and
  after the commitment but before the confirmation.

### Live asymmetric isolation

The old owner is not killed in this case: it stays alive and keeps serving inside its still
valid authority, while the metadata majority stays reachable independently. The matrix must
show that a fabricated or foreign fence proof is refused, that the successor does not
activate while the old owner is unfenced, and that restoring the path lets the real fence and
the handover complete.

### Independent oracle

`q1_oracle.reconcile_safety` reconciles the scenario journal from externally observed results
only, never from a broker's own claim. A contradiction is `INVALID`, a broken safety
invariant is `FAIL` and a journal without evidence is `NOT_RUN`. The oracle requires that:

- every scenario reaches its barrier and declares its build identity; a missed barrier or a
  missing conclusion is a failure, never a pass;
- every killed process is restarted on its original storage;
- no acknowledged publication disappears from a history observed after the acknowledgement,
  and a producer token never changes its offset or its message identity;
- an uncertain outcome is never classified as a definite noncommit;
- the incarnation of an intact history never changes;
- no target commit precedes the committed activation;
- no old-owner commit follows its durable fence;
- no activation happens without a verified transfer of the source inventory and, when a
  bundle is moved, of the dependent inventory;
- the committed stage never moves backwards and never skips;
- a repeated committed transition never creates a second revision;
- no disposition is published twice, and every disposition is proved against its
  destination;
- the cursor never moves backwards, and the staged chunk receipts are unique per chunk
  identity with a stable digest and the expected count.

The lease-expiry fallback stays unsupported. A handover needs the durable fence of the actual
old owner; an expiry that merely lets the old owner stop committing is not accepted as a
substitute for observing that fence.

## C-03 Q2 Raft storage adapter (executed part)

The Q2 Raft adapter of [07-q2-consensus.md](07-q2-consensus.md) is exercised at two
levels: an in-process integration test of the library adapter, and a three-process
harness of the observable `q2-raft-node` process. Both record their observations in an
independent journal; the stage and results of C-03 itself are kept in
`docs/roadmap/tasks.json` and under `benchmarks/evidence/`, and this section documents
the contract the fixtures are required to prove.

### In-process integration contract

`crates/consensus/tests/three_voters.rs` drives the real node lifecycle — three voters
on free loopback ports with separate data directories, the bounded TCP transport and a
log compaction that forces a snapshot transfer — with every wait bounded by a deadline.
It covers:

- bootstrap and election: the designated bootstrap node initializes the membership, a
  leader is elected with a term, and the leader is a live voter;
- replication across partitions: commands submitted through a follower are forwarded to
  the leader, applied with increasing committed indexes, and every voter reaches the
  same revision with the same per-partition counts and chain digests;
- a stale-epoch refusal that creates no entry: a proposal carrying a foreign metadata
  epoch is refused before it becomes a Raft entry, and the last applied index does not
  move;
- leader death and replacement with the committed prefix intact: the surviving majority
  elects a new leader in a later term, and every survivor keeps the previously committed
  partitions unchanged while applying the new term's entries;
- a restarted voter catching up: a voter stopped earlier restarts from its original data
  directory and reaches the same committed view as the rest of the group;
- a voter recovering after the leader purged past its index: while a voter is down, the
  leader snapshots and compacts until its purge point is beyond that voter's last
  applied index, and the restarted voter then recovers the same committed chains through
  a snapshot installation rather than log replay.

### Three-process harness

`scripts/test_q2_raft.py` runs the acceptance scenarios against three real
`q2-raft-node` processes with three independently persisted data directories on
loopback. The parent process owns a journal per scenario, written and fsynced outside
the node data directories before any deliberate termination, so externally observed
outcomes survive the fault. The scenarios drive leadership changes, majority and
lost-quorum proposals, divergent uncommitted tails, follower catch-up, snapshot
compaction and installation, whole-cluster restart on the original directories,
disk-level corruption injected between restarts, an interrupted append, rejection of
corrupted and foreign snapshots, and the on-disk separation of the Q2 Raft log from the
Q1 WAL and the metadata Raft log. Faults are SIGKILL and restart on the original
directories, and a scenario that cannot reach its required state concludes with an
explicit reason instead of a silent success. Every wait is bounded over the captured
event history, and every child process is terminated in a `finally`.

Three processes on one host with three directories prove protocol behaviour against
independently persisted storage; they are **not** evidence about independent physical
failure domains, and no such claim is made from them.

### Independent oracle

`q1_oracle.reconcile_q2_raft` reconciles a scenario journal from externally observed
results only — the process lifecycle, the applied and recovered state each node
printed, the results a client received, the quorum it observed and the corruption it
injected — never from a broker's own claim that a command committed. Missing or
contradictory evidence is `INVALID`, a broken safety invariant is `FAIL` and a journal
without scenario or cluster evidence is `NOT_RUN`; a journal whose own conclusion is
not a success is never reported as one, and nothing is inferred from a clock. The
oracle requires that:

- a committed result needs a majority of distinct voters reporting the same index and
  chain for the same partition and request identity — one node's applied index is a
  claim, never a majority proof;
- a previously committed count never decreases across a restart of the same node;
- two observations with the same count carry the same chain, so equal counts must have
  equal chains and only an extended count may differ;
- an acknowledgement backed by a lone voter is never treated as a commit;
- a request classified as not committed never appears in any later committed state;
- injected corruption always has an observed outcome — a corruption whose outcome was
  never observed invalidates the journal instead of passing it;
- a snapshot never lowers a previously committed count and never changes a previously
  committed chain;
- no commit is accepted while the latest observed quorum record shows fewer voters
  available than a majority;
- the cluster and shard identity never change across a scenario: a recorded change is a
  failure, and an observation carrying no identity cannot contradict the baseline.

A state observation that carries no request identity cannot prove which proposal it
contains, so such a journal is `INVALID` rather than a pass: the majority
acknowledgement would be unverifiable.
