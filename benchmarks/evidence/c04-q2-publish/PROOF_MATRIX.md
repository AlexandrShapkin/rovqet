# C-04 Q2 publication proof matrix

This matrix distinguishes recorded evidence from test availability. `PASS`
means the cited observation exists in a journal **or** in an executed test from
the passing workspace run recorded below; `NOT_RUN` means no verified
execution proves the **whole named property**. A Rust test cited as executed
proves only its own assertions — a test name alone, without the recorded
execution, would not be a result. Rows whose property needs a live
network/fault observation stay `NOT_RUN` when that observation is absent, even
when related unit tests pass. Oracle fixtures validate the checker, not the
broker. This is not a claim of complete C-04 acceptance.

References:

- Campaign: `scripts/test_q2_publish.py`; scenario numbers below are its numbers,
  not the row numbers in this matrix. The campaign requires the
  `fault-injection` build (`cargo build -p rovqet-server -p rovqet-sdk
  --examples --bins --features fault-injection --locked`); against the normal
  build the pre-majority/post-majority seam does not exist and the script
  correctly reports `NOT_RUN` (journal `q2-publish-not-run.jsonl`).
- Journal: `benchmarks/evidence/c04-q2-publish/q2-publish.jsonl`; `seq` denotes
  the journal sequence field, not a producer sequence.
- Oracle: `scripts/q1_oracle.py::reconcile_q2_publish`. The recorded
  `oracle_verdict` at seq 261 is `PASS`: 14 scenarios, 11 committed
  observations, 11 majority proofs, 2 rejections, 3 unknown outcomes, 63 voter
  states (counts re-read from the journal). Majority witnesses include the
  committed payload bytes, identity, applied index and partition chain. Unknown
  outcomes are not successful ACKs.
- Q2 Raft campaign: `scripts/test_q2_raft.py`, 18 scenarios, all `PASS`;
  journals `benchmarks/evidence/c03-q2-raft/scenario-01.jsonl` … `scenario-18.jsonl`
  (every journal re-checked to end in `"verdict": "PASS"`).
- Rust execution: `cargo fmt --all -- --check`, `cargo check --workspace --locked`
  and `cargo test --workspace --locked` all passed (consensus 40 unit + 1
  integration, SDK 21, server 77; workspace green). Rust tests cited below are
  from that executed run.
- Oracle suite: `python3 scripts/test_q1_oracle.py` passed all 59 tests.
  `python3 scripts/check_docs.py` passed.

| # | Property | Evidence type | Exact scenario/test/fixture reference | Status | Remaining caveat |
| --- | --- | --- | --- | --- | --- |
| 1 | Healthy publication | live network/crash + Rust test | Campaign scenario 2 `committed-quorum-publish`; journal seq 6–17; oracle seq 261; `crates/server/src/q2.rs::a_committed_publication_reports_the_quorum_outcome_and_replays_it` (executed) | PASS | Loopback prototype campaign, not production transport or load coverage. |
| 2 | Sequential publications | live network/crash + Rust test | Scenario 4 `sequence-and-offset-progress`, seq 29–39; scenario 10 publication seq 118–126; `crates/consensus/src/sm.rs::sequential_publications_receive_logical_offsets_zero_one_two_and_never_a_raft_index` (executed) | PASS | Bounded sequence (three publications in the Rust test, a short campaign), not long-running or concurrent producer load. |
| 3 | Two partitions sharing one physical group | Rust test | `crates/consensus/src/sm.rs::two_partitions_in_one_group_have_independent_offset_sequences`; `crates/consensus/src/sm.rs::two_partitions_in_one_batch_keep_independent_counters`; `crates/consensus/tests/three_voters.rs::a_three_voter_shard_group_replicates_and_recovers` (three commands over two partitions through one shard group, executed) | PASS | State-machine/shard-group level proof; no verified live two-partition publication campaign (the C-04 campaign journal is single-partition). |
| 4 | Independent logical offsets | Rust test | `crates/consensus/src/sm.rs::two_partitions_in_one_group_have_independent_offset_sequences` (each partition counts its own offsets from 0); `crates/consensus/tests/three_voters.rs::a_three_voter_shard_group_replicates_and_recovers` (the first command of each partition is its first applied command); `crates/durable/tests/producer_model.rs::same_tokens_in_independent_partitions_and_principals_recover_independently` (executed) | PASS | Unit/integration proof of cross-partition independence; not observed in the live single-partition campaign. |
| 5 | Identical token retry | live network/crash + Rust test | Scenario 3 `idempotent-retry-same-offset`, seq 18–28; oracle seq 261 reports one repeated committed observation; `crates/consensus/src/sm.rs::the_same_request_id_is_idempotent_and_never_double_counts`; `crates/server/src/q2.rs::a_committed_publication_reports_the_quorum_outcome_and_replays_it` (retry reports the original offset and consumes no second offset, executed) | PASS | Does not by itself establish retry after compaction; snapshot-level dedup is row 17. |
| 6 | Older token retry after intervening publications | Rust test | `crates/consensus/src/sm.rs::an_identical_retry_of_an_older_token_returns_the_original_offset_without_a_second_message` (the original token replayed after two intervening commits returns the original offset with no second message, executed) | PASS | State-machine replay proof; no live campaign retry of an older token after later tokens committed. |
| 7 | Same token conflicting content | Rust test + live related | `crates/consensus/src/sm.rs::the_same_token_with_a_different_payload_digest_is_a_conflict`; `crates/durable/tests/partition.rs::retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped` (changed payload or message id yields `SequenceConflict`); `crates/durable/tests/timed_dedup.rs::retry_storm_has_one_durable_append_and_content_conflicts_stay_distinct` (all executed); related live scenario 5 `sequence-conflict-rejection`, seq 40–57 | PASS | Conflicting content is refused as a conflict while the committed token keeps its dedup state (executed tests); the live scenario name alone still does not prove the conflict-versus-definite-rejection distinction. |
| 8 | Sequence gap rejection | Rust test | `crates/consensus/src/sm.rs::a_sequence_gap_and_a_fenced_producer_epoch_are_refused` (an unknown producer not starting at 0 and a skipped sequence are rejected `SequenceGap` and consume no offset); `crates/durable/tests/partition.rs::retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped` (`SequenceGap`) (executed) | PASS | Executed admission-rejection proof, not a live network rejection observation. |
| 9 | Stale producer epoch rejection | Rust test + live related | `crates/consensus/src/sm.rs::a_sequence_gap_and_a_fenced_producer_epoch_are_refused` (after a newer epoch commits, the old epoch is `ProducerFenced` even at sequence 0); `crates/durable/tests/partition.rs::retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped` (old epoch stays fenced across shutdown/reopen) (executed); related live scenario 6 `stale-ownership-epoch-rejection`, seq 58–72 | PASS | Ownership epoch is not producer epoch: the campaign still supplies producer epoch 0, so producer-epoch fencing rests on the executed Rust tests, not a live epoch transition. |
| 10 | New epoch accepted/durable fencing | Rust test | `crates/consensus/src/sm.rs::a_newer_producer_epoch_starts_at_zero_and_fences_the_older_epoch`; `crates/durable/tests/partition.rs::retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped` (the renewed epoch commits and the old epoch remains fenced after reopen); `crates/durable/tests/producer_model.rs::same_tokens_in_independent_partitions_and_principals_recover_independently` (new epoch commits across restarts) (executed) | PASS | No verified live producer-epoch transition followed by broker restart; fencing durability is proven at the durable/state-machine layer only. |
| 11 | Leader restart preserving committed state | live network/crash | Scenario 10 `owner-crash-after-quorum`, seq 112–127; kill seq 114, restart seq 116, retained chain and payload comparison seq 117 | PASS | Covers the campaign's owner/leader arrangement, not arbitrary simultaneous failures. |
| 12 | One unavailable follower with 2/3 majority | live network/crash | Scenario 13 `one-unavailable-follower-quorum`, seq 165–195: leader node 1 observed among live voters (seq 166–167); follower node 2 stopped so exactly one follower is unavailable while the leader runs, live `[1,3]` (seq 168–170); publication `message_id` 301 (producer 22) commits `committed_quorum` at logical offset 6 (seq 171–172); the SDK acknowledgment is recorded with the follower down (seq 173–176); both available voters' states carry the record exactly once (seq 177–179); node 2 restarts and catches up to the same committed state (seq 180, 193–194); conclusion `PASS` seq 195. Related: q2-raft scenario 6 `one-follower-unavailable` (`benchmarks/evidence/c03-q2-raft/scenario-06.jsonl`, PASS) and campaign scenarios 8–9, seq 88–111 | PASS | Loopback prototype campaign: exactly one follower unavailable across the observed publication window, not an arbitrary failure schedule or production transport. |
| 13 | Loss of majority prevents ACK | live network/crash | Scenario 8 `lost-quorum-no-false-commit`, seq 88–98; scenario 9 settles the uncertain request; related q2-raft scenario 7 `two-voters-unavailable` (PASS); oracle seq 261 | PASS | Demonstrates no false COMMITTED_QUORUM in the observed outage, not a latency guarantee. |
| 14 | Leader crash before majority | live network/crash | Scenario 12 `leader-crash-before-majority`, seq 137–164; barrier seq 142, pre-kill voter observations seq 146–149, kill seq 150, unknown seq 152, retry commit seq 156, exactly-once evidence seq 163 | PASS | Barrier identifies node 1, log_index=10, durable_index=10, producer 21/epoch 0/sequence 0/message 300. No voter had committed it before kill; retry commits exactly once at offset 5. This is an armed prototype barrier, not a timing-only inference. |
| 15 | Leader crash after majority before response | live network/crash | Scenario 14 `leader-crash-after-majority-before-response`, seq 196–256: all brokers stopped before the seam was armed, then restarted (seq 197–203) with a warm-up commit (seq 204–211); `postmajority_barrier` armed for `message_id` 302 (producer 23, logical offset 8, `committed_index` 14, barrier node 1) at seq 213, and the barrier names the committed index, the logical offset and a live voter that committed the entry (seq 214–216); every running voter carries the barrier message at the barrier offset before the kill (seq 217–220) and no acknowledgment for the armed token reached the client before the kill (seq 221); leader node 1 SIGKILLed after the barrier (seq 222–223); the interrupted call reports `OutcomeUnknown` and never `COMMITTED_QUORUM` (seq 224–225); after full-group restart every voter retained the committed prefix (seq 229–237); the retry commits exactly once at offset 8 (seq 238–246) and a second retry reports the same offset with no duplicate (seq 247–255); conclusion `PASS` seq 256. Related: campaign scenario 10 `owner-crash-after-quorum`, seq 112–127; q2-raft scenario 16 `killed-leader-uncertain-commit` (PASS) | PASS | Armed prototype barrier: majority is shown by the barrier's committed index plus the pre-kill voter states, and the unanswered window by the recorded absence of a pre-kill acknowledgment; no timing-only inference is claimed. |
| 16 | Uncertain-response retry | live network/crash | Scenarios 8–9 `lost-quorum-no-false-commit` / `quorum-restored-retry-settles`, seq 88–111; scenario 12 unknown seq 152 and settled retry seq 156–163 | PASS | Unknown is preserved as unknown; this does not prove row 15's separate post-majority/pre-response crash window. |
| 17 | Snapshot/compaction dedup | Rust test + live related | `crates/consensus/src/sm.rs::a_snapshot_round_trip_restores_the_whole_publication_state` (producers, outcomes, logical offsets and retained payload bytes survive snapshot install; an identical retry of an older token after install returns the original offset); `crates/consensus/src/sm.rs::a_snapshot_that_lost_its_committed_payload_bytes_is_refused`; `crates/consensus/src/sm.rs::recovery_refuses_a_state_whose_committed_payload_bytes_are_gone`; `crates/metadata-raft/tests/snapshots.rs::an_interrupted_publication_keeps_the_previous_snapshot`; `crates/metadata-raft/tests/snapshots.rs::a_corrupted_published_snapshot_is_never_served` (all executed); related live q2-raft scenario 11 `snapshot-compaction` (PASS) | PASS | Executed snapshot round-trip proof of dedup and payload retention; the metadata-snapshot tests cover publication-state safety of snapshots, not Q2 payloads directly. |
| 18 | Lagging follower snapshot install | live network/crash + Rust test | q2-raft scenario 12 `lagging-follower-snapshot-install`, `benchmarks/evidence/c03-q2-raft/scenario-12.jsonl` PASS (log compacted past the stopped follower's applied index, a snapshot install event observed on its restart, follower partition chain and applied index match the leader); `crates/metadata-raft/tests/three_voters.rs::a_lagging_follower_catches_up_after_log_compaction` (executed) | PASS | The live scenario re-compares partition count, chain and applied index after install; producer-outcome dedup after a live install is covered only by the Rust round-trip test (row 17), not re-published live. |
| 19 | Stale/expired metadata rejected | live network/crash + Rust test | Scenario 6 `stale-ownership-epoch-rejection`, seq 58–72; `crates/server/src/q2.rs::a_stale_ownership_epoch_creates_no_raft_entry` (executed); `crates/consensus/src/sm.rs::a_stale_metadata_epoch_is_rejected_but_still_advances_the_applied_index`; `crates/durable/src/authority.rs::a_fenced_partition_refuses_foreign_stale_and_expired_authority`; `crates/control/tests/inter_node.rs::expired_authority_fails_closed` (all executed) | PASS | Stale ownership/epoch is live-observed; expired authority is proven fail-closed by executed Rust tests, not by a live expired-lease campaign scenario. |
| 20 | Wrong durable class rejected | Rust test | `crates/server/src/q2.rs::a_qos1_publication_to_the_q2_topic_is_refused` (executed in the passing server suite); `crates/durable/tests/partition.rs::incompatible_class_partition_and_owner_are_rejected_before_storage` (executed) | PASS | No matching live campaign scenario; unit/integration refusal proof only. |
| 21 | Oversized payload refused | Rust test | `crates/server/src/q2.rs::an_oversized_payload_is_refused` (executed); `crates/consensus/src/sm.rs::the_inline_payload_bound_still_rejects_an_oversized_command`; `crates/wal/tests/storage.rs::oversized_and_out_of_order_records_are_rejected_before_writing` (executed) | PASS | No live oversized publication against a running campaign; refusal is proven before storage/append in executed tests. |
| 22 | Q1 WAL loss does not affect Q2 | live network/crash + Rust test | q2-raft scenario 18 `durability-domain-separation`, `benchmarks/evidence/c03-q2-raft/scenario-18.jsonl` PASS (the Q2 data directory holds no Q1 `MANIFEST`/`segment-*` files and no metadata `MRL1` log; a live Q1 broker publish leaves every voter's Q2 committed state unchanged; the Q1 directory holds its own `MANIFEST` and no `QRL1` log); `crates/server/src/main.rs::q2_needs_cluster_mode_and_a_topic_distinct_from_q1` (executed) | PASS | Domain separation is observed (disjoint directories, distinct log magics, unchanged Q2 state under active Q1 writes); no literal Q1 WAL deletion was injected — non-interference follows from the observed disjoint storage domains, not from an observed loss event. |
| 23 | Resource exhaustion rejection preserving dedup | Rust test | `crates/consensus/src/sm.rs::the_producer_and_retention_bounds_refuse_admission_but_keep_deduplicating` (`ResourceExhausted` refusal while an existing producer keeps deduplicating; the outcome bound refuses a new sequence and never evicts a retained one); `crates/durable/tests/partition.rs::outcome_quota_fails_closed_without_eviction_and_retry_still_succeeds` (`Capacity` rejection, same-token retry returns the same outcome, also after reopen); `crates/durable/tests/partition.rs::queue_byte_budget_rejects_before_append_and_releases_after_result`; `crates/wal/tests/storage.rs::queue_count_and_byte_exhaustion_are_bounded` (all executed) | PASS | Executed bound/fail-closed proof; no live exhausted-resource rejection followed by a same-token retry over the network. |

## Scope of the result

All twenty-three properties now have recorded executed evidence — from the live
C-04 campaign journal (14 scenarios), the passing `cargo test --workspace
--locked` run, or the passing 18-scenario Q2 Raft campaign; no row is
`NOT_RUN`. The two formerly `NOT_RUN` properties are closed by live
observations, not by re-labeling: row 12 by campaign scenario 13
`one-unavailable-follower-quorum` (a **Q2 publication** acknowledged with
exactly one follower unavailable, closing the gap where the raft scenario
commits only an Inline command) and row 15 by campaign scenario 14
`leader-crash-after-majority-before-response` (an armed post-majority/
pre-response barrier, the window neither scenario 10 nor q2-raft scenario 16
arms). The live PASS for campaign scenario 12 must still not be downgraded to
an absent/timing-only experiment, nor used as a substitute for the distinct
after-majority/before-response window of row 15, which scenario 14 arms
directly.

Rust `PASS` entries prove the assertions of the cited tests in the executed
workspace run; unless the row also cites a campaign or q2-raft journal they are
not live network/fault observations, and each such row's caveat says what remains
unobserved.

Validation recorded while preparing this revision:

- `cargo fmt --all -- --check` — passed (exit 0, re-run for this revision).
- `cargo check --workspace --locked` — passed.
- `cargo test --workspace --locked` — passed (399 tests, 0 failed, 0 ignored).
- `cargo build -p rovqet-server -p rovqet-sdk --examples --bins --features
  fault-injection --locked` — passed (exit 0).
- `python3 scripts/test_q2_publish.py` (fault-injection build) — exit 0; all
  14 scenarios PASS, including scenario 13 `one-unavailable-follower-quorum`
  (row 12, seq 165–195) and scenario 14
  `leader-crash-after-majority-before-response` (row 15, seq 196–256); 47
  embedded oracle fixture tests OK; journal `oracle_verdict` seq 261 PASS
  (`{"attempts": 11, "committed": 11, "majority_proofs": 11, "rejected": 2,
  "retries": 2, "scenarios": 14, "unknown": 3, "voter_states": 63}`; counts
  re-read from `q2-publish.jsonl`, which contains 14 `scenario_started`
  records). Against the normal (non-fault) build the same command reports
  `NOT_RUN` (`q2-publish-not-run.jsonl`) — the seam is feature-gated, so the
  campaign must run on the fault build.
- `python3 scripts/test_q2_raft.py` — 18/18 scenarios PASS; every
  `benchmarks/evidence/c03-q2-raft/scenario-NN.jsonl` re-checked to end in
  `"verdict": "PASS"`.
- `python3 scripts/test_q1_oracle.py` — 59 tests passed (exit 0).
- `python3 scripts/check_docs.py` — passed (42 markdown files, 36 roadmap
  tasks).

The recorded journal oracle PASS and the passing test runs are narrower than
all 23 properties passing as live observations; row 22's PASS rests on observed
domain separation rather than an injected Q1 WAL loss.
