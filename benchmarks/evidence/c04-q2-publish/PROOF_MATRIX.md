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
  not the row numbers in this matrix.
- Journal: `benchmarks/evidence/c04-q2-publish/q2-publish.jsonl`; `seq` denotes
  the journal sequence field, not a producer sequence.
- Oracle: `scripts/q1_oracle.py::reconcile_q2_publish`. The recorded
  `oracle_verdict` at seq 169 is `PASS`: 12 scenarios, 7 committed observations,
  7 majority proofs, 2 rejections, 2 unknown outcomes, 34 voter states
  (counts re-read from the journal). Majority witnesses include the committed
  payload bytes, identity, applied index and partition chain. Unknown outcomes
  are not successful ACKs.
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
| 1 | Healthy publication | live network/crash + Rust test | Campaign scenario 2 `committed-quorum-publish`; journal seq 6–17; oracle seq 169; `crates/server/src/q2.rs::a_committed_publication_reports_the_quorum_outcome_and_replays_it` (executed) | PASS | Loopback prototype campaign, not production transport or load coverage. |
| 2 | Sequential publications | live network/crash + Rust test | Scenario 4 `sequence-and-offset-progress`, seq 29–39; scenario 10 publication seq 118–126; `crates/consensus/src/sm.rs::sequential_publications_receive_logical_offsets_zero_one_two_and_never_a_raft_index` (executed) | PASS | Bounded sequence (three publications in the Rust test, a short campaign), not long-running or concurrent producer load. |
| 3 | Two partitions sharing one physical group | Rust test | `crates/consensus/src/sm.rs::two_partitions_in_one_group_have_independent_offset_sequences`; `crates/consensus/src/sm.rs::two_partitions_in_one_batch_keep_independent_counters`; `crates/consensus/tests/three_voters.rs::a_three_voter_shard_group_replicates_and_recovers` (three commands over two partitions through one shard group, executed) | PASS | State-machine/shard-group level proof; no verified live two-partition publication campaign (the C-04 campaign journal is single-partition). |
| 4 | Independent logical offsets | Rust test | `crates/consensus/src/sm.rs::two_partitions_in_one_group_have_independent_offset_sequences` (each partition counts its own offsets from 0); `crates/consensus/tests/three_voters.rs::a_three_voter_shard_group_replicates_and_recovers` (the first command of each partition is its first applied command); `crates/durable/tests/producer_model.rs::same_tokens_in_independent_partitions_and_principals_recover_independently` (executed) | PASS | Unit/integration proof of cross-partition independence; not observed in the live single-partition campaign. |
| 5 | Identical token retry | live network/crash + Rust test | Scenario 3 `idempotent-retry-same-offset`, seq 18–28; oracle seq 169 reports one repeated committed observation; `crates/consensus/src/sm.rs::the_same_request_id_is_idempotent_and_never_double_counts`; `crates/server/src/q2.rs::a_committed_publication_reports_the_quorum_outcome_and_replays_it` (retry reports the original offset and consumes no second offset, executed) | PASS | Does not by itself establish retry after compaction; snapshot-level dedup is row 17. |
| 6 | Older token retry after intervening publications | Rust test | `crates/consensus/src/sm.rs::an_identical_retry_of_an_older_token_returns_the_original_offset_without_a_second_message` (the original token replayed after two intervening commits returns the original offset with no second message, executed) | PASS | State-machine replay proof; no live campaign retry of an older token after later tokens committed. |
| 7 | Same token conflicting content | Rust test + live related | `crates/consensus/src/sm.rs::the_same_token_with_a_different_payload_digest_is_a_conflict`; `crates/durable/tests/partition.rs::retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped` (changed payload or message id yields `SequenceConflict`); `crates/durable/tests/timed_dedup.rs::retry_storm_has_one_durable_append_and_content_conflicts_stay_distinct` (all executed); related live scenario 5 `sequence-conflict-rejection`, seq 40–57 | PASS | Conflicting content is refused as a conflict while the committed token keeps its dedup state (executed tests); the live scenario name alone still does not prove the conflict-versus-definite-rejection distinction. |
| 8 | Sequence gap rejection | Rust test | `crates/consensus/src/sm.rs::a_sequence_gap_and_a_fenced_producer_epoch_are_refused` (an unknown producer not starting at 0 and a skipped sequence are rejected `SequenceGap` and consume no offset); `crates/durable/tests/partition.rs::retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped` (`SequenceGap`) (executed) | PASS | Executed admission-rejection proof, not a live network rejection observation. |
| 9 | Stale producer epoch rejection | Rust test + live related | `crates/consensus/src/sm.rs::a_sequence_gap_and_a_fenced_producer_epoch_are_refused` (after a newer epoch commits, the old epoch is `ProducerFenced` even at sequence 0); `crates/durable/tests/partition.rs::retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped` (old epoch stays fenced across shutdown/reopen) (executed); related live scenario 6 `stale-ownership-epoch-rejection`, seq 58–72 | PASS | Ownership epoch is not producer epoch: the campaign still supplies producer epoch 0, so producer-epoch fencing rests on the executed Rust tests, not a live epoch transition. |
| 10 | New epoch accepted/durable fencing | Rust test | `crates/consensus/src/sm.rs::a_newer_producer_epoch_starts_at_zero_and_fences_the_older_epoch`; `crates/durable/tests/partition.rs::retry_identity_conflicts_gaps_and_producer_fencing_remain_scoped` (the renewed epoch commits and the old epoch remains fenced after reopen); `crates/durable/tests/producer_model.rs::same_tokens_in_independent_partitions_and_principals_recover_independently` (new epoch commits across restarts) (executed) | PASS | No verified live producer-epoch transition followed by broker restart; fencing durability is proven at the durable/state-machine layer only. |
| 11 | Leader restart preserving committed state | live network/crash | Scenario 10 `owner-crash-after-quorum`, seq 112–127; kill seq 114, restart seq 116, retained chain and payload comparison seq 117 | PASS | Covers the campaign's owner/leader arrangement, not arbitrary simultaneous failures. |
| 12 | One unavailable follower with 2/3 majority | live network/crash | Related: q2-raft scenario 6 `one-follower-unavailable` (`benchmarks/evidence/c03-q2-raft/scenario-06.jsonl`, PASS) commits an **Inline raft command** with one follower down; campaign scenarios 8 `lost-quorum-no-false-commit` and 9 `quorum-restored-retry-settles`, seq 88–111 | NOT_RUN | No verified **Q2 publication** acknowledged while exactly one follower is unavailable; the raft scenario's Inline command is not a publish, and quorum loss/restoration does not establish this property. |
| 13 | Loss of majority prevents ACK | live network/crash | Scenario 8 `lost-quorum-no-false-commit`, seq 88–98; scenario 9 settles the uncertain request; related q2-raft scenario 7 `two-voters-unavailable` (PASS); oracle seq 169 | PASS | Demonstrates no false COMMITTED_QUORUM in the observed outage, not a latency guarantee. |
| 14 | Leader crash before majority | live network/crash | Scenario 12 `leader-crash-before-majority`, seq 137–164; barrier seq 142, pre-kill voter observations seq 146–149, kill seq 150, unknown seq 152, retry commit seq 156, exactly-once evidence seq 163 | PASS | Barrier identifies node 1, log_index=10, durable_index=10, producer 21/epoch 0/sequence 0/message 300. No voter had committed it before kill; retry commits exactly once at offset 5. This is an armed prototype barrier, not a timing-only inference. |
| 15 | Leader crash after majority before response | live network/crash | Related: campaign scenario 10 `owner-crash-after-quorum`, seq 112–127 (kills after an already observed committed prefix); q2-raft scenario 16 `killed-leader-uncertain-commit` (PASS, interrupted append with no publication barrier) | NOT_RUN | No verified majority-acknowledged-but-unanswered publication window; neither cited run arms a post-majority/pre-response barrier. |
| 16 | Uncertain-response retry | live network/crash | Scenarios 8–9 `lost-quorum-no-false-commit` / `quorum-restored-retry-settles`, seq 88–111; scenario 12 unknown seq 152 and settled retry seq 156–163 | PASS | Unknown is preserved as unknown; this does not prove row 15's separate post-majority/pre-response crash window. |
| 17 | Snapshot/compaction dedup | Rust test + live related | `crates/consensus/src/sm.rs::a_snapshot_round_trip_restores_the_whole_publication_state` (producers, outcomes, logical offsets and retained payload bytes survive snapshot install; an identical retry of an older token after install returns the original offset); `crates/consensus/src/sm.rs::a_snapshot_that_lost_its_committed_payload_bytes_is_refused`; `crates/consensus/src/sm.rs::recovery_refuses_a_state_whose_committed_payload_bytes_are_gone`; `crates/metadata-raft/tests/snapshots.rs::an_interrupted_publication_keeps_the_previous_snapshot`; `crates/metadata-raft/tests/snapshots.rs::a_corrupted_published_snapshot_is_never_served` (all executed); related live q2-raft scenario 11 `snapshot-compaction` (PASS) | PASS | Executed snapshot round-trip proof of dedup and payload retention; the metadata-snapshot tests cover publication-state safety of snapshots, not Q2 payloads directly. |
| 18 | Lagging follower snapshot install | live network/crash + Rust test | q2-raft scenario 12 `lagging-follower-snapshot-install`, `benchmarks/evidence/c03-q2-raft/scenario-12.jsonl` PASS (log compacted past the stopped follower's applied index, a snapshot install event observed on its restart, follower partition chain and applied index match the leader); `crates/metadata-raft/tests/three_voters.rs::a_lagging_follower_catches_up_after_log_compaction` (executed) | PASS | The live scenario re-compares partition count, chain and applied index after install; producer-outcome dedup after a live install is covered only by the Rust round-trip test (row 17), not re-published live. |
| 19 | Stale/expired metadata rejected | live network/crash + Rust test | Scenario 6 `stale-ownership-epoch-rejection`, seq 58–72; `crates/server/src/q2.rs::a_stale_ownership_epoch_creates_no_raft_entry` (executed); `crates/consensus/src/sm.rs::a_stale_metadata_epoch_is_rejected_but_still_advances_the_applied_index`; `crates/durable/src/authority.rs::a_fenced_partition_refuses_foreign_stale_and_expired_authority`; `crates/control/tests/inter_node.rs::expired_authority_fails_closed` (all executed) | PASS | Stale ownership/epoch is live-observed; expired authority is proven fail-closed by executed Rust tests, not by a live expired-lease campaign scenario. |
| 20 | Wrong durable class rejected | Rust test | `crates/server/src/q2.rs::a_qos1_publication_to_the_q2_topic_is_refused` (executed in the passing server suite); `crates/durable/tests/partition.rs::incompatible_class_partition_and_owner_are_rejected_before_storage` (executed) | PASS | No matching live campaign scenario; unit/integration refusal proof only. |
| 21 | Oversized payload refused | Rust test | `crates/server/src/q2.rs::an_oversized_payload_is_refused` (executed); `crates/consensus/src/sm.rs::the_inline_payload_bound_still_rejects_an_oversized_command`; `crates/wal/tests/storage.rs::oversized_and_out_of_order_records_are_rejected_before_writing` (executed) | PASS | No live oversized publication against a running campaign; refusal is proven before storage/append in executed tests. |
| 22 | Q1 WAL loss does not affect Q2 | live network/crash + Rust test | q2-raft scenario 18 `durability-domain-separation`, `benchmarks/evidence/c03-q2-raft/scenario-18.jsonl` PASS (the Q2 data directory holds no Q1 `MANIFEST`/`segment-*` files and no metadata `MRL1` log; a live Q1 broker publish leaves every voter's Q2 committed state unchanged; the Q1 directory holds its own `MANIFEST` and no `QRL1` log); `crates/server/src/main.rs::q2_needs_cluster_mode_and_a_topic_distinct_from_q1` (executed) | PASS | Domain separation is observed (disjoint directories, distinct log magics, unchanged Q2 state under active Q1 writes); no literal Q1 WAL deletion was injected — non-interference follows from the observed disjoint storage domains, not from an observed loss event. |
| 23 | Resource exhaustion rejection preserving dedup | Rust test | `crates/consensus/src/sm.rs::the_producer_and_retention_bounds_refuse_admission_but_keep_deduplicating` (`ResourceExhausted` refusal while an existing producer keeps deduplicating; the outcome bound refuses a new sequence and never evicts a retained one); `crates/durable/tests/partition.rs::outcome_quota_fails_closed_without_eviction_and_retry_still_succeeds` (`Capacity` rejection, same-token retry returns the same outcome, also after reopen); `crates/durable/tests/partition.rs::queue_byte_budget_rejects_before_append_and_releases_after_result`; `crates/wal/tests/storage.rs::queue_count_and_byte_exhaustion_are_bounded` (all executed) | PASS | Executed bound/fail-closed proof; no live exhausted-resource rejection followed by a same-token retry over the network. |

## Scope of the result

Twenty-one of the twenty-three properties now have recorded executed evidence —
from the live C-04 campaign journal, the passing `cargo test --workspace
--locked` run, or the passing 18-scenario Q2 Raft campaign. Two remain
`NOT_RUN` under the whole-property rule: row 12 (no live **Q2 publication**
with exactly one follower unavailable; the raft scenario commits only an Inline
command) and row 15 (no observed majority-before-response publication window).
In particular, the live PASS for campaign scenario 12 must not be downgraded to
an absent/timing-only experiment, nor used as a substitute for the distinct
after-majority/before-response window of row 15.

Rust `PASS` entries prove the assertions of the cited tests in the executed
workspace run; unless the row also cites a campaign or q2-raft journal they are
not live network/fault observations, and each such row's caveat says what remains
unobserved.

Validation recorded while preparing this revision:

- `cargo fmt --all -- --check` — passed.
- `cargo check --workspace --locked` — passed.
- `cargo test --workspace --locked` — passed (consensus 40 unit + 1
  integration, SDK 21, server 77; workspace green).
- `python3 scripts/test_q2_publish.py` — all 12 scenarios PASS; journal
  `oracle_verdict` seq 169 PASS (7 committed, 7 majority proofs, 2 rejected,
  2 unknown, 34 voter states; counts re-read from `q2-publish.jsonl`, which
  contains 12 `scenario_started` records).
- `python3 scripts/test_q2_raft.py` — 18/18 scenarios PASS; every
  `benchmarks/evidence/c03-q2-raft/scenario-NN.jsonl` re-checked to end in
  `"verdict": "PASS"`.
- `python3 scripts/test_q1_oracle.py` — 59 tests passed.
- `python3 scripts/check_docs.py` — passed.

The recorded journal oracle PASS and the passing test runs are narrower than
all 23 properties passing as live observations; rows 12 and 15 remain
`NOT_RUN`, and row 22's PASS rests on observed domain separation rather than an
injected Q1 WAL loss.
