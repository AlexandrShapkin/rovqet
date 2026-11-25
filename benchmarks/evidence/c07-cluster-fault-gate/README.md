# C-07 — cluster fault gate evidence index

Executed evidence for the C-07 Q0/Q1/Q2 cluster fault gate. This directory is
the index for `fault-gate.jsonl` (parent-owned, fsynced observation journal),
`oracle.json` (independent reconciliation), and the metadata-authority notes in
`METADATA-AUTHORITY.md`.

## 1. Commit and command

- Source tested: `c28b23e3efce9deca7a16fa7e67bfa3f83271df2` (binaries built from
  that head; the reuse-citation and oracle-regeneration changes in this
  increment touch only the campaign's bookkeeping, not the four scenario bodies
  or their assertions).
- Run:

  ```sh
  cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked
  cargo build -p rovqet-metadata-raft --bin metadata-raft-node --locked
  python3 scripts/test_cluster_fault_gate.py
  ```

The campaign owns the journal, reconciles it after the scenarios, writes
`oracle.json`, and exits non-zero if any scenario or the reconciliation is not
PASS. The journal records 144 events and the run exits 0.

## 2. Environment

- Linux, loopback-only (`127.0.0.1`), plaintext research profile.
- Every listener gets an ephemeral port; all process storage is an independent
  disposable directory under a per-run temporary root.
- Process counts per scenario: the healthy baseline runs one broker (hosting the
  Q0 admission path plus the Q1 and Q2 routes), three real Q2 voter processes,
  one external Q0 publisher and two external Q0 subscribers, plus short-lived Q1
  publisher and Q2 consumer clients. The metadata scenarios run the broker as
  metadata voter 3 plus two metadata voter processes (nodes 1 and 2); no
  additional broker is started for the DLQ route.

## 3. Cluster configurations

- **Q2 data domain:** three voters (`[1, 2, 3]`), required majority 2. Each
  voter has its own persistent directory; the broker dials all three.
- **Broker:** one process with the Q0 admission path and one Q1 route
  (tenant 1, topic 3, partition 0, incarnation 42, owner epoch 7) and one Q2
  route (tenant 1, topic 4, partition 0, incarnation 42, owner epoch 5).
- **Live metadata cluster (F7a/F7b):** three-voter metadata Raft (two metadata
  voter processes plus the broker as the third voter), required majority 2.
  Metadata authority is separate from the Q2 data quorum and gets its own
  ownership epoch.
- **DLQ-configured route (F7b):** the source route and a DLQ placement
  (`lease_ms=200`, owner 3, epoch 7) are installed through the live metadata
  cluster so the broker's committed-update watcher prints the authority witness.
  No terminal disposition is exercised.

## 4. Scenarios and key observed values

1. `healthy-q0-q1-q2-baseline` — **PASS**. Q0 is admitted with two subscriber
   deliveries. Q1 returns `COMMITTED_LOCAL` incarnation 42 at offsets `[0, 2]`.
   Q2 returns `COMMITTED_QUORUM` at offset 0 with payload digest
   `c9b832517ae5c476650c25c095a65bfdeadb072c8f29a264d7757184539ebe48`; the ACK
   at generation 1 advances the durable group cursor to 1 on all three voters
   (`{1: 1, 2: 1, 3: 1}`). A dedup retry of the same producer token returns
   offset 0 and creates no second record.
2. `consumer-ack-refused-without-q2-majority` — **PASS**. A real delivery token
   (generation 1, offset 0) is fetched, then two of three Q2 voters are
   SIGKILLed. The identical ACK returns `OutcomeUnknown` and leaves the cursor
   unadvanced (`{1: None, 2: None, 3: None} -> {1: None}`); no `ACK_COMMITTED` is
   fabricated. Q1 still commits (`COMMITTED_LOCAL 42 0`) during the outage.
   After voters 2 and 3 restart on their original storage (leader 1), the
   identical ACK identity resolves committed exactly once at offset 0 with the
   durable cursor at 1 on all voters; a second retry keeps one record per voter.
3. `metadata-election-preserves-ownership-epoch` — **PASS**. Killing the
   metadata leader elects a new leader: leader 1→2, term 1→2 (the election
   winner is not fixed; a prior run elected node 3, and the property is the new
   higher-term leader, not a specific node). The committed placement and
   ownership epoch are unchanged at epoch 7→7, and Q1 publishes
   `COMMITTED_LOCAL 42 0` and `COMMITTED_LOCAL 42 2` without an intervening
   metadata mutation under the 60 000 ms cached authority.
4. `expired-authority-fails-closed` — **PASS**. The broker prints
   `Q1 DLQ authority: epoch=7 owner=3 lease_ms=200`; that line is emitted at or
   after the source's fresh install, so it upper-bounds expiry. After 302.3 ms
   (>= 200 ms), a publication at sequence 1 is refused: `StaleEpoch`, exit 1,
   no `COMMITTED_LOCAL`. The manifest stays at 2→2 records and the externally
   read history at 1→1 record, so no durable record was created. Elapsed time
   alone is never the proof; the typed refusal is.

## 5. Independent reconciliation

`oracle.json` records:

```json
{
  "verdict": "PASS",
  "reasons": [],
  "counts": {
    "records": 144, "scenarios": 4, "conclusions": 4, "scenario_passes": 4,
    "q1_committed": 6, "q2_committed": 4, "majority_proofs": 4,
    "ack_committed": 2, "refusals": 2, "uncertain": 1,
    "reused_evidence": 11, "witness_fields": 24, "violations": 0
  }
}
```

The verifier reads the journal only. It requires a distinct-voter witness for
every commit and refuses a cursor or ACK that outruns those witnesses. It also
verifies each `reused_evidence` citation: the named file must exist and every
verdict it records must be PASS. `scripts/test_q1_oracle.py` covers the
rejection rules, including the missing-file and recorded-FAIL fixtures.

## 6. Cited evidence (reuse, not repetition)

C-07 does not re-run properties already proved by C-02…C-06. The campaign emits
one `reused_evidence` event per cited property, naming the C-07 scenario it
supports:

- `benchmarks/evidence/c04-q2-publish/q2-publish.jsonl` — premajority and
  postmajority leader-crash safety (C-04 scenarios 12 and 14) and the
  one-unavailable-follower publication (scenario 13), supporting the F1 Q2
  commit baseline.
- `benchmarks/evidence/c05-q2-consumer/oracle.json` — consumer ACK crash
  windows, sparse cursor and generation fencing, supporting F1.
- `benchmarks/evidence/c03-q2-raft/scenario-14.jsonl` — corrupted committed
  replica refused, supporting F1.
- `benchmarks/evidence/c03-q2-raft/scenario-15.jsonl` — a healthy majority
  preserves committed history, supporting F1.
- `benchmarks/evidence/c06-q1-q2-isolation/oracle.json` — Q1 commits while the
  Q2 domain lacks quorum, supporting F3b; and Q2 history surviving isolated Q1
  storage loss, supporting F7a.
- `benchmarks/evidence/c02-safety-matrix/D2-pending-terminal-at-migration.jsonl`
  — durable PENDING terminal across migration without duplication, supporting
  F3b.
- `benchmarks/evidence/c02-crash-matrix/verdicts.json` — the 16-window Q1
  migration crash matrix (fencing/ownership boundaries), supporting F7a.
- `benchmarks/evidence/c02-bundle-campaign/verdict.json` — the source+DLQ
  bundle with no duplicate DLQ publication and exactly-once disposition,
  supporting F7b.

All eleven citations record PASS; the reconciler resolves each path under
`benchmarks/evidence/` and counts `reused_evidence: 11`.

## 7. CI

`.github/workflows/fast.yml` job `c07-fault-gate` (own `timeout-minutes: 15`,
modeled on `q1-q2-isolation`) checks out the repository, installs the stable
Rust toolchain, builds the normal broker and SDK examples
(`cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked`), builds
the metadata Raft voter (`cargo build -p rovqet-metadata-raft --bin
metadata-raft-node --locked`), and runs `python3 scripts/test_cluster_fault_gate.py`.
The campaign exits non-zero on any non-PASS scenario or reconciliation; there is
no `|| true`. Every pre-existing job is unchanged.

## 8. Limitations

1. **One Q2 route per broker process.** This build exposes a single Q2 route in
   the broker, so two-partition isolation cannot be shown through two live
   routes here. It is proven at the shard/adapter level by
   `crates/consensus/tests/three_voters.rs::two_partitions_in_one_shard_group_stay_independent`;
   this directory does not claim a two-route wire demonstration.
2. **Loopback prototype.** All processes, identities and storage are local and
   disposable, with plaintext transport. No production security, multi-host
   independence or operational profile is certified.
3. **No physical disk-loss certification.** WAL corruption and manifest changes
   in the cited C-02/C-06 evidence are targeted file mutations on disposable
   storage, not physical device failure.
4. **Metadata Raft is not the Q2 data quorum.** F7a/F7b exercise metadata
   authority only; they do not combine metadata elections with Q2 quorum loss.
5. **ACK uncertainty may resolve later.** In F3b the uncertain ACK is not proof
   of non-delivery; exactly-once resolution is established only by the post-
   restore evidence and the unchanged record count.

## 9. Flake reproduction and resolution

Two intermittent failures seen while building this gate were diagnosed to their
root cause and fixed in code. Neither was retired by a retry, a longer timeout
or a weakened assertion.

### 9.1 `AddrInUse` on metadata voter restart

| | before the fix | after the fix (`02f4605`) |
|---|---|---|
| failure mode | deterministic `EADDRINUSE`: bind loopback port 0, read the endpoint, close the listener, let a competitor bind that endpoint, then the node's bind of the same endpoint fails | the node is handed the already-bound listener descriptor (a clone of the socket), so no release/rebind interval exists and the endpoint stays reserved through shutdown and restart |
| repetition campaign | the ownership gap is demonstrated with a controlled interleaving only; no spontaneous failure of the unmodified test is claimed | **120/120** executed test-binary runs (80 isolated + 40 concurrent across `authority`, `three_voters`, `migration_quorum`, `asymmetric_partition`) with **0** test failures and **0** `AddrInUse`; additionally 10/10 simultaneous `cargo test -p rovqet-metadata-raft` processes, 49 tests each, zero `AddrInUse` |

Commits: `fe82242` (hand the bound listeners to the node), `6908a94` (the tests
retain voter sockets across startup and restart), evidence recorded in `02f4605`
(`crates/metadata-raft/tests/LISTENER_OWNERSHIP.md`).

### 9.2 D2 pending-terminal-at-migration intermittency

| | baseline | fixed |
|---|---|---|
| build | `02f4605` | `001bd1d` + `3cbc312` |
| D2 executions | 10 run: **6 failures**, 4 PASS (every failure the same mode: the source never printed the dependent 1 ms `Q1 DLQ authority` line) | **20/20 PASS** (exit 0, scenario PASS, oracle PASS; all 20 printed six source DLQ authority lines) |

Commits: `001bd1d` (an identical `{owner, epoch, lease}` re-install keeps its
original installation clock and can no longer renew an expired lease) and
`3cbc312` (the server installs authority from the committed Raft snapshot rather
than the asynchronously published routing-store copy). The full diagnosis,
deterministic lease probe and both measurement tables are in
`benchmarks/evidence/c02-safety-matrix/D2-intermittency-diagnostic.md` (committed
by `dd3facb`); the C-02 ledger note records the same resolution.
