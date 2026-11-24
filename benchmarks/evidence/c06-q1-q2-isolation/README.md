# C-06 — executed evidence index (partial; C-06 stays IN_PROGRESS)

C-06 remains **IN_PROGRESS** in `docs/roadmap/tasks.json`. This directory indexes
four executed artifacts of the C-06 increment. None of them is a full acceptance
of the work package, and the **Not proven** section below bounds every claim made
here.

## 1. Wire-level wrong class over authenticated TCP — `153440f`

Artifacts: `wire.jsonl` (fsynced observation journal), `wire-result.txt`
(runner output).

```sh
cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked
python3 scripts/test_q1_q2_wire.py
```

The wire test runs a Q1 broker and three Q2 voters over real loopback TCP. A QOS
field is explicit in PUBLISH (major-v1 field 12); wrong-class requests are
rejected with `ERROR 14` (`UNSUPPORTED_GUARANTEE`) on both routes: Q2-on-Q1 and
Q1-on-Q2. Q1 replay, manifest and record count are unchanged after the rejection;
Q2 messages, producer outcomes and next offset are unchanged on all three voters.
A subsequent correct-class publication of the same producer sequence succeeds at
the next offset. `wire-result.txt` records q1_qos2 PASS, q2_qos1 PASS and
`full_c06: NOT_RUN`.

CI: `.github/workflows/fast.yml` job `q1-q2-wire` (`dc88e19`) builds the normal
broker and SDK examples and runs this regression.

## 2. Focused isolation journal — `0008cfb`, `391e8b5`

Artifact: `isolation.jsonl` (51 records, three scenario conclusions).

```sh
timeout 600 python3 scripts/test_q1_q2_isolation.py
```

- `q1-restart-preserves-incarnation-and-offsets` **PASS**: three publications at
  offsets `[0, 2, 4]` under incarnation 42, SIGKILL, restart on the same storage
  replays `READ_RECORD 42` at `0/2/4`; the manifest is unchanged at 6; a dedup
  retry of the original producer token returns offset 0 with no new record.
- `q1-unrecoverable-history-fails-closed-and-fresh-incarnation-is-distinct`
  **PASS**: with the history files removed, a restart without initialization exits
  1 with `REPLAY_NOT_AVAILABLE: existing Q1 history missing; no automatic
  initialization`, prints no `listening on` line and does not recreate MANIFEST. A
  restart with a fresh incarnation 43 starts empty, the old offsets `[0, 2]` are
  not replayable, and the old producer token is accepted as a fresh
  `COMMITTED_LOCAL 43 0`.
- `q2-quorum-loss-blocks-quorum-ack-while-q1-commits` **PASS**: baseline Q2
  offset 0 and Q1 offset 0; voters 2 and 3 are killed with exit -9; a fresh Q2
  publish produces only a 15 s client timeout, journaled `uncertain` and never a
  fabricated ACK; the same broker returns `COMMITTED_LOCAL 42 2` for Q1;
  restarting the voters on their original storage restores a Q2 quorum commit at
  offset 2. The uncertain request may commit after restoration; the timeout is not
  proof of non-delivery.

The Q2 scenario disturbs the Q2 **data** quorum. Metadata is supplied by static
JSON fixtures, not a live metadata Raft quorum, so no metadata-quorum isolation
claim is made. All processes and storage are disposable loopback prototypes; no
physical host-failure or production-security certification is implied.

## 3. Independent reconciliation — `e422185`

Artifacts: `scripts/q1_oracle.py` (`reconcile_q1_q2_isolation`), `oracle.json`.

```sh
python3 -c "import json,sys; sys.path.insert(0,'scripts'); import q1_oracle as o; recs=[json.loads(l) for l in open('benchmarks/evidence/c06-q1-q2-isolation/isolation.jsonl') if l.strip()]; print(json.dumps(o.reconcile_q1_q2_isolation(recs), indent=2, sort_keys=True))"
```

The verifier reads the journal only. `oracle.json` records verdict **PASS**,
reasons `[]`, and counts
`{"records": 51, "scenarios": 3, "q1_acknowledged": 9, "q2_quorum_acknowledged": 3, "q2_blocked_timeouts": 1, "majority_loss_windows": 1, "violations": 0}`.
The reconciler enforces ten violation classes and is covered by eleven fixtures
added in `e422185`:

```sh
python3 scripts/test_q1_oracle.py   # Ran 77 tests ... OK
```

## 4. CI isolation job

`.github/workflows/fast.yml` job `q1-q2-isolation` (own `timeout-minutes: 10`)
builds the **normal** broker and SDK examples
(`cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked`, no
`--features fault-injection`) and runs `python3 scripts/test_q1_q2_isolation.py`.
Every pre-existing job is unchanged. The campaign was not run locally as part of
the documentation increment that added this job; CI executes it.

## Not proven

These are explicit gaps, not failures and not PASS:

1. **`q2-history-survives-isolated-q1-storage-loss` was not executed.** No
   recorded scenario wipes Q1 storage while a Q2 history is preserved. The
   `q1-unrecoverable-history-fails-closed-and-fresh-incarnation-is-distinct`
   scenario proves Q1 fail-closed behavior only.
2. **Per-partition isolation (§7) was not exercised.** No recorded scenario
   addresses multiple logical partitions in one shard. This evidence does not
   establish whether the current build supports that configuration; the question
   is **unverified** here and no claim is made either way.
3. **Three-voter stability repetitions belong to `7d7d801`.** The 20/20 isolated
   repetitions, 12/12 under 10 CPU hogs on 8 cores and 18/18 with 6 concurrent
   test processes (50/50 total) are recorded in the C-06 ledger for that commit
   and are not repeated or re-measured by this increment.

No general Q1-to-Q2 migration, class-conflict validation, independent consumer
state, cross-partition ACK rejection or production security property is proven
by this directory.
