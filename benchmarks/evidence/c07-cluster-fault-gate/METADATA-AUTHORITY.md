# C-07 metadata authority increment (F7a / F7b)

Scope: loopback-only, plaintext research profile on one host, independent
disposable storage. Metadata Raft is distinct from the Q2 data quorum.
This does not establish production readiness or physical failure independence.

## Work-package ledger

- F7a implemented: two observable metadata voter processes and a broker as the
  third voter; committed Q1 placement at epoch 7 with a 60-second lease.
  Publish, kill the metadata leader, await an observed higher-term leader,
  compare the survivor's committed placement, then publish again without any
  intervening metadata mutation. The journal retains leader/term events,
  committed placements, SDK results, incarnation and offsets.
- F7b implemented and **PASS** on a DLQ-configured route. The DLQ exists only so
  the committed-update watcher prints the authority witness; no terminal
  disposition is exercised. The source placement is moved to a 200 ms lease
  first (fresh install clock), then the DLQ placement makes the watcher print
  `Q1 DLQ authority: epoch=7 owner=3 lease_ms=200`. That line can only be
  printed at or after the source's fresh install, so it upper-bounds expiry.
  After the observed window elapses (302.2 ms >= 200 ms), sequence 1 is
  refused: `Error: Remote { code: Some(StaleEpoch), raw_code: 17, retryable:
  false, retry_after: None }`, exit 1, no `COMMITTED_LOCAL`. The manifest count
  is 2 before and 2 after and the externally read history is 1 record before
  and 1 after, so no durable record was created. Elapsed time alone is never
  the proof: owner 3 and epoch 7 are unchanged and match the request, so the
  only remaining fencing outcome is `ExpiredLease` (mapped to `StaleEpoch`).
  Sequence 1 (not 0) means a fabricated success would have appended a second
  record and moved both counts. No lease-expiry migration fallback is used.
- Already established elsewhere, not repeated: stale ownership epoch rejection
  (C-04 scenario 6), stale consumer generation rejection (C-05 `fencing`), and
  epoch 7-to-8 handover (C-02 D2 journal).

Next unblocked increment: the separate C-07 oracle reconciler.

## Verification

```sh
cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked
cargo build -p rovqet-metadata-raft --bin metadata-raft-node --locked
python3 scripts/test_cluster_fault_gate.py
python3 scripts/test_q1_q2_isolation.py
python3 scripts/test_q1_dlq_uncertainty.py D2
python3 scripts/test_metadata_raft.py
python3 scripts/test_cluster_routing.py
python3 scripts/check_docs.py
git diff --check
```

Observed: builds succeed (existing Rust warnings); the fault gate reports all
four scenarios **PASS** and exits **0**. Isolation, D2, metadata Raft, routing
and documentation checks exit 0. Exact per-run observations are in
`fault-gate.jsonl`; metadata scenario observations and conclusions carry
explicit `metadata_quorum` scope.
