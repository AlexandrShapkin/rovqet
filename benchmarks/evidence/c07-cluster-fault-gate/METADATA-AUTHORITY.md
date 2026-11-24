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
- F7b implemented with an honest **INVALID** result: a 200-ms placement is
  committed, but the non-DLQ Q1 route exposes no updated authority-install or
  expired-state diagnostic. The ordinary `Q1 authority` diagnostic is
  startup-only; the watcher prints updated authority only for a configured DLQ.
  A committed short lease alone does not establish when the actor installed
  it or that it expired. No publication-refusal/no-record proof is claimed.
  No lease-expiry migration fallback is used.
- Next unblocked increment: the separate C-07 oracle reconciler.
  To complete F7b's live proof, expose non-DLQ installed/expired authority
  state, then assert the typed refusal and unchanged durable record count.

Already established elsewhere, not repeated: stale ownership epoch rejection
(C-04 scenario 6), stale consumer generation rejection (C-05 `fencing`), and
epoch 7-to-8 handover (C-02 D2 journal).

## Verification

```sh
cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked
cargo build -p rovqet-metadata-raft --bin metadata-raft-node --locked
python3 scripts/test_cluster_fault_gate.py
python3 scripts/test_q1_q2_isolation.py
python3 scripts/test_metadata_raft.py
python3 scripts/test_cluster_routing.py
python3 scripts/check_docs.py
git diff --check
```

Observed: builds succeed (existing Rust warnings); fault gate reports three
PASS scenarios and F7b INVALID, exiting **1**, deliberately not hiding the
coverage gap. Isolation, metadata Raft, routing and documentation checks exit
0. Exact per-run observations are in `fault-gate.jsonl`; metadata scenario
observations and conclusions carry explicit `metadata_quorum` scope.
