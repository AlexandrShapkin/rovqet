# Partial C-06 validation

Implementation commits: `153440f` (wire regression), `dc88e19` (separate CI job).

GitHub Actions run [45393522128](https://github.com/AlexandrShapkin/rovqet/actions/runs/45393522128)
for `dc88e194e66e386f7c1149f042ad7d0183bb6e9a` completed successfully.
`gh run watch 45393522128 --exit-status` succeeded; all four jobs executed:
`docs`, `rust`, `q2-publish`, `q1-q2-wire`.

## Local validation

Exit 0:

- `cargo fmt --all -- --check`
- `cargo check --workspace --locked`
- `cargo test --workspace --locked`
- `cargo test -p rovqet-consensus --locked`
- `cargo test -p rovqet-durable --locked`
- `cargo test -p rovqet-server --locked`
- `cargo test -p rovqet-sdk --locked`
- `cargo test -p rovqet-testkit --locked`
- `python3 scripts/check_docs.py`
- `cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked`
- `python3 scripts/test_q1_q2_wire.py` (normal build)
- `python3 scripts/q0_smoke.py`
- `python3 scripts/q1_smoke.py --check-hooks-disabled` (normal build)
- `cargo build -p rovqet-server -p rovqet-sdk --examples --bins --features fault-injection --locked`
- `python3 scripts/test_q2_raft.py`
- `python3 scripts/test_q2_publish.py`
- `python3 scripts/test_q2_consumer.py`
- `python3 scripts/test_q1_oracle.py`
- `python3 scripts/test_q1_migration.py`
- `python3 scripts/test_q1_migration_bundle.py`
- `python3 scripts/test_q1_migration_crash.py`
- `python3 scripts/test_q1_migration_partition.py`
- `python3 scripts/test_q1_replay.py`
- `python3 scripts/test_q1_consumer.py`
- `python3 scripts/test_q1_terminal.py`
- `python3 scripts/test_q1_durability.py`
- `python3 scripts/test_q1_faults.py`
- `python3 scripts/test_q1_q2_wire.py` (fault-injection build)
- `git diff --check`

Exit 1: `python3 scripts/test_q1_dlq_uncertainty.py`.
D1 and D3 passed; D2 failed before proving the dependent authority expiry:

```text
BLOCKED: the source never printed 'Q1 DLQ authority: epoch=7 owner=1 lease_ms=1'
after the dependent placement commit, so the expired dependent authority cannot
be proven
Q1 coordinator: the proposal was not accepted:
Raft("APIError(ForwardToLeader(ForwardToLeader { leader_id: None, leader_node: None }))")
FAILED SCENARIOS: ['D2-pending-terminal-at-migration']
FAIL
```

This failure has not been diagnosed or repaired by the wire-only increment.
It is not counted as PASS, and local validation as a whole is not green.
Local detailed logs are in `/tmp/opencode/c06-153440f/`.
Previously committed C-02 through C-05 evidence is preserved rather than
replaced with incidental outputs from this validation run.

## Remaining acceptance

The requested `scripts/test_q1_q2_isolation.py`,
`reconcile_q1_q2_isolation` and its negative fixtures do not exist in this
increment. The full isolation verdict is NOT_RUN, and C-06 remains IN_PROGRESS.
