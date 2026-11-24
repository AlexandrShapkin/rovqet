# Partial C-06 validation

C-06 remains **IN_PROGRESS**. This file indexes the executed increment only; it
is not an acceptance record.

## Implementation commits

| Commit | Content |
| --- | --- |
| `153440f` | `scripts/test_q1_q2_wire.py`: wrong durable class rejected over real TCP on both routes |
| `dc88e19` | separate `q1-q2-wire` CI job |
| `0008cfb` | `scripts/test_q1_q2_isolation.py`: Q1 restart/dedup and fail-closed history loss |
| `391e8b5` | isolation scenario `q2-quorum-loss-blocks-quorum-ack-while-q1-commits` |
| `e422185` | `reconcile_q1_q2_isolation` and `oracle.json` |
| `792cf10` | separate `q1-q2-isolation` CI job |

## CI

Run [45427293901](https://github.com/AlexandrShapkin/rovqet/actions/runs/45427293901)
for `792cf106101c63de3463a8147dec183981214f5b` completed **success**
(`gh run watch 45427293901 --exit-status` exited 0). Jobs: `docs`, `rust`,
`q2-publish`, `q1-q2-wire`, `q1-q2-isolation` — all success. The
`q1-q2-isolation` job executed the campaign step
`python3 scripts/test_q1_q2_isolation.py` after
`cargo build -p rovqet-server -p rovqet-sdk --examples --bins --locked` (normal
build; no `--features fault-injection`). Every pre-existing job is unchanged.

The earlier run
[45393522128](https://github.com/AlexandrShapkin/rovqet/actions/runs/45393522128)
for `dc88e194e66e386f7c1149f042ad7d0183bb6e9a` was also green with `docs`,
`rust`, `q2-publish` and `q1-q2-wire`.

## Local validation of this increment

The isolation campaign was **not** run locally while adding the CI job and this
index; CI executed it. What was executed locally, all exit 0:

- `python3 scripts/check_docs.py` — `documentation checks PASS: 45 markdown files, 36 roadmap tasks`
- `python3 -c "import json; json.load(open('docs/roadmap/tasks.json'))"`
- `git diff --check`
- the `reconcile_q1_q2_isolation` reproduction command in
  [`README.md`](README.md) returned exactly the committed `oracle.json`
  (`verdict PASS`, `violations 0`)
- `python3 scripts/test_q1_oracle.py` was previously recorded as `Ran 77 tests … OK`
  at `e422185`

`python3 scripts/test_q1_dlq_uncertainty.py` was not run for this increment and
is not claimed green; its status is recorded in the C-02 ledger entry.

## Corrected statements

An earlier revision of this file said `scripts/test_q1_q2_isolation.py`,
`reconcile_q1_q2_isolation` and the negative fixtures did not exist and that the
isolation verdict was NOT_RUN. That is no longer true: the script, the reconciler
and eleven fixtures exist, and `oracle.json` reconciles the journal as PASS.

## Not proven

1. `q2-history-survives-isolated-q1-storage-loss` was not executed.
2. Per-partition isolation (§7 of the architecture) was not exercised; whether the
   current build supports multiple logical partitions in one shard is unverified
   here.
3. The three-voter stability repetitions (20/20, 12/12, 18/18) belong to
   `7d7d801` and are recorded elsewhere.

Consequently the full C-06 proof — wrong class denied, Q1 incarnation/Q2 prefix
separation, storage-loss survival and partition isolation — is not complete. No
acceptance claim is made.
