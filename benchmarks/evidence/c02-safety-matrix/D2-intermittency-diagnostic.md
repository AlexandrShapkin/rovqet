# C-02 D2 intermittency diagnosis

Baseline: `02f4605`. Existing committed D1/D2/D3 campaign evidence is retained
unchanged; this diagnostic does not invalidate those successful executions.

## Deterministic lease probe

A standalone Rust probe imports `crates/durable/src/authority.rs`, installs
`Authority::new(1, 7, Duration::from_millis(1))` at `start`, checks at
`start + 3s`, reinstalls the identical authority at that instant, and checks again.
Before the fix, observed output:

```text
before identical install: Err(ExpiredLease)
after identical install: Ok(())
```

This proves that reinstalling an unchanged placement extended its authorization.
The fix preserves the original installation time for identical authorities.
Changed owner, epoch, or lease values still install a fresh clock, subject to
the actor's existing epoch-regression rejection.

## Independent publication ordering

The Raft routing-store publisher and the Q1 authority watcher independently
subscribe to committed metadata updates. Previously the authority watcher read
the routing store, which could still contain the preceding revision. With no
later metadata update, the watcher never installed the committed placement.
Authority lookup now reads the Raft node's committed snapshot directly;
static-fixture mode continues to use the routing store.

## Campaign measurements

### Unchanged-build baseline (10 D2 runs at `02f4605`)

```text
run     01 02 03 04 05 06 07 08 09 10
result  P  F  F  F  F  P  P  P  F  F
totals: 4 PASS, 6 BLOCKED
```

All 6 failures are the same mode: the source never printed the 1 ms DLQ
authority line. The other observed mode (durable `PENDING` terminal missing
after a successful `DlqUnavailable` NACK, `pending_terminal == 0`) did not
occur in these 10 runs; it was seen only in earlier sessions. The mode-2
failure, reproduced 6/10 times, is what the fix targets.

Representative mode-2 failure (run 02; runs 03-05, 09, 10 are identical in
shape). The source's last authority observations before the block:

```text
[source] Q1 DLQ authority: epoch=0 owner=0 lease_ms=0
[source] Q1 DLQ authority: epoch=7 owner=1 lease_ms=600000
[voter3] {"event":"proposed","node":3,"ok":true,"response":{"Applied":{"revision":6}}}
```

The voter subsequently reported revision 6 with the dependent placement's
`lease_valid_for_ms` equal to 1, but the source never printed the 1 ms authority.
This is distinct from lease renewal: the watcher missed the placement itself.

Executed successfully:

```text
cargo test -p rovqet-durable --locked authority::tests
cargo test -p rovqet-server --locked only_committed_metadata_becomes_the_ownership_authority
cargo test -p rovqet-durable --locked
cargo test -p rovqet-server --locked
cargo test --workspace --locked
cargo fmt --all -- --check
cargo check --workspace --locked
python3 scripts/check_docs.py
```

The isolated fixed broker passed **20 consecutive D2 executions**:

```text
run     01 02 03 04 05 06 07 08 09 10
result  P  P  P  P  P  P  P  P  P  P
run     11 12 13 14 15 16 17 18 19 20
result  P  P  P  P  P  P  P  P  P  P
```

Here `P` means exit status 0, scenario PASS, and oracle PASS, not BLOCKED.
All 20 executions printed six source DLQ authority lines, exercising repeated
installation rather than avoiding it.

Also passed on the fixed broker: `test_q1_migration` (7 tests),
`test_q1_migration_bundle` (6 tests), `test_q1_migration_partition`, the full
`test_q1_migration_crash` 16-window matrix, and D1/D3 of
`test_q1_dlq_uncertainty.py`. The first external wrapper invocation for the
unittest-based migration/bundle scripts exited 5 with `NO TESTS RAN` (incorrect
`__main__` registration); that invocation is not counted as a pass, and the
corrected wrapper reruns passed. No repository campaign script is modified.

Diagnostic execution uses the unchanged `scenario_d2` function via an external
wrapper that disables scratch-directory removal and redirects each execution's
oracle evidence to a separate directory under `/tmp/opencode/d2-diagnosis/`.
The fixed broker is built in `/tmp/opencode/d2-fixed-target/` so baseline and
post-fix measurements cannot silently switch binaries.

Exact isolated broker build:

```sh
cargo build -p rovqet-server --features fault-injection --locked \
  --target-dir /tmp/opencode/d2-fixed-target
```

The D2 invocation is equivalent to:

```sh
ROVQET_SERVER=/tmp/opencode/d2-fixed-target/debug/rovqet \
  ROVQET_KEEP_TMP=1 python3 scripts/test_q1_dlq_uncertainty.py D2
```

The external wrapper only redirects evidence and preserves scratch state; it
does not alter scenario logic, waits, assertions, client requests, or verdicts.
The migration scripts without a broker override are executed by an external
wrapper replacing only their broker path constant (including the imported
common harness constant) with the isolated binary. No repository campaign
script is modified.
