# Defect found by this campaign and fixed

Symptom: after durable consumer metadata was interleaved into the WAL, standalone
`read_q1` returned `ResourceExhausted` instead of committed history. The trigger was a
continuation at an odd WAL position (for example 9 after an ACK and generation checkpoint).

Cause: major-1 codec validation in `crates/protocol/src/replay.rs` still required even
`READ_RECORD`/`READ_END` offsets. B-05 had made application offsets sparse, so the server
could encode a valid odd continuation that the same version refused to validate, and the
transport mapped that encode failure to `RESOURCE_EXHAUSTED`.

Deterministic reproduction before the fix:

```sh
# publish three messages, acknowledge one, claim the group again, then read
python3 - <<'PY'
# (see scripts/test_q1_durability.py::test_consumer_crash_windows)
PY
target/debug/examples/read_q1 durable 1 3 0 42 0 4 65536 0
# observed: Error: ResourceExhausted
```

Fix: accept every in-range application position in codec validation. Golden vectors and the
wire layout are unchanged. Regression coverage:
`crates/protocol/tests/read_replay.rs::sparse_application_offsets_allow_odd_record_and_terminal_positions`
and the consumer crash matrix in this directory. Exploratory pre-fix journals were aborted
harness runs and are not retained as proof; this record and the regression test are the
reproducible evidence.
