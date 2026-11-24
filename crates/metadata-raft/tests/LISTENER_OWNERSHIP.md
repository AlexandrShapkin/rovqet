# Metadata listener ownership regression

The four loopback integration binaries (`authority`, `three_voters`,
`migration_quorum`, `asymmetric_partition`) reserve sockets, not port numbers.
Every three-voter test holds all three reservations before starting a node.
The standard-library listeners are nonblocking; each node receives a Tokio
listener made from a cloned descriptor for the same socket. The originals stay
alive through shutdown and restart, so there is no release/rebind interval.

`authority::an_injected_listener_is_not_rebound_and_owns_the_advertised_endpoint`
also supplies an independently occupied configuration address. Startup must use
the injected listener, and the endpoint must remain reserved after node shutdown
while the test still owns its original descriptor.

## Verification

- `cargo test -p rovqet-metadata-raft --locked`: 49 passed, zero failed/ignored.
- `cargo fmt --all -- --check`: passed.
- `cargo check --workspace --locked`: passed (existing unused-dependency warnings).
- `cargo test --workspace --locked`: 414 passed, zero failed/ignored.
- Ten simultaneous `cargo test -p rovqet-metadata-raft --locked` processes:
  10/10 passed, zero `AddrInUse` occurrences (49 tests per process).

The original race was reproduced with a controlled interleaving of the original
algorithm: bind loopback port zero, read the endpoint, close the listener, bind
that endpoint in a competitor, then attempt the node's bind. The last bind
returns `EADDRINUSE`. This is a deterministic demonstration of the ownership gap,
not a claim of reproducing a spontaneous unmodified integration-test failure.

Source commits: `fe82242` (API and architecture) and `6908a94` (integration tests).
The completed repetition campaign produced:

| Binary | Isolated executions | Concurrent executions | Failures | `AddrInUse` |
|---|---:|---:|---:|---:|
| `authority` | 20 | 10 | 0 | 0 |
| `three_voters` | 20 | 10 | 0 | 0 |
| `migration_quorum` | 20 | 10 | 0 | 0 |
| `asymmetric_partition` | 20 | 10 | 0 | 0 |
| **Total** | **80** | **40** | **0** | **0** |

Each execution invokes the compiled test binary with `--test-threads=1`.
Concurrent runs execute all four affected binaries simultaneously, retaining
the full election, minority, recovery and compaction assertions.
The ten simultaneous whole-package Cargo processes above are additional runs.

An initial campaign harness hit its 120-second command deadline after 11 complete
isolated rounds plus `authority` and `three_voters` in round 12 (46 successful
binary executions, no test failures or `AddrInUse`). That incomplete campaign is
excluded from the table. The campaign was restarted with a 600-second command
deadline and completed all 120 executions successfully.

Build the executables with
`cargo test -p rovqet-metadata-raft --locked --no-run --message-format=json`;
use the `compiler-artifact` records' `executable` paths for the four named test
targets. Run each binary serially for each of 20 rounds, then run the four binaries
concurrently for each of ten rounds. Record each exit status and reject any
`AddrInUse` / `Address already in use` output. Local raw logs and per-execution JSON
are in `/tmp/opencode/listener-repetitions-complete/`; the extra Cargo logs are in
`/tmp/opencode/listener-cargo-load/`. These temporary paths are local verification
artifacts, not a claim of permanently published campaign data.
