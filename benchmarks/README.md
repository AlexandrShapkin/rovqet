# Q0 exploratory measurements

Build the broker and independent Rust client/oracle in release mode:

```sh
cargo build --release --locked -p rovqet-server -p rovqet-testkit
python3 -m unittest discover -s benchmarks -p 'test_*.py'
python3 benchmarks/run_q0.py --output benchmarks/results/q0-unique-run
```

The output directory must be new. Every repetition starts a separate broker and
separate generator process. The default matrix uses three independent repetitions
with seeds 42, 43 and 44. Select a bounded subset with, for example:

```sh
python3 benchmarks/run_q0.py --profiles A-basic,B-fanout,C-many-to-many \
  --repetitions 3 --seed 42 --output benchmarks/results/q0-throughput
```

`profiles.json` contains the exact CLI workload configurations. `rate` is total
open-loop offered messages/second; zero means a burst. `messages` is the measured
message count; warmup messages are additional and remain in the event evidence.
All participants use real authenticated TCP sessions. `D-idle-*` establishes and
holds real connections; no virtual clients are substituted for failed sessions.
The tool is isolated plaintext loopback research software, not a production or
H1/PERF/VAL-01 qualification.

## Evidence files

- `manifest.json`: source commit, dirty-tree flag, environment, seeds and per-run
  completion/invalid/NOT_RUN status.
- `<profile>-rN/run.json`: exact commands, configuration, generator summary,
  resource summary and every broker metric snapshot.
- `resources.jsonl`: 100 ms samples by default, broker and generator RSS, CPU
  seconds, FD count, loopback counters and `/proc/net/sockstat` evidence.
- `broker.stderr`: periodic and final `BROKER_METRICS` records, including actual
  broker admission/delivery/drop/expiry and outstanding queue accounting.
- `generator/`: independent JSONL event evidence, aggregate oracle result and raw
  latency distributions produced by `rovqet-testkit`.

The generator uses a single monotonic clock origin for offers, send/ACK and
subscriber reception. The runner never subtracts broker timestamps and never
averages percentiles across repetitions. Preserve each histogram and run result
separately. CPU is reported as one logical core = 100%; the mean covers sampled
process lifetime, including setup and drain, while peak interval CPU helps expose
load-generator saturation. It is not a steady-state CPU claim. Cgroup quotas are
recorded where accessible; host CPU count alone may overstate the available CPU.

Loopback RX/TX and socket memory are not broker-exclusive. Loopback counters cover
the network namespace, and `/proc/net/sockstat` memory may include host activity.
These values must not be silently attributed to Rovqet alone. Missing metrics are
`UNAVAILABLE`, not zero. The resource summary does not replace raw samples.
Connection-density incremental RSS should compare actual authenticated connection
counts and held-session RSS across profiles, not treat the configured limit as a
measurement. The connection envelope is a conservative configured safety bound,
not actual RSS or allocated memory.

Above 1,024 broker connections, the runner adds an explicit connection-memory
budget only if its envelope (1,380,352 bytes per configured session) fits within
75% of current `MemAvailable` and accessible remaining cgroup memory. Otherwise
the run is `NOT_RUN`, with the exact limiting factor in `run.json`. Other process,
FD, port or authentication failures remain visible in generator evidence. The
10,000-connection profile is attempted only when the safety calculation permits.

Raw output is ignored by Git. Commit selected compact summaries/histograms and a
reproducible report, preserving the full local event/resource evidence separately
and retaining content hashes if raw files are not committed. An invalid process
exit, missing/malformed summary, logger failure or resource evidence error must
not be reported as a successful broker measurement.

Direct tool example and independent append-only evidence replay:

```sh
cargo run -p rovqet-testkit -- q0-load --endpoint 127.0.0.1:7443 \
  --publishers 1 --subscribers 1 --messages 1000 --rate 1000 \
  --payload-bytes 256 --seed 42 --output /tmp/rovqet-example-new
cargo run -p rovqet-testkit -- replay --run /tmp/rovqet-example-new
```

Each publisher has a bounded pending queue (default 128). Open-loop offers keep
following their schedule without waiting for broker ACKs; a full generator queue
is counted as `client_dropped`, separately from a broker rejection. Scheduler
lateness, generator CPU and offered-to-delivery latency expose generator limits.
`--duration-secs` derives the message count from `--rate` when `--messages` is
omitted; `--warmup-secs` or `--warmup-messages` excludes warmup from measured
histograms while preserving all warmup events. Queue, oracle and logger state
have explicit caps. Evidence saturation invalidates the run, never silently
omits events. Connection setup and publisher settlement have deadlines; uncertain
or incomplete outcomes remain separate from missing admitted copies.

The oracle correlates run-scoped unique IDs in deterministic payloads, along
with publisher IDs and SHA-256 digests. SDK wire message IDs remain session-scoped;
no broker dedup state is used. `--check-order true` diagnoses inversions only
within each publisher's fixed partition key at each subscriber; it makes no
cross-partition/global ordering claim. JSONL replay must match every oracle
aggregate. Exact nanosecond-value frequency maps retain the complete distribution,
with p50/p95/p99/p99.9 calculated by nearest rank, not averaged percentiles.

Rates expose both observation-window goodput (including final settlement/drain)
and active-window goodput (first measured offer through last measured offer,
ACK or delivery). The latter excludes empty drain time but retains rejected,
client-dropped and unknown counts. Latencies include ACK and same-generator-clock
send-to-receipt and offer-to-receipt distributions, so publisher queue delay is
visible. Missing admitted copies are Q0 observations and are never reclassified
as expiry without broker telemetry; unknown outcomes are never called definite
loss. Broker drop/expiry telemetry includes warmup, while oracle measured counts
exclude it. These totals must not be compared without that distinction.

## Pinned NATS Core baseline (R0-03)

Use the same release testkit and profiles for both backends. The official Linux
amd64 NATS Server 2.11.10 release archive is SHA-256 pinned in `install_nats.py`:

```sh
python3 benchmarks/install_nats.py --output /tmp/rovqet-nats-server-2.11.10
python3 benchmarks/run_q0.py --backend nats \
  --nats-server /tmp/rovqet-nats-server-2.11.10 \
  --profiles A-basic,B-fanout,C-many-to-many,D-idle-128,D-idle-512,D-idle-1024,D-idle-4096,E-slow,F-overload,F-burst \
  --repetitions 3 --seed 42 --output benchmarks/results/nats-new
```

`nats/core.conf` is an isolated plaintext loopback development configuration with
static test credentials, 256KiB maximum payload and 256KiB per-connection pending
output ceiling (NATS requires max_payload <= max_pending; Rovqet allows1MiB frames,
while both compared workloads use at most1KiB). The runner copies it into every run directory, substitutes only
the profile's maximum connection count, verifies the server version, starts a
fresh server, discovers its actual ephemeral TCP listener and stops it on exit.
Resource sampling, deterministic seed, offered schedule, payload identities,
JSONL event oracle and histograms reuse the Rovqet path. The generator uses the
official `async-nats` Rust client, isolated in the testkit crate. No queue groups
are used: exact subject `demo` subscriptions all receive physical copies. Rovqet
routes the same demo topic across four partitions; NATS Core has no partition
ownership or matching priority/resource hierarchy.

NATS Core publishes are **unconfirmed client enqueue operations**, never Rovqet
`ADMITTED`. End-of-stream flush only bounds publication progress; it is not a
per-publication admission or delivery acknowledgement. Compare actual received
IDs and same-process monotonic send-to-receipt distributions. Never compare Core
publish-enqueue latency with Rovqet PUB_ACK latency. Missing unconfirmed copies
remain separate from missing admitted copies. Server rejection/expiry reasons
that Core cannot expose per publication are UNAVAILABLE, rather than invented.

The slow consumer configurations are deliberately semantically different:
Rovqet enforces delivery credits, DROP_NEWEST and broker queue TTL; NATS Core has
no equivalent credit replenishment or per-message TTL in this profile and may
disconnect slow consumers at its pending-output boundary. SDK/client channel
capacities do not imply identical effective kernel/runtime buffering.
`E-credit-starved` is explicitly NOT_RUN for NATS. A slow/overload observation is
useful but does not establish an equivalent drop policy or durability advantage.
The high-density resource-envelope safety check is retained for both baselines;
10,000 sessions remain NOT_RUN when the host cannot safely cover the envelope.

Sustained-rate runs are INVALID for capacity claims if actual offered rate is
below90% of requested, maximum scheduling lateness exceeds the larger of5ms and
10% of the offer window, or the bounded generator queue sheds offers. Reasons
remain in `run.json`; original traces and distributions are retained. A burst has
no sustained target and its client-side shedding is counted explicitly.

Future-only `nats/jetstream-single.conf` and `nats/jetstream-cluster.conf` prepare
isolated bounded storage templates. They do not create streams or consumers and
are not executed Q1/Q2 comparisons. Separate store directories, replica counts,
fsync/commit and consumer-ACK semantics need validation when corresponding Rovqet
functionality exists. No durable equivalence is claimed here.

Official references: [server release](https://github.com/nats-io/nats-server/releases/tag/v2.11.10),
[configuration](https://docs.nats.io/reference/config/),
[Core publish](https://docs.nats.io/using-nats/developer/sending), and
[slow consumers](https://docs.nats.io/running-a-nats-service/nats_admin/slow_consumers).
