# Q0 exploratory evidence — f9221b3

R0-02 is complete. A-07 remains **IN_PROGRESS**: the safe subset has been measured, but the 10,000-real-session case is NOT_RUN and generator-limited overload runs do not establish sustainable broker capacity.

The final matrix has 36 COMPLETE runs and three NOT_RUN entries (10k density). Each executable case has three independent repetitions, seeds 42/43/44. All final runs record a clean tree at commit `f9221b3`; broker and generator are separate release-mode processes. All 36 complete event traces were replayed and matched every oracle aggregate. No duplicate, corrupt, out-of-order or unknown-outcome observations occurred. Zero final broker ingress/outbound messages, active work sessions and subscriptions were observed after shutdown.

## Reproduce

```sh
git checkout f9221b3
cargo build --release --locked -p rovqet-server -p rovqet-testkit
python3 benchmarks/run_q0.py --output benchmarks/results/new-run --repetitions 3
```

Exact commands, configurations, seeds, environment, statuses and every per-run percentile are in [results.json](results.json). [source-index.json](source-index.json) hashes every full raw source file. All 180 exact nanosecond frequency distributions are retained in `latency-distributions.jsonl.gz`; sampled resources are in `resources.jsonl.gz`. Full events remain locally under ignored `benchmarks/results/q0-f9221b3`; one complete representative trace/summary is committed in compressed form. Hashes alone do not replace the omitted full traces.

## Host and methodology

- CPU: AMD Ryzen 5 PRO 2500U w/ Radeon Vega Mobile Gfx, 8 logical CPUs; kernel `7.2.14-zen1-1-zen`.
- OS: `Linux-7.2.14-zen1-1-zen-x86_64-with-glibc2.44`. `/proc/meminfo` reports 15,205,252 KiB physical RAM. Accessible cgroup CPU/memory limits: UNAVAILABLE.
- FD soft/hard limits: [524288, 524288]; ephemeral TCP ports `32768-60999`; somaxconn `4096`.
- Loopback plaintext development profile, fixed four-partition demo topic, bounded queues and DROP_NEWEST, default credit window64/256KiB. Stock data-rate quotas remain enforced. Broker metrics sampled250ms; process/kernel counters100ms.
- Latencies use the same independent-generator monotonic origin; no broker/client clock subtraction. E2E is send-to-SDK receipt, not socket write. Exact distributions retain p50/p95/p99/p99.9; no percentile averaging.
- CPU100% means one core. Reported CPU means include setup/drain; peaks are100ms interval observations. These are short exploratory runs, not sustained H1/SLO results. Namespace loopback RX/TX and socket memory include unrelated traffic; broker-exclusive ingress/kernel memory is UNAVAILABLE.

## Seed42 workload observations

Rows below are individual first repetitions, not averages. `D/s` is physical delivery goodput over the active measured window, ending at the last offer/ACK/delivery; the JSON also includes the longer observation/drain-window rate. The denominator retains the offered workload, and rejection/client-drop counts are shown explicitly.

| Profile | Auth sessions | Offered/admitted/rejected/client-drop | SDK copies / missing admitted copies | Active D/s | ACK p99 ms | E2E p50/p95/p99/p99.9 ms | Broker peak RSS MiB | Broker/gen mean CPU% |
|---|---:|---|---|---:|---:|---|---:|---|
| A-basic | 2 | 1000/1000/0/0 | 1000 / 0 | 1001.3 | 0.490 | 0.406/0.537/0.640/0.822 | 3.61 | 18.1/21.7 |
| B-fanout | 1001 | 20/20/0/0 | 20000 / 0 | 20253.2 | 0.387 | 15.560/34.507/37.893/41.753 | 14.23 | 76.0/64.7 |
| C-many-to-many | 110 | 1000/1000/0/0 | 10000 / 0 | 9974.0 | 0.974 | 0.572/1.097/1.391/2.044 | 4.58 | 54.9/48.7 |
| E-slow | 3 | 500/500/0/0 | 632 / 368 | 196.5 | 0.534 | 0.443/2451.673/2724.424/2725.512 | 3.62 | 9.0/9.6 |
| E-credit-starved | 3 | 500/500/0/0 | 500 / 500 | 1002.0 | 0.451 | 0.419/0.522/0.589/0.837 | 3.66 | 9.5/9.9 |
| F-overload | 110 | 10000/699/9301/0 | 6752 / 238 | 11577.2 | 6.223 | 393.904/496.391/506.085/513.211 | 5.90 | 58.2/56.9 |
| F-burst | 20 | 5000/1017/1160/2823 | 10170 / 0 | 19115.2 | 3.055 | 255.842/406.184/417.866/419.757 | 4.28 | 54.9/44.9 |

A: 1p/1s, 1000 measured256B messages at1000/s; B: 1p/1000s, 20 measured256B messages at20/s; C: 100p/10s, 1000 measured1KiB messages at1000/s. Warmup remains in raw evidence and is excluded from measured counts. All three A/B/C repetitions received every expected admitted copy. B established1001 sessions, with20,000 measured physical copies each repetition.

For slow consumers, receipt delay can exceed broker TTL because data already written into the client/kernel socket is read later; TTL is enforced while queued in the broker, not at application receipt. In seed42 E-slow the healthy subscriber received all500 measured copies, the slow subscriber 132;368 copies were explicitly shed. In E-credit-starved the healthy subscriber received500 and the starved subscriber 0. Broker telemetry including warmup reports456 newest drops plus64 broker-queue TTL expirations. Those aggregate reasons are not assigned to individual measured missing IDs.

- E-slow-r1: subscriber 0 132 copies, E2E p99=2725.490ms, subscriber 1 500 copies, E2E p99=0.722ms.
- E-credit-starved-r1: subscriber 0 0 copies, latency UNAVAILABLE, subscriber 1 500 copies, E2E p99=0.589ms.

F-overload requested 100,000 offers/s but achieved 51486/s; max offer lateness 95.9ms and generator peak 392.0% (four-worker runtime) indicate generator pressure. This is joint-system overload evidence, not a claim that the broker sustains 100k/s. F-burst exposes bounded generator queue shedding separately. Rejected, missing, expired and client-dropped work is not hidden behind successful-ACK latency percentiles.

## Real idle connection sweep

| Actual authenticated | Failed attempts | Setup rate connections/s | Broker peak RSS MiB | Peak FDs | Mean broker CPU% (setup/hold/drain) |
|---:|---:|---:|---:|---:|---:|
| 128 | 0 | 1843.3 | 4.42 | 138 | 2.8 |
| 512 | 0 | 1945.6 | 7.11 | 522 | 10.0 |
| 1024 | 0 | 2119.2 | 11.26 | 1034 | 19.2 |
| 2000 | 0 | 2198.9 | 19.04 | 2010 | 30.2 |
| 4096 | 0 | 2268.9 | 35.63 | 4106 | 38.2 |

All three 4096-session repetitions succeeded: broker peak RSS 35.32–35.63MiB, FDs 4106; no failed connection attempts. Seed42 incremental peak-RSS slope from128 to4096 sessions is approximately 8248 bytes/connection. This is an idle allocator/runtime observation, not a full-load or worst-case memory promise. The default1024 limit is configurable, not a demonstrated hardware ceiling; above it the server requires an explicit conservative1,380,352-byte/session envelope plus external headroom verification.
10,000 sessions: **NOT_RUN** in all three repetitions: requires 13803520000 bytes connection envelope, exceeds 75% of 9955053568 available bytes. The requested envelope is13,803,520,000 bytes. Maximum tested is4096; maximum possible capacity is unknown. Kernel TCP memory is recorded as a namespace aggregate, not added to broker RSS as if exclusive.

## Correctness finding and limits

The first matrix at 942f4ef contained an INVALID E-slow run with 52 ordering inversions. A newly available credit could be taken by a later item during the same queue scan. The deterministic transport regression `credit_blocked_head_cannot_be_overtaken_within_subscription` failed before the narrow FIFO fix and passes after. Commit f9221b3 retries blocked subscriptions only on a new scan. The original failing event trace and summary are retained in `before-fix/`; every corrected scenario was rerun three times, with zero ordering inversions.

R0-02 validation: 81 Rust unit/integration tests after the transport regression; 10 Python sampler/export tests; formatter, locked workspace check, independent TCP smoke and 36 trace replays. The uncorrected initial failure is retained, never relabeled PASS. Q1/Q2/recovery and NATS comparisons are NOT_RUN. A-07 remains IN_PROGRESS pending a safe 10k-session host/profile and a sustainable-rate experiment whose generator is not the limiting factor. No production PERF/VAL-01 claim is made.
