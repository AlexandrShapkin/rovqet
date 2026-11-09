# NATS Core / Rovqet Q0 exploratory comparison — 438fa0c

R0-03 is **DONE**. A-07 remains **IN_PROGRESS**. The independent client/oracle uses official `async-nats = 0.39.0` and checksum-pinned NATS Server **2.11.10**. All final workload runs identify clean source commit `438fa0c622bb40180b2f526e7ba5ea1781a32bf5`. These are short, same-host exploratory measurements, not production H1/PERF/VAL-01 qualification.

## Reproduce

```sh
git checkout 438fa0c
cargo build --release --locked -p rovqet-server -p rovqet-testkit
python3 benchmarks/install_nats.py
python3 benchmarks/run_q0.py --output benchmarks/results/new-rovqet --repetitions 3
python3 benchmarks/run_q0.py --backend nats --output benchmarks/results/new-nats --repetitions 3
python3 benchmarks/compare_q0.py --rovqet benchmarks/results/new-rovqet --nats benchmarks/results/new-nats --output /tmp/new-comparison.json
```

Compact exports use the existing exporter (a new output directory is required):

```sh
python3 benchmarks/export_q0.py --source benchmarks/results/new-rovqet --output /tmp/new-rovqet-export --max-distribution-bytes 3145728
python3 benchmarks/export_q0.py --source benchmarks/results/new-nats --output /tmp/new-nats-export --max-distribution-bytes 3145728
gzip -n -c benchmarks/results/new-rovqet/A-basic-r1/generator/summary.json > /tmp/new-rovqet-export/representative-summary.json.gz
gzip -n -c benchmarks/results/new-nats/A-basic-r1/generator/summary.json > /tmp/new-nats-export/representative-summary.json.gz
```

Each backend ran sequentially, with three independent server/generator process pairs per profile and seeds42/43/44. The second backend ran after the first, not concurrently; this is not randomized/interleaved or CPU-pinned. Host contention, temperature and short duration limit causal performance conclusions.

## Semantics and evidence

- Rovqet ADMITTED is volatile admission; NATS publish is unconfirmed client enqueue, never admission.
- Compare actual application deliveries and same-generator-clock E2E only. Publication latencies are not equivalent.
- Core has no Rovqet credits/TTL/egress quotas; slow/overload/burst outcomes are policy observations, not equivalent capacity.
- NATS has one subject; Rovqet uses four demo partitions with fixed publisher key. No global order claim.
- Plaintext loopback lab, short repetitions; CPU includes setup/drain. No H1/VAL-01 claim.
- No average percentiles; exact distributions retained by export_q0.py. Namespace network/socket memory not process-exclusive.

NATS server authentication uses the same fixed development identity and plaintext loopback transport. Core permits 256KiB payloads and 256KiB pending output per connection; Rovqet permits 1MiB frames and has hierarchical reservations, credits, egress quotas, DROP_NEWEST and a2s workload TTL. Workload payloads match at256B/1KiB. Those limits are not equivalent buffering policies. NATS has no delivery-credit operation, so that profile is NOT_RUN. The official NATS client independently reads sockets into a bounded64-message subscriber channel; slow application reads can drop there. Notification counts are lower bounds and include warmup. Missing unconfirmed copies are observations, not definite admitted loss.

An official-client temporary-subject self-echo establishes authentication, subscription readiness and final publication processing. It never acknowledges application receipt. This adds control traffic outside the workload IDs; process/network counters include it. Core local publish enqueue completions are recorded separately and its ACK histogram is empty.

Full raw outputs remain locally under ignored `benchmarks/results/comparison-438fa0c-{rovqet,nats}`. Each backend export contains every per-run summary and percentile, a SHA256 source index, resource samples, retained exact latency distributions and a complete representative event trace/summary. Export metadata explicitly identifies any distribution subset omitted by the archive size cap; full original distributions remain locally available. Hashes are not substitutes for omitted raw traces. `comparison.json` pairs profile/seed and rejects workload or host-identity mismatches; no percentiles are averaged.

## Run status

- rovqet: 45 entries; COMPLETE=36, INVALID=3, NOT_RUN=6.
- nats: 45 entries; COMPLETE=30, INVALID=3, NOT_RUN=12.

All66 COMPLETE runs and the three generator-limited Rovqet traces independently replayed successfully (69 passes). The three NATS overload traces failed replay as expected after bounded event-channel overflow: recorded offered counts6892/6772/7245 are incomplete subsets of the configured10000 offers, not true offered totals. Their publication/delivery counters and latency distributions are unsuitable for comparative conclusions. The original partial evidence remains preserved and marked INVALID. All66 COMPLETE runs had zero duplicates, corruption, scoped inversions and unknown outcomes. Rovqet final queue occupancy, subscriptions and work sessions were zero.

## Seed42 observations

Individual r1 results below; `D/s` is physical deliveries over first measured offer through last offer/outcome/delivery. Counts exclude warmup. Resource CPU means span sampled process lifetime including setup/drain; one core=100%. Missing means missing **admitted** copies for Rovqet and unreceived **unconfirmed** copies for NATS.

| Profile | Backend | Sessions | Offered / admitted or unconfirmed / rejected / client shed | Copies / missing | Active D/s | E2E p50/p95/p99/p99.9 ms | Server RSS MiB | Server/gen mean CPU% | Status |
|---|---|---:|---|---|---:|---|---:|---|---|
| A-basic | rovqet | 2 | 1000/1000/0/0 | 1000 / 0 | 1001.170 | 0.465/0.921/1.866/2.947 | 3.645 | 19.400/24.977 | COMPLETE |
| A-basic | nats | 2 | 1000/1000/0/0 | 1000 / 0 | 1001.073 | 0.352/0.610/0.812/1.503 | 15.965 | 10.825/19.738 | COMPLETE |
| B-fanout | rovqet | 1001 | 20/14/6/0 | 14000 / 0 | 11583.365 | 145.821/241.243/322.988/426.741 | 14.266 | 74.856/72.440 | COMPLETE |
| B-fanout | nats | 1001 | 20/20/0/0 | 20000 / 0 | 20142.080 | 20.382/40.479/49.619/69.266 | 75.188 | 66.967/52.299 | COMPLETE |
| C-many-to-many | rovqet | 110 | 1000/1000/0/0 | 10000 / 0 | 8911.299 | 17.113/134.016/142.719/147.261 | 4.766 | 63.890/62.108 | COMPLETE |
| C-many-to-many | nats | 110 | 1000/1000/0/0 | 10000 / 0 | 9934.432 | 0.612/1.142/1.668/3.700 | 25.969 | 31.288/36.353 | COMPLETE |
| E-credit-starved | rovqet | 3 | 500/500/0/0 | 500 / 500 | 999.347 | 0.465/0.678/0.783/0.890 | 3.645 | 9.872/11.232 | COMPLETE |
| E-credit-starved | nats | — | 0/0/0/0 | 0 / 0 | UNAVAILABLE | UNAVAILABLE/UNAVAILABLE/UNAVAILABLE/UNAVAILABLE | UNAVAILABLE | UNAVAILABLE/UNAVAILABLE | NOT_RUN |
| E-slow | rovqet | 3 | 500/500/0/0 | 632 / 368 | 196.236 | 0.467/2454.970/2722.096/2722.982 | 3.594 | 9.295/11.017 | COMPLETE |
| E-slow | nats | 3 | 500/500/0/0 | 569 / 431 | 308.622 | 0.451/1210.167/1356.679/1357.307 | 17.520 | 6.207/9.294 | COMPLETE |
| F-burst | rovqet | 20 | 5000/1061/967/2972 | 8180 / 2430 | 9440.261 | 444.199/659.940/671.126/676.605 | 4.465 | 61.078/60.005 | COMPLETE |
| F-burst | nats | 20 | 5000/5000/0/0 | 6257 / 43743 | 27016.671 | 29.648/74.601/86.324/96.640 | 22.012 | 10.659/47.322 | COMPLETE |
| F-controlled-10000 | rovqet | 11 | 10000/7971/2029/0 | 1322 / 6649 | 1232.612 | 154.955/214.797/224.911/228.900 | 5.277 | 64.190/84.040 | COMPLETE |
| F-controlled-10000 | nats | 11 | 10000/10000/0/0 | 10000 / 0 | 9975.379 | 1.189/3.588/5.424/7.305 | 20.430 | 35.364/83.979 | COMPLETE |
| F-controlled-5000 | rovqet | 11 | 5000/5000/0/0 | 2435 / 2565 | 2361.287 | 51.916/61.478/62.706/63.457 | 3.660 | 55.491/67.237 | COMPLETE |
| F-controlled-5000 | nats | 11 | 5000/5000/0/0 | 5000 / 0 | 5000.880 | 0.495/0.889/1.313/2.240 | 19.625 | 24.818/51.607 | COMPLETE |
| F-overload | rovqet | 110 | 10000/956/9044/0 | 4613 / 4947 | 4482.878 | 637.293/785.583/799.427/804.044 | 6.270 | 66.086/89.696 | INVALID |
| F-overload | nats | 110 | UNAVAILABLE: incomplete event evidence | UNAVAILABLE | UNAVAILABLE | UNAVAILABLE | 29.035 | 32.250/72.053 | INVALID |

The unconfirmed NATS column is not an admission count. Rejected=0 there means no locally reported per-message rejection; server admission/rejection/drop/expiry attribution is UNAVAILABLE in this Core runner. Received-subset latencies must be read with every rejected, shed, unknown and unreceived count. INVALID runs are retained, not valid sustainable-rate estimates. Burst has no sustained target and its client-side shedding is explicit.

## Repetitions and density

| Profile | Backend | r1/r2/r3 physical copies | r1/r2/r3 E2E p99 ms |
|---|---|---|---|
| A-basic | rovqet | 1000/1000/1000 | 1.866/1.133/0.986 |
| A-basic | nats | 1000/1000/1000 | 0.812/0.785/0.672 |
| B-fanout | rovqet | 14000/13000/17000 | 322.988/501.931/383.431 |
| B-fanout | nats | 20000/20000/20000 | 49.619/41.091/47.285 |
| C-many-to-many | rovqet | 10000/8870/10000 | 142.719/220.043/109.708 |
| C-many-to-many | nats | 10000/10000/10000 | 1.668/1.685/1.440 |
| F-controlled-5000 | rovqet | 2435/2537/2486 | 62.706/62.984/69.405 |
| F-controlled-5000 | nats | 5000/5000/5000 | 1.313/5.644/6.550 |
| F-controlled-10000 | rovqet | 1322/1358/1361 | 224.911/206.185/187.958 |
| F-controlled-10000 | nats | 10000/10000/10000 | 5.424/4.271/5.864 |

Idle-session sweep (real authenticated TCP, no simulated clients):

| Backend/profile | Actual sessions r1/r2/r3 | Peak RSS MiB r1/r2/r3 | Peak FD r1/r2/r3 | Status |
|---|---|---|---|---|
| rovqet/D-idle-10000 | —/—/— | UNAVAILABLE/UNAVAILABLE/UNAVAILABLE | —/—/— | NOT_RUN/NOT_RUN/NOT_RUN |
| rovqet/D-idle-1024 | 1024/1024/1024 | 11.406/11.234/11.203 | 1034/1034/1034 | COMPLETE/COMPLETE/COMPLETE |
| rovqet/D-idle-128 | 128/128/128 | 4.215/4.371/4.371 | 138/138/138 | COMPLETE/COMPLETE/COMPLETE |
| rovqet/D-idle-2000 | 2000/2000/2000 | 18.898/18.922/19.016 | 2010/2010/2010 | COMPLETE/COMPLETE/COMPLETE |
| rovqet/D-idle-4096 | —/—/— | UNAVAILABLE/UNAVAILABLE/UNAVAILABLE | —/—/— | NOT_RUN/NOT_RUN/NOT_RUN |
| rovqet/D-idle-512 | 512/512/512 | 7.434/7.430/7.457 | 522/522/522 | COMPLETE/COMPLETE/COMPLETE |
| nats/D-idle-10000 | —/—/— | UNAVAILABLE/UNAVAILABLE/UNAVAILABLE | —/—/— | NOT_RUN/NOT_RUN/NOT_RUN |
| nats/D-idle-1024 | 1024/1024/1024 | 67.531/67.426/75.781 | 1030/1030/1030 | COMPLETE/COMPLETE/COMPLETE |
| nats/D-idle-128 | 128/128/128 | 26.141/24.324/25.969 | 134/134/134 | COMPLETE/COMPLETE/COMPLETE |
| nats/D-idle-2000 | —/—/— | UNAVAILABLE/UNAVAILABLE/UNAVAILABLE | —/—/— | NOT_RUN/NOT_RUN/NOT_RUN |
| nats/D-idle-4096 | —/—/— | UNAVAILABLE/UNAVAILABLE/UNAVAILABLE | —/—/— | NOT_RUN/NOT_RUN/NOT_RUN |
| nats/D-idle-512 | 512/512/512 | 40.566/43.242/43.156 | 518/518/518 | COMPLETE/COMPLETE/COMPLETE |

The common high-density guard conservatively requires the configured Rovqet connection envelope to fit75% of current memory headroom for either baseline. A skipped NATS density run is not proof of a NATS capacity limit. The previously measured4,096-session Rovqet result remains valid at its earlier environment state; current availability may support fewer. Potential bounded queued capacity and actual idle RSS are different quantities. No safety limit was reduced.10,000 sessions were not attempted. The envelope includes potential1MiB frame buffers and256KiB outbound capacity per connection plus control/metadata overhead; it is a configuration guard, not preallocated resident memory. Idle RSS therefore does not validate the partial-frame/queued-payload worst case, and no budget reduction is justified by that slope alone.

## Invalid and unavailable cases

- rovqet/D-idle-4096/seed42: **NOT_RUN** — requires 5653921792 bytes connection envelope, exceeds 75% of 4204175360 available bytes.
- rovqet/D-idle-4096/seed43: **NOT_RUN** — requires 5653921792 bytes connection envelope, exceeds 75% of 4204175360 available bytes.
- rovqet/D-idle-4096/seed44: **NOT_RUN** — requires 5653921792 bytes connection envelope, exceeds 75% of 4204204032 available bytes.
- rovqet/D-idle-10000/seed42: **NOT_RUN** — requires 13803520000 bytes connection envelope, exceeds 75% of 4204212224 available bytes.
- rovqet/D-idle-10000/seed43: **NOT_RUN** — requires 13803520000 bytes connection envelope, exceeds 75% of 4204093440 available bytes.
- rovqet/D-idle-10000/seed44: **NOT_RUN** — requires 13803520000 bytes connection envelope, exceeds 75% of 4204101632 available bytes.
- rovqet/F-overload/seed42: **INVALID** — generator offered 31247.905/s below 90% of requested 100000/s; generator max offer lateness exceeds max(5ms, 10% of offer duration).
- rovqet/F-overload/seed43: **INVALID** — generator offered 32374.410/s below 90% of requested 100000/s; generator max offer lateness exceeds max(5ms, 10% of offer duration).
- rovqet/F-overload/seed44: **INVALID** — generator offered 26691.846/s below 90% of requested 100000/s; generator max offer lateness exceeds max(5ms, 10% of offer duration).
- nats/D-idle-2000/seed42: **NOT_RUN** — requires 2760704000 bytes connection envelope, exceeds 75% of 3637964800 available bytes.
- nats/D-idle-2000/seed43: **NOT_RUN** — requires 2760704000 bytes connection envelope, exceeds 75% of 3637964800 available bytes.
- nats/D-idle-2000/seed44: **NOT_RUN** — requires 2760704000 bytes connection envelope, exceeds 75% of 3637817344 available bytes.
- nats/D-idle-4096/seed42: **NOT_RUN** — requires 5653921792 bytes connection envelope, exceeds 75% of 3637817344 available bytes.
- nats/D-idle-4096/seed43: **NOT_RUN** — requires 5653921792 bytes connection envelope, exceeds 75% of 3637321728 available bytes.
- nats/D-idle-4096/seed44: **NOT_RUN** — requires 5653921792 bytes connection envelope, exceeds 75% of 3637321728 available bytes.
- nats/D-idle-10000/seed42: **NOT_RUN** — requires 13803520000 bytes connection envelope, exceeds 75% of 3637338112 available bytes.
- nats/D-idle-10000/seed43: **NOT_RUN** — requires 13803520000 bytes connection envelope, exceeds 75% of 3637338112 available bytes.
- nats/D-idle-10000/seed44: **NOT_RUN** — requires 13803520000 bytes connection envelope, exceeds 75% of 3637338112 available bytes.
- nats/E-credit-starved/seed42: **NOT_RUN** — NATS Core has no Rovqet delivery-credit operation; credit-starved profile is not equivalent.
- nats/E-credit-starved/seed43: **NOT_RUN** — NATS Core has no Rovqet delivery-credit operation; credit-starved profile is not equivalent.
- nats/E-credit-starved/seed44: **NOT_RUN** — NATS Core has no Rovqet delivery-credit operation; credit-starved profile is not equivalent.
- nats/F-overload/seed42: **INVALID** — generator summary must explicitly declare valid=true.
- nats/F-overload/seed43: **INVALID** — generator summary must explicitly declare valid=true.
- nats/F-overload/seed44: **INVALID** — generator summary must explicitly declare valid=true.

## Generator and measurement limitations

`generator-instrumentation.json` records a targeted dirty-tree pre/post diagnostic, not baseline evidence. Payload/digest elapsed dropped238.83→147.29ms and delivery-digest elapsed89.71→40.84ms in one pair after replacing32 per-byte formatting allocations with one fixed-capacity hex string and removing one redundant receive clone. Both100k/s profiles remained INVALID; no statistically established speedup or sustainable service-rate claim follows. Stage elapsed timings overlap across tasks and include preemption. Exact histogram aggregation, evidence enqueue and scheduling remain material; the controlled rate profiles and saturation guards expose their limits. Hardware perf recording was UNAVAILABLE because the host denied perf events; no sysctl changes were made.

All latency timestamps originate in one independent generator process monotonic clock. No unsynchronized cross-host subtraction. Server/generator are distinct processes on the same CPU package. Resource sampler interval100ms; namespace loopback traffic and socket memory include unrelated host activity. Per-process exclusive kernel socket memory and Core per-message reject/drop attribution are UNAVAILABLE. Software binary hashes are in `software.json`. See each backend results.json environment for exact CPU/kernel/RAM/FD/TCP values and commands. Available RAM can change between runs. Short profiles and upper-layer queue differences prevent a general performance ranking.

## Verification and next work

Executed: workspace formatting and locked compilation;84 Rust workspace tests;16 focused testkit tests after final generator changes;22 Python tests; independent SDK publisher/two-subscriber TCP smoke; real pinned NATS1p1s smoke and JSONL replay; legacyQ0 trace replay. Final matrix replay counts are recorded in `replay-validation.json`. CI42707917261 passed both jobs and both real TCP smokes. CI includes both real socket smokes and their oracle replays.

R0-04 inspected: TCP and segmented WAL remain the reference defaults; no transport redesign or speculative spike is needed for this Q0 baseline. Its explicit default rationale/spike evidence and R0-05 wire/storage seam freeze remain TODO before Q1 preparation is complete. A-07 still needs safe higher-density environment and generator-valid sustainable-rate investigation. No Q1/Q2 functionality or durability comparison was implemented.
