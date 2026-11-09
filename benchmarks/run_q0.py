#!/usr/bin/env python3
"""Separate broker/testkit processes with bounded Linux resource sampling.

No percentile aggregation across runs. All raw resource samples remain JSONL.
Kernel socket and loopback counters are network-namespace aggregates, never
misrepresented as broker-exclusive usage. CPU percent uses one logical core=100%.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import platform
import resource
import selectors
import signal
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
PAGE = os.sysconf("SC_PAGE_SIZE")
HZ = os.sysconf("SC_CLK_TCK")


def read_text(path):
    try:
        return Path(path).read_text().strip()
    except (OSError, ValueError):
        return None


def process_sample(pid, proc_root=Path("/proc")):
    """RSS/current FDs and cumulative CPU; stat comm may contain parentheses."""
    try:
        stat = (proc_root / str(pid) / "stat").read_text()
        fields = stat[stat.rfind(")") + 2:].split()
        cpu_ticks = int(fields[11]) + int(fields[12])
        rss_bytes = int(fields[21]) * PAGE
        fds = len(list((proc_root / str(pid) / "fd").iterdir()))
        return {"pid": pid, "rss_bytes": rss_bytes, "cpu_seconds": cpu_ticks / HZ,
                "fd_count": fds}
    except (OSError, ValueError, IndexError):
        return {"pid": pid, "status": "UNAVAILABLE", "reason": "process exited or proc inaccessible"}


def network_sample(proc_root=Path("/proc")):
    result = {"scope": "loopback network-namespace counters; sockstat memory may include host activity", "interface": "lo"}
    try:
        for line in (proc_root / "net/dev").read_text().splitlines():
            if ":" in line:
                name, data = line.split(":", 1)
                if name.strip() == "lo":
                    values = data.split()
                    result.update(rx_bytes=int(values[0]), tx_bytes=int(values[8]))
        sockstat = (proc_root / "net/sockstat").read_text().strip()
        result["sockstat_raw"] = sockstat
        for line in sockstat.splitlines():
            if line.startswith("TCP:"):
                fields = line.split()[1:]
                values = dict(zip(fields[::2], map(int, fields[1::2])))
                result["tcp"] = values
                if "mem" in values:
                    result["tcp_socket_memory_bytes"] = values["mem"] * PAGE
        if "rx_bytes" not in result:
            result["network_status"] = "UNAVAILABLE: loopback interface absent"
    except (OSError, ValueError, IndexError) as exc:
        result["status"] = "UNAVAILABLE"
        result["reason"] = str(exc)
    return result


def environment():
    cpuinfo = read_text("/proc/cpuinfo") or ""
    cpu_model = next((s.split(":", 1)[1].strip() for s in cpuinfo.splitlines()
                      if s.startswith("model name")), "UNAVAILABLE")
    paths = {
        "memory": "/proc/meminfo", "cpu_quota": "/sys/fs/cgroup/cpu.max",
        "memory_limit": "/sys/fs/cgroup/memory.max", "memory_current": "/sys/fs/cgroup/memory.current",
        "somaxconn": "/proc/sys/net/core/somaxconn", "tcp_rmem": "/proc/sys/net/ipv4/tcp_rmem",
        "tcp_wmem": "/proc/sys/net/ipv4/tcp_wmem", "port_range": "/proc/sys/net/ipv4/ip_local_port_range",
        "file_max": "/proc/sys/fs/file-max", "sockets": "/proc/net/sockstat",
    }
    return {"utc": dt.datetime.now(dt.timezone.utc).isoformat(), "platform": platform.platform(),
            "kernel": platform.release(), "machine": platform.machine(), "cpu_model": cpu_model,
            "logical_cpus": os.cpu_count(), "clock_ticks_per_sec": HZ, "page_bytes": PAGE,
            "rlimit_nofile": list(resource.getrlimit(resource.RLIMIT_NOFILE)),
            "linux": {name: read_text(path) or "UNAVAILABLE" for name, path in paths.items()},
            "latency_clock": "all generator send/ACK/delivery timestamps share one process monotonic origin; broker clock not subtracted",
            "resource_notes": "CPU one core=100%; /proc/net counters share network namespace and may contain unrelated traffic"}


def git_info():
    def git(*args):
        run = subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True)
        return run.stdout.strip() if run.returncode == 0 else "UNAVAILABLE"
    return {"git_sha": git("rev-parse", "HEAD"), "working_tree_dirty": bool(git("status", "--porcelain"))}


class ResourceSampler:
    def __init__(self, path, interval=0.1):
        self.path = path
        self.interval = interval
        self.broker_pid = None
        self.generator_pid = None
        self.stop_event = threading.Event()
        self.started = time.monotonic_ns()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.error = None

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise RuntimeError("resource sampler did not stop")
        if self.error:
            raise RuntimeError(f"resource evidence failed: {self.error}")

    def run(self):
        try:
            # Buffered file, controlled flush on each 10 samples (one second).
            with self.path.open("x", buffering=256 * 1024) as output:
                count = 0
                while True:
                    record = {"time_mono_ns": time.monotonic_ns() - self.started,
                              "network": network_sample()}
                    if self.broker_pid is not None:
                        record["broker"] = process_sample(self.broker_pid)
                    if self.generator_pid is not None:
                        record["generator"] = process_sample(self.generator_pid)
                    output.write(json.dumps(record, separators=(",", ":")) + "\n")
                    count += 1
                    if count % 10 == 0:
                        output.flush()
                    if self.stop_event.wait(self.interval):
                        break
        except Exception as exc:
            self.error = str(exc)


def summarize_resources(path):
    records = [json.loads(line) for line in path.read_text().splitlines()]
    summary = {}
    for kind in ("broker", "generator"):
        samples = [(r["time_mono_ns"], r[kind]) for r in records
                   if kind in r and "cpu_seconds" in r[kind]]
        if not samples:
            summary[kind] = {"status": "UNAVAILABLE", "reason": "no live process samples"}
            continue
        cpus = []
        for (ta, a), (tb, b) in zip(samples, samples[1:]):
            if tb > ta:
                cpus.append(100 * (b["cpu_seconds"] - a["cpu_seconds"]) / ((tb - ta) / 1e9))
        elapsed = (samples[-1][0] - samples[0][0]) / 1e9
        summary[kind] = {
            "samples": len(samples), "sampled_seconds": elapsed,
            "rss_min_bytes": min(s["rss_bytes"] for _, s in samples),
            "rss_max_bytes": max(s["rss_bytes"] for _, s in samples),
            "fd_max": max(s["fd_count"] for _, s in samples),
            "cpu_seconds_delta": samples[-1][1]["cpu_seconds"] - samples[0][1]["cpu_seconds"],
            "cpu_percent_mean": 100 * (samples[-1][1]["cpu_seconds"] - samples[0][1]["cpu_seconds"]) / elapsed if elapsed > 0 else None,
            "cpu_percent_peak_interval": max(cpus) if cpus else None,
        }
    if records:
        a, b = records[0]["network"], records[-1]["network"]
        summary["network"] = {"scope": "loopback network-namespace counters; sockstat memory may include host activity", "interface": "lo"}
        for counter in ("rx_bytes", "tx_bytes"):
            if counter in a and counter in b:
                summary["network"][counter + "_delta"] = b[counter] - a[counter]
            else:
                summary["network"][counter + "_delta"] = None
        socket_samples = [r["network"]["tcp_socket_memory_bytes"] for r in records
                          if "tcp_socket_memory_bytes" in r["network"]]
        summary["network"]["tcp_socket_memory_max_bytes"] = max(socket_samples) if socket_samples else None
        summary["network"]["tcp_socket_memory_status"] = "AVAILABLE_NAMESPACE_AGGREGATE" if socket_samples else "UNAVAILABLE"
    return summary


def await_readiness(process, path, timeout_seconds=15, backend="rovqet"):
    """Persist startup bytes without TextIO read-ahead hiding readiness lines."""
    deadline = time.monotonic() + timeout_seconds
    marker = "Listening for client connections on " if backend == "nats" else "listening on "
    pending = ""
    with selectors.DefaultSelector() as selector:
        selector.register(process.stderr, selectors.EVENT_READ)
        while time.monotonic() < deadline:
            if not selector.select(max(0, deadline - time.monotonic())):
                break
            chunk = os.read(process.stderr.fileno(), 65536)
            if not chunk:
                break
            decoded = chunk.decode("utf-8", errors="replace")
            path.write(decoded)
            path.flush()
            pending += decoded
            for line in pending.splitlines(keepends=True):
                if not line.endswith("\n"):
                    break
                if marker in line:
                    return line.split(marker, 1)[1].strip()
            pending = pending.rsplit("\n", 1)[-1]
            if len(pending) > 65536:
                raise RuntimeError("broker startup log line exceeds bound")
    raise RuntimeError("broker did not report listener readiness")


def stop_process(process, signal_first=False):
    if process is None or process.poll() is not None:
        return
    if signal_first:
        process.send_signal(signal.SIGINT)
    else:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def command_for(profile, endpoint, seed, output, testkit):
    command = [str(testkit), "--endpoint", endpoint, "--seed", str(seed), "--output", str(output)]
    for name, value in profile.items():
        if name.startswith("broker_"):
            continue
        flag = "--" + name.replace("_", "-")
        command += [flag, str(value).lower() if isinstance(value, bool) else str(value)]
    return command


def connection_budget(profile, env):
    """Reject unsafe high-density reservations rather than changing the limit blindly."""
    connections = profile.get("broker_max_connections", 1024)
    if connections <= 1024:
        return None, None
    envelope = connections * 1_380_352
    available = None
    for line in env["linux"]["memory"].splitlines():
        if line.startswith("MemAvailable:"):
            available = int(line.split()[1]) * 1024
    limit = env["linux"]["memory_limit"]
    current = env["linux"]["memory_current"]
    if str(limit).isdigit() and str(current).isdigit():
        remaining = max(0, int(limit) - int(current))
        available = remaining if available is None else min(available, remaining)
    if available is None:
        return None, "UNAVAILABLE memory headroom; high-density run not authorized by a known safety envelope"
    if envelope > available * 0.75:
        return None, f"requires {envelope} bytes connection envelope, exceeds 75% of {available} available bytes"
    return envelope, None


def saturation_reasons(profile, result):
    """Conservative generator-validity guard, independent of broker delivery success."""
    rate = profile.get("rate", 0)
    if not rate or profile.get("idle_connections", 0):
        return []  # Burst client shedding is explicit, but is not a sustained-rate claim.
    measurement, oracle = result.get("measurement", {}), result.get("oracle", {})
    offered = oracle.get("offered", 0)
    duration = measurement.get("offer_secs", 0)
    actual = max(0, offered - 1) / duration if duration > 0 else 0
    reasons = []
    if actual < rate * 0.9:
        reasons.append(f"generator offered {actual:.3f}/s below 90% of requested {rate}/s")
    if measurement.get("max_offer_lateness_ns", 0) > max(5_000_000, duration * 1e8):
        reasons.append("generator max offer lateness exceeds max(5ms, 10% of offer duration)")
    if oracle.get("client_dropped", 0):
        reasons.append("bounded generator publisher queue shed sustained-rate offers")
    return reasons


def validate_artifacts(output, profile):
    """Require independently replayable evidence and live process resource samples."""
    summary_path = output / "generator/summary.json"
    if not summary_path.is_file():
        raise ValueError("missing generator summary")
    result = json.loads(summary_path.read_text())
    if not isinstance(result, dict) or result.get("valid") is not True:
        raise ValueError("generator summary must explicitly declare valid=true")
    for key in ("oracle", "config", "measurement"):
        if not isinstance(result.get(key), dict):
            raise ValueError(f"generator summary missing {key} object")
    events = output / "generator/events.jsonl"
    if not events.is_file() or events.stat().st_size == 0:
        raise ValueError("missing or empty independent JSONL event evidence")
    resources = output / "resources.jsonl"
    if not resources.is_file() or resources.stat().st_size == 0:
        raise ValueError("missing or empty process resource evidence")
    samples = [json.loads(line) for line in resources.read_text().splitlines()]
    for process in ("broker", "generator"):
        if not any(isinstance(row.get(process), dict) and
                   all(isinstance(row[process].get(key), (float, int)) and row[process][key] >= 0
                       for key in ("rss_bytes", "cpu_seconds", "fd_count")) for row in samples):
            raise ValueError(f"no valid live {process} resource samples")
    return result


def nats_command(binary, profile, output):
    version = subprocess.run([str(binary), "--version"], capture_output=True, text=True, timeout=10)
    if version.returncode != 0 or version.stdout.strip() != "nats-server: v2.11.10":
        raise ValueError("NATS baseline requires pinned nats-server v2.11.10")
    config = (ROOT / "benchmarks/nats/core.conf").read_text()
    config = config.replace("max_connections: 1024", f"max_connections: {profile.get('broker_max_connections', 1024)}")
    path = output / "nats.conf"
    path.write_text(config)
    return [str(binary), "--config", str(path)], version.stdout.strip()


def one_run(name, profile, seed, output, broker_binary, testkit_binary, sample_interval=0.1, timeout_seconds=120, backend="rovqet"):
    if backend not in ("rovqet", "nats"):
        raise ValueError("unknown benchmark backend")
    output.mkdir(parents=True, exist_ok=False)
    metadata = {"profile": name, "profile_config": profile, "seed": seed,
                "environment": environment(), **git_info(), "status": "INCOMPLETE", "backend": backend}
    budget, skip_reason = connection_budget(profile, metadata["environment"])
    if backend == "nats" and profile.get("credit_starved", 0):
        skip_reason = "NATS Core has no Rovqet delivery-credit operation; credit-starved profile is not equivalent"
    if skip_reason:
        metadata.update(status="NOT_RUN", reason=skip_reason, wall_seconds=0)
        (output / "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
        return metadata
    broker_command = [str(broker_binary), "--listen", "127.0.0.1:0", "--max-connections",
                      str(profile.get("broker_max_connections", 1024)), "--metrics-interval-ms", "250"]
    if budget is not None:
        broker_command += ["--connection-budget-bytes", str(budget)]
    metadata["broker_command"] = broker_command
    broker = generator = None
    sampler = ResourceSampler(output / "resources.jsonl", sample_interval)
    stderr_thread = None
    drain_errors = []
    opened = []
    started = time.monotonic()
    try:
        if backend == "nats":
            broker_command, version = nats_command(broker_binary, profile, output)
            metadata.update(broker_command=broker_command, server_version=version,
                            server_metrics_status="UNAVAILABLE: Core per-publication admission/drop/expiry counters not provided by this runner")
        broker_error = (output / "broker.stderr").open("x")
        broker_out = (output / "broker.stdout").open("x")
        opened.extend([broker_error, broker_out])
        broker = subprocess.Popen(broker_command, stdout=broker_out, stderr=subprocess.PIPE,
                                  text=True, bufsize=1)
        sampler.broker_pid = broker.pid
        sampler.start()
        endpoint = await_readiness(broker, broker_error, backend=backend)
        # Drain stderr concurrently to avoid blocking the broker on metric evidence.
        def drain_stderr():
            try:
                for line in broker.stderr:
                    broker_error.write(line)
                broker_error.flush()
            except Exception as exc:
                drain_errors.append(str(exc))
        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stderr_thread.start()
        generator_command = command_for(profile, endpoint, seed, output / "generator", testkit_binary)
        if backend == "nats":
            generator_command += ["--backend", "nats"]
        metadata["generator_command"] = generator_command
        metadata["endpoint"] = endpoint
        gen_out = (output / "generator.stdout").open("x")
        gen_err = (output / "generator.stderr").open("x")
        opened.extend([gen_out, gen_err])
        generator = subprocess.Popen(generator_command, stdout=gen_out, stderr=gen_err)
        sampler.generator_pid = generator.pid
        metadata["generator_exit"] = generator.wait(timeout=timeout_seconds)
        metadata["status"] = "COMPLETE" if generator.returncode == 0 else "INVALID"
    except Exception as exc:
        metadata["status"] = "INVALID"
        metadata["error"] = str(exc)
    finally:
        stop_process(generator)
        stop_process(broker, signal_first=True)
        if broker is not None:
            metadata["broker_exit"] = broker.returncode
            if broker.returncode != 0:
                metadata["status"] = "INVALID"
        if stderr_thread:
            stderr_thread.join(timeout=5)
            if stderr_thread.is_alive() or drain_errors:
                metadata["status"] = "INVALID"
                metadata["stderr_error"] = drain_errors or ["drain thread did not finish"]
        if broker is not None and broker.stderr is not None:
            broker.stderr.close()
        if sampler.thread.ident is not None:
            try:
                sampler.stop()
            except Exception as exc:
                metadata["status"] = "INVALID"
                metadata["sampler_error"] = str(exc)
        for file in opened:
            file.close()
        metadata["wall_seconds"] = time.monotonic() - started
        if (output / "resources.jsonl").exists():
            try:
                metadata["resources"] = summarize_resources(output / "resources.jsonl")
            except (OSError, ValueError, KeyError, TypeError) as exc:
                metadata.update(status="INVALID", resource_error=str(exc))
        generator_summary = output / "generator/summary.json"
        if generator_summary.exists():
            try:
                metadata["generator_result"] = json.loads(generator_summary.read_text())
                validate_artifacts(output, profile)
                metadata["generator_saturation_reasons"] = saturation_reasons(profile, metadata["generator_result"])
                if metadata["generator_saturation_reasons"]:
                    metadata["status"] = "INVALID"
                if not (output / "generator/events.jsonl").is_file():
                    metadata["status"] = "INVALID"
                    metadata["events_error"] = "missing independent JSONL event evidence"
                if metadata["generator_result"].get("valid") is False:
                    metadata["status"] = "INVALID"
            except (OSError, ValueError, TypeError, AttributeError) as exc:
                metadata["status"] = "INVALID"
                metadata["summary_error"] = str(exc)
        elif metadata["status"] == "COMPLETE":
            metadata["status"] = "INVALID"
            metadata["summary_error"] = "missing generator summary"
        metrics = []
        stderr_path = output / "broker.stderr"
        if stderr_path.exists():
            for line in stderr_path.read_text().splitlines():
                if line.startswith("BROKER_METRICS "):
                    line = line[len("BROKER_METRICS "):]
                try:
                    metrics.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        metadata["broker_metric_records"] = metrics
        (output / "run.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profiles", default="A-basic,B-fanout,C-many-to-many,D-idle-128,D-idle-512,D-idle-1024,D-idle-2000,D-idle-4096,D-idle-10000,E-slow,E-credit-starved,F-overload,F-burst,F-controlled-5000,F-controlled-10000")
    parser.add_argument("--profile-file", type=Path, default=ROOT / "benchmarks/profiles.json")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--backend", choices=("rovqet", "nats"), default="rovqet")
    parser.add_argument("--nats-server", type=Path, default=Path("/tmp/rovqet-nats-server-2.11.10"))
    parser.add_argument("--broker", type=Path, default=ROOT / "target/release/rovqet")
    parser.add_argument("--testkit", type=Path, default=ROOT / "target/release/rovqet-testkit")
    parser.add_argument("--sample-interval", type=float, default=0.1)
    parser.add_argument("--timeout", type=float, default=120)
    return parser.parse_args()


def main():
    args = parse_args()
    profiles = json.loads(args.profile_file.read_text())["profiles"]
    chosen = args.profiles.split(",")
    if args.repetitions < 1 or not 0.01 <= args.sample_interval <= 5 or not 0 < args.timeout <= 600:
        raise SystemExit("invalid repetitions, sample interval or timeout")
    if any(name not in profiles for name in chosen):
        raise SystemExit("unknown profile")
    if args.output.exists():
        raise SystemExit("output directory must be new; existing evidence is never overwritten")
    args.output.mkdir(parents=True)
    manifest = {"schema_version": 1, **git_info(), "environment": environment(),
                "profile_file": str(args.profile_file.resolve()), "backend": args.backend, "runs": [],
                "notes": ["Exploratory local plaintext Q0, not H1/PERF/VAL-01 qualification",
                          "No percentiles are averaged across runs",
                          "Namespace network counters can contain unrelated traffic"]}
    for name in chosen:
        for repeat in range(args.repetitions):
            seed = args.seed + repeat
            run_dir = args.output / f"{name}-r{repeat + 1}"
            result = one_run(name, profiles[name], seed, run_dir,
                             (args.nats_server if args.backend == "nats" else args.broker).resolve(),
                             args.testkit.resolve(), args.sample_interval, args.timeout, args.backend)
            manifest["runs"].append({"directory": run_dir.name, "profile": name, "seed": seed,
                                     "status": result["status"]})
            (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(f"{name} r{repeat + 1}: {result['status']} ({result['wall_seconds']:.2f}s)", flush=True)
    if any(run["status"] == "INVALID" for run in manifest["runs"]):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
