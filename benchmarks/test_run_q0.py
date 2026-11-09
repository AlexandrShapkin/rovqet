"""Focused resource parsing, evidence integrity, and process cleanup tests."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("run_q0", Path(__file__).with_name("run_q0.py"))
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class SamplingTests(unittest.TestCase):
    def test_proc_stat_parses_parenthesized_process_name(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = root / "42"
            process.mkdir()
            (process / "fd").mkdir()
            (process / "fd/0").touch()
            fields = ["0"] * 22
            fields[0] = "S"
            fields[11] = "50"
            fields[12] = "25"
            fields[21] = "10"
            (process / "stat").write_text("42 (a (nested) name) " + " ".join(fields))
            sample = RUNNER.process_sample(42, root)
            self.assertEqual(sample["rss_bytes"], 10 * RUNNER.PAGE)
            self.assertEqual(sample["cpu_seconds"], 75 / RUNNER.HZ)
            self.assertEqual(sample["fd_count"], 1)
            self.assertEqual(RUNNER.process_sample(99, root)["status"], "UNAVAILABLE")

    def test_network_sampling_labels_namespace_and_memory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "net").mkdir()
            (root / "net/dev").write_text("lo: 100 1 0 0 0 0 0 0 200 1 0 0 0 0 0 0\n")
            (root / "net/sockstat").write_text("TCP: inuse 3 orphan 0 tw 2 alloc 5 mem 7\n")
            sample = RUNNER.network_sample(root)
            self.assertEqual(sample["rx_bytes"], 100)
            self.assertEqual(sample["tx_bytes"], 200)
            self.assertEqual(sample["tcp_socket_memory_bytes"], 7 * RUNNER.PAGE)
            self.assertIn("host activity", sample["scope"])

    def test_cpu_and_resource_summary_retains_units(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "samples.jsonl"
            data = []
            for n in range(3):
                data.append({"time_mono_ns": n * 1_000_000_000,
                             "broker": {"rss_bytes": 100 + n, "cpu_seconds": n / 2, "fd_count": n + 1},
                             "network": {"rx_bytes": n * 10, "tx_bytes": n * 20, "tcp_socket_memory_bytes": n * 100}})
            path.write_text("".join(json.dumps(row) + "\n" for row in data))
            result = RUNNER.summarize_resources(path)
            self.assertEqual(result["broker"]["cpu_percent_mean"], 50)
            self.assertEqual(result["broker"]["rss_max_bytes"], 102)
            self.assertEqual(result["network"]["rx_bytes_delta"], 20)
            self.assertEqual(result["generator"]["status"], "UNAVAILABLE")

    def test_high_density_requires_measured_memory_headroom(self):
        env = {"linux": {"memory": "MemAvailable: 1000000 kB", "memory_limit": "UNAVAILABLE", "memory_current": "UNAVAILABLE"}}
        budget, reason = RUNNER.connection_budget({"broker_max_connections": 10000}, env)
        self.assertIsNone(budget)
        self.assertIn("exceeds", reason)
        env["linux"]["memory"] = "MemAvailable: 40000000 kB"
        budget, reason = RUNNER.connection_budget({"broker_max_connections": 2000}, env)
        self.assertEqual(budget, 2000 * 1380352)
        self.assertIsNone(reason)
        env["linux"].update(memory_limit="1000000000", memory_current="900000000")
        self.assertIsNotNone(RUNNER.connection_budget({"broker_max_connections": 2000}, env)[1])

    def test_invalid_binary_records_incomplete_evidence_without_leak(self):
        with tempfile.TemporaryDirectory() as directory:
            result = RUNNER.one_run("bad", {}, 1, Path(directory) / "run", Path("/missing/broker"), Path("/missing/testkit"))
            self.assertEqual(result["status"], "INVALID")
            saved = json.loads((Path(directory) / "run/run.json").read_text())
            self.assertIn("error", saved)

    def test_profile_command_preserves_boolean_and_excludes_broker_config(self):
        command = RUNNER.command_for({"messages": 3, "check_order": False, "broker_max_connections": 2000}, "127.0.0.1:1", 9, Path("out"), Path("testkit"))
        self.assertIn("--check-order", command)
        self.assertIn("false", command)
        self.assertNotIn("--broker-max-connections", command)


if __name__ == "__main__":
    unittest.main()
