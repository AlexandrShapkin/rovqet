"""NATS parity, bounded lifecycle, evidence and saturation guard fixtures."""
import importlib.util
import json
from pathlib import Path
import os
import signal
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("nats_run_q0", Path(__file__).with_name("run_q0.py"))
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


def executable(path, body):
    path.write_text(f"#!{sys.executable}\n" + body)
    path.chmod(0o755)
    return path


class NatsRunnerTests(unittest.TestCase):
    def test_same_profile_preserves_workload_and_seed(self):
        profile = {"publishers": 100, "subscribers": 10, "payload_bytes": 1024,
                   "messages": 1000, "rate": 1000, "warmup_messages": 100,
                   "check_order": False, "broker_max_connections": 1024}
        a = RUNNER.command_for(profile, "127.0.0.1:1", 42, Path("out"), Path("testkit"))
        b = RUNNER.command_for(dict(profile), "127.0.0.1:1", 42, Path("out"), Path("testkit"))
        self.assertEqual(a, b)
        self.assertEqual(a[a.index("--seed") + 1], "42")
        self.assertNotIn("--broker-max-connections", a)

    def test_saturation_distinguishes_overload_burst_and_valid_rate(self):
        result = {"measurement": {"offer_secs": 0.999, "max_offer_lateness_ns": 2_000_000},
                  "oracle": {"offered": 1000, "client_dropped": 0}}
        self.assertEqual(RUNNER.saturation_reasons({"rate": 1000}, result), [])
        result["measurement"]["offer_secs"] = 2
        self.assertIn("below 90%", RUNNER.saturation_reasons({"rate": 1000}, result)[0])
        result["measurement"].update(offer_secs=0.999, max_offer_lateness_ns=200_000_000)
        self.assertIn("lateness", RUNNER.saturation_reasons({"rate": 1000}, result)[0])
        result["oracle"]["client_dropped"] = 1
        self.assertTrue(any("shed" in reason for reason in RUNNER.saturation_reasons({"rate": 1000}, result)))
        self.assertEqual(RUNNER.saturation_reasons({"rate": 0}, result), [])

    def test_pinned_version_and_exact_bounded_configuration(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = executable(root / "server", 'print("nats-server: v2.11.10")\n')
            command, version = RUNNER.nats_command(server, {"broker_max_connections": 2000}, root)
            self.assertEqual(version, "nats-server: v2.11.10")
            self.assertEqual(command, [str(server), "--config", str(root / "nats.conf")])
            config = (root / "nats.conf").read_text()
            self.assertIn('listen: "127.0.0.1:-1"', config)
            self.assertIn("max_pending: 262144", config)
            self.assertIn("max_connections: 2000", config)
            executable(server, 'print("nats-server: v0.0.0")\n')
            with self.assertRaises(ValueError):
                RUNNER.nats_command(server, {}, root)

    def test_startup_shutdown_and_missing_summary_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pid = root / "server.pid"
            server = executable(root / "server", f'''import os,signal,sys,time
if "--version" in sys.argv:
 print("nats-server: v2.11.10");sys.exit(0)
open({str(pid)!r},"w").write(str(os.getpid()))
signal.signal(signal.SIGINT, lambda *args: sys.exit(0))
print("[1] [INF] Listening for client connections on 127.0.0.1:12345",file=sys.stderr,flush=True)
while True: time.sleep(0.1)
''')
            generator = executable(root / "generator", "pass\n")
            result = RUNNER.one_run("fixture", {}, 42, root / "run", server, generator, backend="nats")
            self.assertEqual(result["status"], "INVALID")
            self.assertEqual(result["summary_error"], "missing generator summary")
            self.assertEqual(result["broker_exit"], 0)
            with self.assertRaises(ProcessLookupError):
                os.kill(int(pid.read_text()), 0)
            self.assertIn("--backend", result["generator_command"])
            self.assertTrue((root / "run/resources.jsonl").is_file())

    def test_missing_malformed_and_resource_artifacts_are_invalid(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "generator").mkdir()
            summary = root / "generator/summary.json"
            with self.assertRaisesRegex(ValueError, "missing generator summary"):
                RUNNER.validate_artifacts(root, {})
            summary.write_text('{}')
            with self.assertRaisesRegex(ValueError, "valid=true"):
                RUNNER.validate_artifacts(root, {})
            summary.write_text(json.dumps({"valid": True, "oracle": {}, "config": {}, "measurement": {}}))
            with self.assertRaisesRegex(ValueError, "event evidence"):
                RUNNER.validate_artifacts(root, {})
            (root / "generator/events.jsonl").write_text('{"event":"fixture"}\n')
            with self.assertRaisesRegex(ValueError, "resource evidence"):
                RUNNER.validate_artifacts(root, {})
            resource = root / "resources.jsonl"
            resource.write_text('{"time_mono_ns":0}\n')
            with self.assertRaisesRegex(ValueError, "broker resource samples"):
                RUNNER.validate_artifacts(root, {})
            sample = {"rss_bytes": 100, "cpu_seconds": 0.1, "fd_count": 3}
            resource.write_text(json.dumps({"time_mono_ns": 0, "broker": sample, "generator": sample}) + "\n")
            self.assertTrue(RUNNER.validate_artifacts(root, {})["valid"])
            summary.write_text(json.dumps({"valid": True, "oracle": {}, "config": {}}))
            with self.assertRaisesRegex(ValueError, "measurement object"):
                RUNNER.validate_artifacts(root, {})

    def test_unsupported_credit_operation_records_not_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = RUNNER.one_run("credit", {"credit_starved": 1}, 42,
                                    root / "run", Path("/missing"), Path("/missing"), backend="nats")
            self.assertEqual(result["status"], "NOT_RUN")
            self.assertIn("no Rovqet delivery-credit", result["reason"])
            self.assertEqual(json.loads((root / "run/run.json").read_text())["seed"], 42)


if __name__ == "__main__":
    unittest.main()
