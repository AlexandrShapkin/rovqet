import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("export_q0", Path(__file__).with_name("export_q0.py"))
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


class ExportTests(unittest.TestCase):
    def fixture(self, root):
        source = root / "source"
        source.mkdir()
        manifest = {"git_sha": "fixture-sha", "runs": [
            {"directory": "A-basic-r1", "status": "COMPLETE"},
            {"directory": "D-idle-10000-r1", "status": "NOT_RUN"}]}
        (source / "manifest.json").write_text(json.dumps(manifest) + "\n")
        complete = source / "A-basic-r1"
        complete.mkdir()
        (complete / "generator").mkdir()
        (complete / "run.json").write_text(json.dumps({"status": "COMPLETE", "generator_result": {
            "oracle": {"ack_latency": {"p99_ns": 200, "buckets_ns": {"100": 2, "200": 1}}}}}) + "\n")
        (complete / "resources.jsonl").write_text('{"time_mono_ns":0,"broker":{"rss_bytes":42}}\n')
        events = b'{"event":"offered","id":1}\n{"event":"ack","id":1}\n'
        (complete / "generator/events.jsonl").write_bytes(events)
        skipped = source / "D-idle-10000-r1"
        skipped.mkdir()
        (skipped / "run.json").write_text(json.dumps({"status": "NOT_RUN", "reason": "insufficient memory"}) + "\n")
        return source, events

    def test_compact_results_keep_statuses_percentiles_and_content_digests(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, events = self.fixture(root)
            output = root / "export"
            result = EXPORTER.export(source, output)
            self.assertEqual([run["status"] for run in result["runs"]], ["COMPLETE", "NOT_RUN"])
            self.assertEqual(result["runs"][1]["reason"], "insufficient memory")
            latency = result["runs"][0]["generator_result"]["oracle"]["ack_latency"]
            self.assertEqual(latency["p99_ns"], 200)
            self.assertEqual(latency["buckets_ns"]["samples"], 3)
            self.assertNotIn("100", latency["buckets_ns"])
            archived = [json.loads(line) for line in gzip.decompress((output / "latency-distributions.jsonl.gz").read_bytes()).splitlines()]
            self.assertEqual(archived[0]["buckets_ns"], {"100": 2, "200": 1})
            self.assertEqual(gzip.decompress((output / "representative-events.jsonl.gz").read_bytes()), events)
            index = json.loads((output / "source-index.json").read_text())
            entry = next(row for row in index["files"] if row["path"].endswith("events.jsonl"))
            self.assertEqual(entry["sha256"], hashlib.sha256(events).hexdigest())
            self.assertEqual(entry["bytes"], len(events))
            self.assertFalse((output / "EXPORT_INCOMPLETE").exists())
            with self.assertRaises(FileExistsError):
                EXPORTER.export(source, output)
            self.assertEqual(json.loads((output / "results.json").read_text())["runs"], result["runs"])

    def test_archives_are_reproducible_and_no_percentile_is_averaged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, _ = self.fixture(root)
            a, b = root / "a", root / "b"
            EXPORTER.export(source, a)
            EXPORTER.export(source, b)
            for name in ("resources.jsonl.gz", "representative-events.jsonl.gz", "latency-distributions.jsonl.gz"):
                self.assertEqual((a / name).read_bytes(), (b / name).read_bytes())

    def test_invalid_or_incomplete_input_is_not_silently_promoted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, _ = self.fixture(root)
            (source / "A-basic-r1/run.json").unlink()
            result = EXPORTER.export(source, root / "export")
            self.assertEqual(result["runs"][0]["status"], "INVALID")
            self.assertIn("unavailable", result["runs"][0]["export_note"])

    def test_truncated_event_evidence_fails_with_incomplete_marker(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, _ = self.fixture(root)
            (source / "A-basic-r1/generator/events.jsonl").write_text('{"event":"offered"}')
            with self.assertRaisesRegex(ValueError, "truncated event"):
                EXPORTER.export(source, root / "export")
            self.assertTrue((root / "export/EXPORT_INCOMPLETE").exists())


if __name__ == "__main__":
    unittest.main()
