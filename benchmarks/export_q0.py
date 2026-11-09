#!/usr/bin/env python3
"""Export compact, content-addressed Q0 evidence without averaging percentiles.

Full original events remain in the ignored source directory. Exported digests are
not a substitute for those events; the retained subset is declared explicitly.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import shutil

SCHEMA_VERSION = 1


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as data:
        for block in iter(lambda: data.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def raw_index(source):
    records = []
    for path in sorted(source.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"source evidence contains a symlink: {path}")
        if path.is_file():
            records.append({"path": path.relative_to(source).as_posix(),
                            "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    return records


def read_indexed_json(source, relative, index):
    path = source / relative
    data = path.read_bytes()
    expected = index.get(relative)
    if expected is None or hashlib.sha256(data).hexdigest() != expected["sha256"]:
        raise ValueError(f"source changed during export: {relative}")
    return json.loads(data)


def run_name(name):
    path = Path(name)
    if not name or path.name != name or name in (".", ".."):
        raise ValueError(f"invalid run directory: {name!r}")
    return name


def extract_histograms(value, directory, path, records):
    if isinstance(value, dict):
        result = {}
        for key, child in value.items():
            if key == "buckets_ns":
                if not isinstance(child, dict) or any(
                    not str(ns).isdigit() or not isinstance(count, int) or isinstance(count, bool) or count < 0
                    for ns, count in child.items()
                ):
                    raise ValueError(f"invalid exact latency histogram at {directory}:{path}")
                record_id = f"{directory}:{path or '<root>'}"
                digest = hashlib.sha256(canonical(child)).hexdigest()
                reference = {"record_id": record_id, "sha256": digest,
                             "distinct_values": len(child), "samples": sum(child.values())}
                records.append({"record_id": record_id, "directory": directory, "path": path,
                                "unit": "nanoseconds", "sha256": digest, "buckets_ns": child})
                result[key] = reference
            else:
                result[key] = extract_histograms(child, directory,
                                                 f"{path}.{key}" if path else key, records)
        return result
    if isinstance(value, list):
        return [extract_histograms(child, directory, f"{path}[{i}]", records)
                for i, child in enumerate(value)]
    return value


def gzip_jsonl(path, rows):
    # Empty original filename and mtime=0 make identical archives byte-for-byte reproducible.
    with path.open("xb") as destination:
        with gzip.GzipFile(filename="", fileobj=destination, mode="wb", mtime=0) as zipped:
            for row in rows:
                zipped.write(canonical(row) + b"\n")


def update_histogram_references(value, retained):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "buckets_ns" and isinstance(child, dict) and "record_id" in child:
                included = child["record_id"] in retained
                child["retained"] = included
                child["archive"] = "latency-distributions.jsonl.gz" if included else None
                if not included:
                    child["raw_source"] = "original run.json and generator/summary.json; indexed in source-index.json"
            else:
                update_histogram_references(child, retained)
    elif isinstance(value, list):
        for child in value:
            update_histogram_references(child, retained)


def resource_rows(source, directories, index):
    for directory in directories:
        relative = f"{directory}/resources.jsonl"
        if relative not in index:
            continue
        path = source / relative
        digest = hashlib.sha256()
        with path.open("rb") as samples:
            for line_number, line in enumerate(samples, 1):
                digest.update(line)
                if not line.endswith(b"\n"):
                    raise ValueError(f"truncated resource JSONL at {relative}:{line_number}")
                try:
                    sample = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"invalid resource JSONL at {relative}:{line_number}") from exc
                yield {"directory": directory, "source_line": line_number, "sample": sample}
        if digest.hexdigest() != index[relative]["sha256"]:
            raise ValueError(f"source changed during export: {relative}")


def copy_event_archive(source, output, directories, index, representative):
    preferred = f"{representative}/generator/events.jsonl"
    candidates = [preferred] + [f"{name}/generator/events.jsonl" for name in directories]
    relative = next((name for name in candidates if name in index), None)
    if relative is None:
        return {"status": "UNAVAILABLE", "reason": "no original events.jsonl exists"}
    destination = output / "representative-events.jsonl.gz"
    digest = hashlib.sha256()
    lines = 0
    with (source / relative).open("rb") as events, destination.open("xb") as target:
        with gzip.GzipFile(filename="", fileobj=target, mode="wb", mtime=0) as zipped:
            for line_number, line in enumerate(events, 1):
                digest.update(line)
                if not line.endswith(b"\n"):
                    raise ValueError(f"truncated event JSONL at {relative}:{line_number}")
                try:
                    json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"invalid event JSONL at {relative}:{line_number}") from exc
                zipped.write(line)
                lines += 1
    if digest.hexdigest() != index[relative]["sha256"]:
        raise ValueError(f"source changed during export: {relative}")
    return {"status": "RETAINED", "source": relative, "source_sha256": digest.hexdigest(),
            "source_bytes": index[relative]["bytes"], "records": lines,
            "archive": destination.name, "archive_bytes": destination.stat().st_size,
            "archive_sha256": sha256_file(destination)}


def export(source, output, max_distribution_bytes=2 * 1024 * 1024,
           representative="A-basic-r1"):
    source = source.resolve()
    output = output.resolve()
    if not source.is_dir():
        raise ValueError("source must be an existing results directory")
    if output == source or source in output.parents:
        raise ValueError("output must be outside the raw source directory")
    if max_distribution_bytes < 1:
        raise ValueError("distribution size limit must be positive")
    records = raw_index(source)
    index = {record["path"]: record for record in records}
    manifest = read_indexed_json(source, "manifest.json", index)
    runs = manifest.get("runs")
    if not isinstance(runs, list):
        raise ValueError("manifest must contain a runs list")
    names = [run_name(row["directory"]) for row in runs]
    if len(set(names)) != len(names):
        raise ValueError("manifest contains duplicate run directories")
    compact_runs = []
    histograms = []
    for row, name in zip(runs, names):
        relative = f"{name}/run.json"
        if relative in index:
            run = read_indexed_json(source, relative, index)
            compact = extract_histograms(run, name, "", histograms)
            compact["directory"] = name
            compact["manifest_status"] = row.get("status", "UNAVAILABLE")
        else:
            compact = {"directory": name, "status": "INVALID",
                       "manifest_status": row.get("status", "UNAVAILABLE"),
                       "export_note": "run.json unavailable; no result inferred"}
        compact_runs.append(compact)
    output.mkdir(parents=True, exist_ok=False)
    marker = output / "EXPORT_INCOMPLETE"
    marker.write_text("An export failure leaves this marker; do not treat partial files as complete evidence.\n")
    distribution = output / "latency-distributions.jsonl.gz"
    gzip_jsonl(distribution, histograms)
    retention_mode = "ALL_EXACT_DISTRIBUTIONS"
    selected = histograms
    if distribution.stat().st_size > max_distribution_bytes:
        retention_mode = "REPRESENTATIVE_EXACT_DISTRIBUTIONS"
        selected = [row for row in histograms if row["directory"] in {representative, "B-fanout-r1"}]
        distribution.unlink()
        gzip_jsonl(distribution, selected)
    retained = {row["record_id"] for row in selected}
    update_histogram_references(compact_runs, retained)
    gzip_jsonl(output / "resources.jsonl.gz", resource_rows(source, names, index))
    event_archive = copy_event_archive(source, output, names, index, representative)
    result = {
        "schema_version": SCHEMA_VERSION,
        "source_directory": str(source),
        "source_manifest_sha256": index["manifest.json"]["sha256"],
        "manifest": manifest,
        "runs": compact_runs,
        "retention": {
            "latency_mode": retention_mode,
            "latency_archive": distribution.name,
            "latency_archive_bytes": distribution.stat().st_size,
            "latency_records_total": len(histograms), "latency_records_retained": len(selected),
            "latency_archive_sha256": sha256_file(distribution),
            "representative_events": event_archive,
            "resources_archive": "resources.jsonl.gz",
            "resources_archive_sha256": sha256_file(output / "resources.jsonl.gz"),
            "full_raw_events_committed": False,
            "notes": ["Every original raw file is hashed and sized in source-index.json.",
                      "Full raw events remain locally in the ignored source directory; hashes cannot reconstruct omitted events.",
                      "Run status, invalid/NOT_RUN reasons and all existing percentile values are preserved; no percentile is averaged.",
                      "Exact nanosecond/count distributions omitted by size selection remain in the indexed raw files."]},
    }
    (output / "source-index.json").write_text(json.dumps({"schema_version": SCHEMA_VERSION,
        "source_directory": str(source), "file_count": len(records),
        "total_source_bytes": sum(row["bytes"] for row in records), "files": records}, indent=2) + "\n")
    (output / "results.json").write_text(json.dumps(result, indent=2) + "\n")
    marker.unlink()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-distribution-bytes", type=int, default=2 * 1024 * 1024)
    parser.add_argument("--representative", default="A-basic-r1")
    args = parser.parse_args()
    result = export(args.source, args.output, args.max_distribution_bytes, args.representative)
    print(json.dumps({"output": str(args.output), "runs": len(result["runs"]),
                      "latency_records_retained": result["retention"]["latency_records_retained"],
                      "raw_events_committed": False}))


if __name__ == "__main__":
    main()
