#!/usr/bin/env python3
"""Pair exploratory Core/Q0 runs without equating publication acknowledgments."""
import argparse
import json
from pathlib import Path


def read_runs(root):
    manifest = json.loads((root / 'manifest.json').read_text())
    rows = {}
    for entry in manifest['runs']:
        name = entry['directory']
        if Path(name).name != name:
            raise ValueError('unsafe run directory')
        path = root / name / 'run.json'
        run = json.loads(path.read_text())
        key = (run['profile'], run['seed'])
        if key in rows:
            raise ValueError('duplicate profile/seed')
        if run['status'] != entry['status']:
            raise ValueError('manifest/run status mismatch')
        rows[key] = run
    return rows


def project(run):
    result = run.get('generator_result', {})
    oracle = result.get('oracle', {})
    return {'status': run['status'], 'reason': run.get('reason'),
            'git_sha': run.get('git_sha'), 'working_tree_dirty': run.get('working_tree_dirty'),
            'counts': {k: v for k, v in oracle.items() if isinstance(v, (int, bool))},
            'latency': {k: {metric: value for metric, value in v.items() if metric != 'buckets_ns'}
                        for k, v in oracle.items() if 'latency' in k and isinstance(v, dict)},
            'measurement': result.get('measurement'), 'throughput': result.get('throughput'),
            'resources': run.get('resources'),
            'generator_saturation_reasons': run.get('generator_saturation_reasons'),
            'instrumentation': result.get('instrumentation'),
            'semantics': result.get('publication_semantics'),
            'server_version': run.get('server_version')}


def compare(rovqet, nats):
    left, right = read_runs(rovqet), read_runs(nats)
    rows = []
    for key in sorted(left.keys() | right.keys()):
        a, b = left.get(key), right.get(key)
        if a is None or b is None:
            rows.append({'profile': key[0], 'seed': key[1], 'comparison': 'NOT_RUN',
                         'reason': 'paired run absent', 'rovqet': project(a) if a else None,
                         'nats': project(b) if b else None})
            continue
        if a['profile_config'] != b['profile_config']:
            raise ValueError(f'workload configuration mismatch: {key}')
        environment_keys = ('cpu_model', 'kernel', 'machine', 'logical_cpus', 'rlimit_nofile')
        if any(a['environment'].get(k) != b['environment'].get(k) for k in environment_keys):
            raise ValueError(f'host environment mismatch: {key}')
        complete = a['status'] == b['status'] == 'COMPLETE'
        rows.append({'profile': key[0], 'seed': key[1], 'config': a['profile_config'],
                     'comparison': 'EXPLORATORY' if complete else 'INVALID_OR_NOT_RUN',
                     'rovqet': project(a), 'nats': project(b)})
    return {'schema_version': 1, 'semantics': [
        'Rovqet ADMITTED is volatile admission; NATS publish is unconfirmed client enqueue, never admission.',
        'Compare actual application deliveries and same-generator-clock E2E only. Publication latencies are not equivalent.',
        'Core has no Rovqet credits/TTL/egress quotas; slow/overload/burst outcomes are policy observations, not equivalent capacity.',
        'NATS has one subject; Rovqet uses four demo partitions with fixed publisher key. No global order claim.',
        'Plaintext loopback lab, short repetitions; CPU includes setup/drain. No H1/VAL-01 claim.',
        'No average percentiles; exact distributions retained by export_q0.py. Namespace network/socket memory not process-exclusive.'
    ], 'runs': rows}


def offer_analysis(path):
    """Offline instrumentation: logging/oracle overhead cannot alter the trace."""
    offers = []
    with path.open() as source:
        for line in source:
            event = json.loads(line)
            if event['event'] == 'offered' and event['measured']:
                offers.append(event['at_ns'])
    gaps = [b-a for a, b in zip(offers, offers[1:])]
    return {'offers': len(offers), 'elapsed_ns': offers[-1]-offers[0] if len(offers)>1 else None,
            'gaps_over_1ms': sum(g > 1_000_000 for g in gaps),
            'gaps_under_10us': sum(g < 10_000 for g in gaps),
            'max_gap_ns': max(gaps) if gaps else None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rovqet', type=Path, required=True)
    parser.add_argument('--nats', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.rovqet, args.nats)
    with args.output.open('x') as out:
        json.dump(result, out, indent=2)
        out.write('\n')

if __name__ == '__main__':
    main()
