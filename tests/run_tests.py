#!/usr/bin/env python3
"""Unified entry point for builtin cases and reproducible custom DAG workloads."""
import argparse
import heapq
import json
import math
from pathlib import Path
import random
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
SCHEDULERS = ('sequential', 'spawn', 'busy-wait', 'blocking')
INT_MAX = 2**31 - 1
UINT64_MAX = 2**64 - 1


def integer(value, minimum=0, maximum=INT_MAX):
    result = int(value)
    if result < minimum or result > maximum:
        raise ValueError(f'integer must be in [{minimum}, {maximum}]: {value}')
    return result


def common(parser):
    parser.add_argument('--binary', type=Path, default=ROOT / 'build/task_scheduler_tests')
    parser.add_argument('--scheduler', choices=(*SCHEDULERS, 'all'), default='blocking')
    parser.add_argument('--threads', '-n', type=lambda v: integer(v, 1), default=4)
    parser.add_argument('--iterations', '-i', type=lambda v: integer(v, 1), default=1)
    parser.add_argument('--timeout', type=float, default=120.0, help='seconds per case/scheduler, including all iterations')


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest='command', required=True)
    listing = commands.add_parser('list', help='list migrated builtin cases')
    listing.add_argument('--binary', type=Path, default=ROOT / 'build/task_scheduler_tests')
    builtin = commands.add_parser('builtin', help='run migrated correctness/performance cases')
    common(builtin)
    builtin.add_argument('--case', nargs='+', default=['all'], help='case names, or all')
    builtin.add_argument('--suite', choices=('all', 'sync', 'async'), default='all')
    idle = commands.add_parser('idle', help='measure burst execution and idle worker resources')
    common(idle)
    idle.add_argument('--idle-ms', type=lambda v: integer(v, 1), default=250,
                      help='idle observation duration per iteration in milliseconds')
    custom = commands.add_parser('custom', help='generate or replay a checked DAG workload')
    common(custom)
    custom.add_argument('--batches', type=lambda v: integer(v, 1), default=8)
    custom.add_argument('--tasks-per-batch', default='16', help='one count, or one comma-separated count per batch (zero allowed)')
    weights = custom.add_mutually_exclusive_group()
    weights.add_argument('--weight', type=lambda v: integer(v, 0, UINT64_MAX), default=None, help='CPU loop iterations per task (default: 10000)')
    weights.add_argument('--weight-range', nargs=2, type=lambda v: integer(v, 0, UINT64_MAX), metavar=('MIN', 'MAX'), help='uniform inclusive integer range per task')
    custom.add_argument('--dag', choices=('independent', 'chain', 'fan-in', 'fan-out', 'diamond', 'tree', 'random', 'custom'), default='chain')
    custom.add_argument('--edge-probability', type=float, default=0.2, help='random DAG edge probability')
    custom.add_argument('--edges', default=None, help='custom DAG edges: source:target,source:target')
    custom.add_argument('--edges-file', type=Path, help='JSON list of [source, target] edges')
    custom.add_argument('--order', default=None, help='explicit topological submission order, e.g. 2,0,1')
    custom.add_argument('--seed', type=lambda v: integer(v, 0, UINT64_MAX), default=0)
    custom.add_argument('--mode', choices=('auto', 'sync', 'async'), default='auto')
    custom.add_argument('--save-config', type=Path, help='save exact weights, edges and order as JSON')
    custom.add_argument('--config', type=Path, help='replay saved workload; generation options are ignored')
    return result


def validate_config(config):
    if not isinstance(config, dict) or config.get('version') != 1:
        raise ValueError('workload config must be an object with version=1')
    weights = config.get('weights')
    if not isinstance(weights, list) or not weights or len(weights) > INT_MAX:
        raise ValueError('weights must contain at least one batch')
    for batch in weights:
        if not isinstance(batch, list) or len(batch) > INT_MAX:
            raise ValueError('each batch must be a list of weights')
        if any(type(w) is not int or w < 0 or w > UINT64_MAX for w in batch):
            raise ValueError('weights must be unsigned 64-bit integers')
    if sum(max(1, len(batch)) for batch in weights) > INT_MAX:
        raise ValueError('total outstanding tasks exceed the scheduler counter range')
    n = len(weights)
    edges = config.get('edges', [])
    if not isinstance(edges, list):
        raise ValueError('edges must be a list')
    unique = set()
    for edge in edges:
        if not isinstance(edge, (list, tuple)) or len(edge) != 2:
            raise ValueError('each edge must be [source, target]')
        if any(type(v) is not int or v < 0 or v >= n for v in edge):
            raise ValueError('edge endpoint outside batch range')
        if edge[0] == edge[1]:
            raise ValueError('self-dependencies are not allowed')
        unique.add(tuple(edge))
    edges = sorted(unique)
    outgoing, indegree = [[] for _ in range(n)], [0] * n
    for source, target in edges:
        outgoing[source].append(target)
        indegree[target] += 1
    ready = [i for i in range(n) if not indegree[i]]
    heapq.heapify(ready)
    automatic = []
    while ready:
        source = heapq.heappop(ready)
        automatic.append(source)
        for target in outgoing[source]:
            indegree[target] -= 1
            if not indegree[target]:
                heapq.heappush(ready, target)
    if len(automatic) != n:
        raise ValueError('dependency graph contains a cycle')
    order = config.get('order', automatic)
    if (not isinstance(order, list) or any(type(v) is not int for v in order)
            or sorted(order) != list(range(n))):
        raise ValueError('order must contain every batch index exactly once')
    positions = {batch: i for i, batch in enumerate(order)}
    if any(positions[a] >= positions[b] for a, b in edges):
        raise ValueError('submission order violates a dependency')
    return {'version': 1, 'weights': weights, 'edges': [list(e) for e in edges], 'order': order}


def generate_config(args):
    if args.config:
        return validate_config(json.loads(args.config.read_text()))
    rng = random.Random(args.seed)
    n = args.batches
    sizes = [integer(v) for v in args.tasks_per_batch.split(',')]
    if len(sizes) == 1:
        sizes *= n
    if len(sizes) != n:
        raise ValueError('--tasks-per-batch must supply one count or exactly --batches counts')
    low, high = args.weight_range or (args.weight if args.weight is not None else 10000,) * 2
    if low > high:
        raise ValueError('weight range MIN must not exceed MAX')
    weights = [[rng.randint(low, high) for _ in range(size)] for size in sizes]
    if not math.isfinite(args.edge_probability) or not 0 <= args.edge_probability <= 1:
        raise ValueError('--edge-probability must be between 0 and 1')
    if args.dag != 'custom' and (args.edges is not None or args.edges_file):
        raise ValueError('--edges and --edges-file require --dag custom')
    if args.edges is not None and args.edges_file:
        raise ValueError('use either --edges or --edges-file')
    edges = []
    if args.dag == 'chain':
        edges = [[i - 1, i] for i in range(1, n)]
    elif args.dag == 'fan-in':
        edges = [[i, n - 1] for i in range(n - 1)]
    elif args.dag == 'fan-out':
        edges = [[0, i] for i in range(1, n)]
    elif args.dag == 'diamond':
        if n < 4:
            raise ValueError('diamond requires at least four batches')
        edges = [[0, i] for i in range(1, n - 1)] + [[i, n - 1] for i in range(1, n - 1)]
    elif args.dag == 'tree':
        edges = [[(i - 1) // 2, i] for i in range(1, n)]
    elif args.dag == 'random':
        topology = list(range(n))
        rng.shuffle(topology)
        edges = [[topology[i], topology[j]] for i in range(n) for j in range(i + 1, n)
                 if rng.random() < args.edge_probability]
    elif args.dag == 'custom':
        if args.edges_file:
            edges = json.loads(args.edges_file.read_text())
        elif args.edges:
            edges = [[int(v) for v in edge.split(':')] for edge in args.edges.split(',')]
    config = {'version': 1, 'weights': weights, 'edges': edges}
    if args.order is not None:
        config['order'] = [int(v) for v in args.order.split(',')]
    return validate_config(config)


def write_native(config, path):
    order = config['order']
    position = {batch: i for i, batch in enumerate(order)}
    predecessors = [[] for _ in order]
    for source, target in config['edges']:
        predecessors[target].append(position[source])
    with path.open('w') as output:
        output.write(f'{len(order)}\n')
        for batch in order:
            deps, weights = predecessors[batch], config['weights'][batch]
            values = [len(weights), len(deps), *sorted(deps), *weights]
            output.write(' '.join(map(str, values)) + '\n')


def run(args):
    binary = args.binary.expanduser().resolve()
    if not binary.is_file():
        raise ValueError(f'runner missing: {binary}; run cmake --build build first')
    if args.command == 'list':
        return subprocess.run([str(binary), '--list']).returncode
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        raise ValueError('--timeout must be positive and finite')
    schedulers = SCHEDULERS if args.scheduler == 'all' else (args.scheduler,)
    failures = 0
    count = 0

    def invoke(mode, argument, scheduler, asynchronous):
        nonlocal failures, count
        count += 1
        command = [str(binary), mode, argument, scheduler, str(args.threads), str(args.iterations),
                   'async' if asynchronous else 'sync']
        try:
            result = subprocess.run(command, timeout=args.timeout)
            failures += result.returncode != 0
        except subprocess.TimeoutExpired:
            print(f'FAIL {argument}: scheduler={scheduler} exceeded {args.timeout}s', file=sys.stderr)
            failures += 1

    if args.command == 'builtin':
        catalog = subprocess.check_output([str(binary), '--list'], text=True)
        cases = dict(line.split('\t') for line in catalog.splitlines())
        if 'all' in args.case and args.case != ['all']:
            raise ValueError('all cannot be combined with individual case names')
        selected = list(cases) if args.case == ['all'] else list(dict.fromkeys(args.case))
        for name in selected:
            if name not in cases:
                raise ValueError(f'unknown case: {name}')
        selected = [name for name in selected if args.suite == 'all' or cases[name] == args.suite]
        if not selected:
            raise ValueError('no cases match the requested suite')
        if args.scheduler not in ('all', 'blocking') and any(cases[name] == 'async' for name in selected):
            raise ValueError('async cases require --scheduler blocking or all; use --suite sync for other schedulers')
        for name in selected:
            for scheduler in schedulers:
                if cases[name] == 'async' and scheduler != 'blocking':
                    print(f'SKIP {name}: {scheduler} has no async implementation', flush=True)
                    continue
                invoke('--builtin', name, scheduler, cases[name] == 'async')
    elif args.command == 'idle':
        for scheduler in schedulers:
            invoke('--idle', str(args.idle_ms), scheduler, False)
    else:
        if args.mode == 'async' and any(s != 'blocking' for s in schedulers):
            raise ValueError('--mode async requires --scheduler blocking')
        config = generate_config(args)
        if args.save_config:
            args.save_config.write_text(json.dumps(config, indent=2) + '\n')
        weights = [w for batch in config['weights'] for w in batch]
        print(f"custom batches={len(config['weights'])} tasks={len(weights)} edges={len(config['edges'])} "
              f"weight_min={min(weights, default=0)} weight_max={max(weights, default=0)} "
              f"total_work={sum(weights)} seed={args.seed if not args.config else 'replay'}", flush=True)
        print('submission_order=' + ','.join(map(str, config['order'])), flush=True)
        with tempfile.TemporaryDirectory(prefix='task_scheduler_') as directory:
            path = Path(directory) / 'workload.txt'
            write_native(config, path)
            for scheduler in schedulers:
                asynchronous = args.mode == 'async' or (args.mode == 'auto' and scheduler == 'blocking')
                invoke('--custom', str(path), scheduler, asynchronous)
    print(f'Completed {count} case/scheduler runs; failures={failures}', flush=True)
    return 1 if failures else 0


def main():
    args = parser().parse_args()
    try:
        return run(args)
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
