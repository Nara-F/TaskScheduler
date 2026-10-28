"""Checks for workload generation, validation and replay, without running benchmarks."""
import json
from pathlib import Path
import tempfile
import unittest

import run_tests as framework


class WorkloadTests(unittest.TestCase):
    def generate(self, *options):
        args = framework.parser().parse_args(['custom', *options])
        return framework.generate_config(args)

    def test_equal_weights(self):
        config = self.generate('--batches', '3', '--tasks-per-batch', '2', '--weight', '7')
        self.assertEqual(config['weights'], [[7, 7]] * 3)
        self.assertEqual(config['edges'], [[0, 1], [1, 2]])

    def test_random_weights_are_reproducible_and_in_range(self):
        options = ('--weight-range', '5', '20', '--dag', 'random', '--seed', '9')
        config = self.generate(*options)
        self.assertEqual(config, self.generate(*options))
        self.assertTrue(all(5 <= w <= 20 for batch in config['weights'] for w in batch))
        self.assertGreater(len({w for batch in config['weights'] for w in batch}), 1)

    def test_all_topologies(self):
        for dag in ('independent', 'chain', 'fan-in', 'fan-out', 'diamond', 'tree', 'random'):
            with self.subTest(dag=dag):
                config = self.generate('--dag', dag, '--seed', '42')
                positions = {b: i for i, b in enumerate(config['order'])}
                self.assertTrue(all(positions[a] < positions[b] for a, b in config['edges']))

    def test_custom_order_and_duplicate_edges(self):
        config = self.generate('--batches', '3', '--dag', 'custom', '--edges', '2:0,0:1,2:0', '--order', '2,0,1')
        self.assertEqual(config['edges'], [[0, 1], [2, 0]])
        self.assertEqual(config['order'], [2, 0, 1])

    def test_cycle_rejected(self):
        with self.assertRaisesRegex(ValueError, 'cycle'):
            self.generate('--batches', '2', '--dag', 'custom', '--edges', '0:1,1:0')

    def test_order_rejected(self):
        for order in ('1,0,2', '0,1,1', '0,1'):
            with self.subTest(order=order), self.assertRaises(ValueError):
                self.generate('--batches', '3', '--order', order)

    def test_bad_edges_rejected(self):
        for edges in ('0:0', '0:8', '-1:0', '1:2:3'):
            with self.subTest(edges=edges), self.assertRaises(ValueError):
                self.generate('--dag', 'custom', '--edges', edges)

    def test_bad_range_and_batch_sizes_rejected(self):
        for args in (('--weight-range', '10', '1'), ('--tasks-per-batch', '2,3'),
                     ('--edge-probability', 'nan'), ('--dag', 'diamond', '--batches', '3')):
            with self.subTest(args=args), self.assertRaises(ValueError):
                self.generate(*args)

    def test_empty_batches(self):
        config = self.generate('--batches', '3', '--tasks-per-batch', '0,2,0', '--weight', '0')
        self.assertEqual(config['weights'], [[], [0, 0], []])

    def test_replay_and_native_serialization(self):
        config = self.generate('--batches', '3', '--tasks-per-batch', '1,2,0', '--weight', '7',
                               '--dag', 'custom', '--edges', '2:0,0:1', '--order', '2,0,1')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'workload.json'
            path.write_text(json.dumps(config))
            self.assertEqual(config, self.generate('--config', str(path)))
            native = Path(directory) / 'workload.txt'
            framework.write_native(config, native)
            self.assertEqual(native.read_text().splitlines(), ['3', '0 0', '1 1 0 7', '2 1 1 7 7'])

    def test_malformed_config_rejected(self):
        for config in ({}, {'version': 1, 'weights': []},
                       {'version': 1, 'weights': [[-1]]}, {'version': 1, 'weights': [[True]]},
                       {'version': 1, 'weights': [[]], 'edges': [[False, 0]]}):
            with self.subTest(config=config), self.assertRaises(ValueError):
                framework.validate_config(config)

    def test_json_edge_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'edges.json'
            path.write_text('[[2,0],[0,1]]')
            config = self.generate('--dag', 'custom', '--batches', '3', '--edges-file', str(path))
            self.assertEqual(config['order'], [2, 0, 1])


if __name__ == '__main__':
    unittest.main()
