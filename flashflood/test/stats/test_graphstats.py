#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import pickle
import unittest
import math

import networkx as nx

from flashflood.stats import graphgen, graphstats


data = {
    "nodes": {"records": [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b"},
        {"id": 3, "value": "c"},
        {"id": 4, "value": "c"}
    ]},
    "edges": {"records": [
        {"source": 1, "target": 2, "weight": 0.5},
        {"source": 2, "target": 3, "weight": 0.8},
        {"source": 1, "target": 3, "weight": 1},
        {"source": 1, "target": 4, "weight": 1},
    ]}
}


class TestGraphStats(unittest.TestCase):
    def test_graph_stats(self):
        G = graphgen.graph_loader(data, "id")
        graphgen.assign_partition(G)
        stats = graphstats.graph_stats(G)
        self.assertEqual(stats["prec"], 0.5)
        self.assertEqual(stats["recall"], 1.0)
        graphstats.random_graph_stats(8, 0.25, 5)

    def test_graph_stats_no_edge(self):
        data2 = pickle.loads(pickle.dumps(data))
        data2["edges"]["records"].clear()
        G = graphgen.graph_loader(data2, "id")
        graphgen.assign_partition(G)
        stats = graphstats.graph_stats(G)
        self.assertEqual(stats["fm"], None)
        self.assertEqual(stats["noniso"], 0)
        stats = graphstats.random_graph_stats(8, 0, 5)
        self.assertEqual(stats["fm"], None)
        self.assertEqual(stats["noniso"], 0)

    def test_graph_stats_full_edge(self):
        data2 = pickle.loads(pickle.dumps(data))
        data2["edges"]["records"].append(
            {"source": 2, "target": 4, "weight": 0.6})
        data2["edges"]["records"].append(
            {"source": 3, "target": 4, "weight": 1})
        G = graphgen.graph_loader(data2, "id")
        graphgen.assign_partition(G)
        stats = graphstats.graph_stats(G)
        self.assertEqual(stats["cc"], 1)
        self.assertEqual(stats["assort"], None)
        stats = graphstats.random_graph_stats(8, 1, 5)
        # print(stats)
        self.assertEqual(stats["cc"], 1)
        self.assertEqual(stats["assort"], None)

    def test_entropy(self):
        G = graphgen.graph_loader(data, "id")
        h, rcds = graphstats.entropy(G, "value")
        self.assertEqual(h, 1.5)
        mapping = {
            "column": {"key": "multi"},
            "key": "id",
            "mapping": {1: "a,b,c", 2: "a", 3: "a,b,d,e", 4: "c,f"}
        }
        graphgen.load_multilabel_mapping(G, mapping, delimiter=",")
        graphgen.assign_weight(G, "multi")
        h, rcds = graphstats.entropy(G, "multi", "weight")
        self.assertAlmostEqual(h, 1.02, 2)

    @unittest.skip("")
    def test_agreement_stats(self):
        G = graphgen.graph_loader(data, "id")
        mapping = {
            "column": {"key": "reference"},
            "key": "id",
            "mapping": {1: "a", 2: "c", 3: "c", 4: "b"}
        }
        graphgen.load_mapping(G, mapping)
        stats = graphstats.agreement_stats(G, "reference", "value")
        self.assertAlmostEqual(stats["nmi"], 0.67, 2)
        self.assertAlmostEqual(stats["vm"], 0.67, 2)
        mapping = {
            "column": {"key": "multiref"},
            "key": "id",
            "mapping": {1: "a,b,c", 2: "a", 3: "b,c", 4: "c"}
        }
        graphgen.load_multilabel_mapping(G, mapping, delimiter=",")
        graphgen.assign_weight(G, "multiref")
        stats = graphstats.agreement_stats(G, "multiref", "value", "weight")
        self.assertAlmostEqual(stats["nmi"], 0.58, 2)
        self.assertAlmostEqual(stats["vm"], 0.54, 2)
        stats["density"] = nx.density(G)
        adj, rnd = graphstats.adjusted_agreement_stats(
            G, stats, "multiref", "value", "weight")
        self.assertAlmostEqual(adj["ami"], 0, 2)
        self.assertAlmostEqual(adj["avm"], 0, 2)
