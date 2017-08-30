#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import pickle
import unittest
from kiwiii.stats import graphgen


data = {
    "nodes": {"records": [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b"},
        {"id": 3, "value": "c"}
    ]},
    "edges": {"records": [
        {"source": 1, "target": 2, "weight": 0.5},
        {"source": 2, "target": 3, "weight": 0.8},
        {"source": 1, "target": 3, "weight": 1}
    ]}
}


class TestGraphGen(unittest.TestCase):
    def test_graph_loader(self):
        G = graphgen.graph_loader(data, "id")
        self.assertEqual(len(G), 3)
        self.assertEqual(G.node[0]["value"], "a")
        self.assertEqual(G.edge[1][2]["weight"], 0.8)

    def test_threshold_network(self):
        G = graphgen.graph_loader(data, "id")
        H = graphgen.threshold_network(G, 0.6)
        self.assertEqual(G.edge[0][1]["weight"], 0.5)
        self.assertEqual(H.number_of_edges(), 2)

    def test_load_mapping(self):
        G = graphgen.graph_loader(data, "id")
        mapping = {
            "column": {"key": "additional"},
            "key": "id",
            "mapping": {1: "a2", 2: "b2", 3: "c2"}
        }
        graphgen.load_mapping(G, mapping)
        self.assertEqual(G.node[2]["additional"], "c2")
        mapping2 = {
            "column": {"key": "multi"},
            "key": "id",
            "mapping": {1: "a,b,c", 2: "d,e,f", 3: "g,h,i"}
        }
        graphgen.load_multilabel_mapping(G, mapping2, delimiter=",")
        self.assertEqual(G.node[2]["multi"][2], "i")

    def test_group_records(self):
        data2 = pickle.loads(pickle.dumps(data))
        data2["nodes"]["records"].append({"id": 4, "value": "b"})
        data2["edges"]["records"].append(
            {"source": 3, "target": 4, "weight": 1})
        G = graphgen.graph_loader(data2, "id")
        rcds = graphgen.group_records(G, "value")
        self.assertEqual(len(rcds), 3)
        mapping = {
            "column": {"key": "multi"},
            "key": "id",
            "mapping": {1: "a,b,c", 2: "a", 3: "a,b,d,e", 4: "c,f"}
        }
        graphgen.load_multilabel_mapping(G, mapping, delimiter=",")
        rcds = graphgen.group_records(G, "multi")
        self.assertEqual(len(rcds), 6)

    def test_assign_partition(self):
        data2 = pickle.loads(pickle.dumps(data))
        data2["nodes"]["records"].append({"id": 4, "value": "b"})
        G = graphgen.graph_loader(data2, "id")
        graphgen.assign_partition(G)
        self.assertEqual(G.node[3]["partition"], 1)

    def test_assign_weight(self):
        G = graphgen.graph_loader(data, "id")
        mapping = {
            "column": {"key": "multi"},
            "key": "id",
            "mapping": {1: "a,b,c", 2: "a", 3: "a,b,d,e"}
        }
        graphgen.load_multilabel_mapping(G, mapping, delimiter=",")
        graphgen.assign_weight(G, "multi")
        self.assertEqual(G.node[1]["weight"], 1)
        self.assertEqual(G.node[2]["weight"], 0.25)

    def test_random_graph(self):
        G = graphgen.random_graph(8, 0.25)
        self.assertIn("partition", G.node[0])
