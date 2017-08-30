#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import os
import time

import networkx as nx
import community


def graph_loader(datatable, index_label,
                 node_keys=None, edge_keys=("weight",)):
    """ load graph JSON file

    Args:
        datatable: JSON object
        node_keys: list of node keys
        edge_keys: list of edge keys

    Returns:
        networkx.Graph
    """
    G = nx.Graph()
    rev = {}
    for i, node in enumerate(datatable["nodes"]["records"]):
        rev[node[index_label]] = i
        if node_keys is None:
            G.add_node(i, node)
        else:
            G.add_node(i, {k: node[k] for k in node_keys})
    for edge in datatable["edges"]["records"]:
        if edge_keys is None:
            G.add_edge(rev[edge["source"]], rev[edge["target"]], edge)
        else:
            G.add_edge(rev[edge["source"]], rev[edge["target"]],
                       {k: edge[k] for k in edge_keys})
    return G


def threshold_network(G, threshold, weight_label="weight"):
    """ Generate threshold network

    Args:
        G: networkx.Graph
        threshold: network threshold
        weight_label: label of weight attribute

    Returns:
        threshold network
    """
    to_be_removed = []
    for u, v, edge in G.edges_iter(data=True):
        if weight_label in edge and edge[weight_label] < threshold:
            to_be_removed.append((u, v))
    H = G.copy()
    H.remove_edges_from(to_be_removed)
    return H


def load_mapping(G, mapping):
    rev = {node[mapping["key"]]: n for n, node in G.nodes_iter(data=True)}
    for k, v in mapping["mapping"].items():
        if k in rev:
            G.node[rev[k]][mapping["column"]["key"]] = v


def load_multilabel_mapping(G, mapping, delimiter="; "):
    rev = {node[mapping["key"]]: n for n, node in G.nodes_iter(data=True)}
    for k, v in mapping["mapping"].items():
        if k in rev:
            vs = v.split(delimiter)
            G.node[rev[k]][mapping["column"]["key"]] = vs


def mapping(G, key, node_label):
    mapping = {
        "created":  time.strftime("%X %x %Z", time.localtime(time.time())),
        "column": {
            "key": node_label,
            "name": node_label,
            "sort": "numeric",
            "visible": True
        },
        "key": key,
        "mapping": {}
    }
    for _, node in G.nodes_iter(data=True):
        mapping["mapping"][node[key]] = node[node_label]
    return mapping


def group_records(G, class_label, cutoff=0):
    """ Parse node attributes into group records

    Args:
        G: graph
        class_label: class label

    Returns:
        records: list of dict [{class: name, members: list of index,]
    """
    groups = {}
    for n, node in G.nodes_iter(data=True):
        c = node[class_label]
        if isinstance(c, list):
            for e in c:
                if e not in groups:
                    groups[e] = []
                groups[e].append(n)
        else:
            if c not in groups:
                groups[c] = []
            groups[c].append(n)
    rcds = []
    for c, members in groups.items():
        if len(members) >= cutoff:
            rcds.append({"class": c, "members": members})
    return rcds


def assign_partition(G, partition_label="partition"):
    """ Assign community partition to the graph by Louvain method"""
    if G.number_of_edges():
        partition = community.best_partition(G, resolution=1)
        for n in G.nodes_iter():
            G.node[n][partition_label] = partition[n]
    else:
        for i, n in enumerate(G.nodes_iter()):
            G.node[n][partition_label] = i


def assign_weight(G, class_label, weight_label="weight"):
    """ Assign label weight"""
    for n, node in G.nodes_iter(data=True):
        G.node[n][weight_label] = 1 / len(node[class_label])


def random_graph(size, density, cache=None,
                 cache_path="~/.kiwiii/randomGraphs"):
    G = None
    if cache is not None:
        fname = "{:.3e}_{}.json.gz".format(density, cache)
        fpath = os.path.join(cache_path, fname)
        if os.path.isfile(fpath):
            G = nx.read_gpickle(fpath)
    if G is None:
        G = nx.fast_gnp_random_graph(size, density)
        assign_partition(G)
        if cache is not None:
            nx.write_gpickle(G, fpath)
    return G
