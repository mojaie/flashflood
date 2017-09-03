#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from collections import Counter
import math

import networkx as nx
import pandas as pd
import community

from flashflood.stats import graphgen


def graph_stats(G, part_label="partition"):
    """ Calculate network and clustering properties

    Args:
        G: networkx.Graph

    Returns:
        dict of parameters(clustering coefficient, assortativity
        maximum modularity, isolated node ratio, precision,
        recall, F-measure)
    """
    partition = {n: node[part_label] for n, node in G.nodes_iter(data=True)}
    # network properties
    cc = nx.average_clustering(G)
    tr = nx.transitivity(G)
    apls = 0
    for g in nx.connected_component_subgraphs(G):
        for node in g:
            path_length = nx.single_source_dijkstra_path_length(g, node)
            apls += sum(path_length.values())
    apl = apls / (len(G) * (len(G) - 1))
    if G.number_of_edges() > 3 and nx.density(G) < 1:
        assort = nx.degree_assortativity_coefficient(G)
    else:
        assort = None
    noniso = 1 - (sum(1 for _, d in G.degree_iter() if not d) / len(G))
    if G.number_of_edges():
        modu = community.modularity(partition, G)
    else:
        modu = None
    # Precision, recall and F-measure
    inner_e = 0
    for u, v in G.edges_iter():
        if G.node[u][part_label] == G.node[v][part_label]:
            inner_e += 1
    cluster_cnt = Counter(partition.values())
    inner_pe = sum([v * (v - 1) / 2 for v in cluster_cnt.values()])
    try:
        prec = inner_e / G.number_of_edges()
    except ZeroDivisionError:
        prec = None
    try:
        recall = inner_e / inner_pe
    except ZeroDivisionError:
        recall = None
    if prec is not None and recall is not None:
        fm = 2 * prec * recall / (prec + recall)
    else:
        fm = None
    return {
        "cc": cc,
        "transit": tr,
        "assort": assort,
        "mod": modu,
        "noniso": noniso,
        "prec": prec,
        "recall": recall,
        "fm": fm,
        "apl": apl
    }


def random_graph_stats(size, density, nrand=5, cache=False):
    ress = []
    run = None
    for i in range(nrand):
        if cache:
            run = i + 1
        H = graphgen.random_graph(size, density, run)
        stats = graph_stats(H)
        ress.append(stats)
    dt = pd.DataFrame(ress)
    rnd = {
        "cc": dt["cc"].median(),
        "transit": dt["transit"].median(),
        "assort": dt["assort"].median(),
        "mod": dt["mod"].median(),
        "noniso": dt["noniso"].median(),
        "prec": dt["prec"].median(),
        "recall": dt["recall"].median(),
        "fm": dt["fm"].median(),
        "apl": dt["apl"].median()
    }
    for k, v in rnd.items():  # float("nan") -> None
        if math.isnan(v):
            rnd[k] = None
    return rnd


def entropy(G, label, weight_label=None, cutoff=0):
    """ Returns information entropy of the cluster

    Args:
        G: graph
        records: mapping records [{class: name, members: [list]},]
        weight_label: if specified, entropy contribution is reduced
            by the weight factor.
    """
    records = graphgen.group_records(G, label, cutoff)
    wl = weight_label
    h = 0
    for rcd in records:
        size = len(rcd["members"])
        if not size:
            continue
        p = size / G.number_of_nodes()
        if wl is not None:
            w = sum(G.node[m][wl] for m in rcd["members"]) / size
            plogp = p * math.log2(p) * w
        else:
            plogp = p * math.log2(p)
        h -= plogp
    return h, records


def agreement_stats(G, truth_label, comm_label,
                    truth_weight=None, truth_cutoff=0):
    """ Calculate fuzzy NMI and V-measure

    Args:
        G: graph
        comm: community class label
        truth: ground truth class label
        weight: weight factor label of ground truth class

    Returns:
        results(dict):
            community entropy, truth class entropy,
            mutual information and normalized mutual information(NMI),
            homogeneity, completeness and V-measure
    """
    ec, comm = entropy(G, comm_label)
    et, truth = entropy(G, truth_label, truth_weight, truth_cutoff)
    N = G.number_of_nodes()
    mi = 0  # Mutual information
    etc = 0  # Conditional entropy truth/comm
    ect = 0  # Conditional entropy comm/truth
    for c in comm:
        for t in truth:
            isec = set(c["members"]) & set(t["members"])
            isize = len(isec)
            if not isize:
                continue
            if truth_weight is not None:
                w = sum(G.node[m][truth_weight] for m in isec) / isize
            else:
                w = 1
            p = isize * N / (len(c["members"]) * len(t["members"]))
            plogp = isize / N * math.log2(p)
            mi += plogp * w
            tplogp = isize / N * math.log2(isize / len(c["members"]))
            cplogp = isize / N * math.log2(isize / len(t["members"]))
            etc -= tplogp * w
            ect -= cplogp * w
    nmi = mi / math.sqrt(et * ec)  # Normalized mutual information
    homo = 1 - (etc / et)  # Homogeneity
    comp = 1 - (ect / ec)  # Completeness
    v = 2 * homo * comp / (homo + comp)  # V-measure
    return {
        "h_comm": ec,
        "h_truth": et,
        "h_ct": ect,
        "h_tc": etc,
        "mi": mi,
        "nmi": nmi,
        "homo": homo,
        "comp": comp,
        "vm": v
    }


def agreement_distribution(G, truth_label, comm_label, truth_cutoff=0):
    """ Distribution of mutual information

    Args:
        G: graph
        comm: community class label
        truth: ground truth class label
        truth_cutoff: cutoff of truth class size

    Returns:
        results(dict): {truth class: mutual information}
    """
    ec, comm = entropy(G, comm_label)
    et, truth = entropy(G, truth_label, None, truth_cutoff)
    N = G.number_of_nodes()
    dist = []
    for t in truth:
        mi = 0
        for c in comm:
            isec = set(c["members"]) & set(t["members"])
            isize = len(isec)
            if not isize:
                continue
            p = isize * N / (len(c["members"]) * len(t["members"]))
            plogp = isize / N * math.log2(p)
            mi += plogp
        dist.append({"class": t["class"], "mi": mi})
    return dist


def adjusted_agreement_stats(G, stats, truth_label, comm_label,
                             truth_weight, truth_cutoff,
                             nrand=5, cache=False):
    """ Calculate Adjusted Mutual Information(AMI)

    Args:
        G: graph
        stats: results dict of agreement_stats
        truth_label: node attribute of ground truth class
        comm_label: node attribute of assigned community
        truth_weight: node attribute of weight factor of ground truth class
        truth_cutoff: minimum truth class size
        nrand: number of randam graph samples to generate

    Returns:
        adj(dict): adjusted NMI and adjusted V-measure
        rnd(dict): median of random graph properties
    """
    ress = []
    run = None
    for i in range(nrand):
        if cache:
            run = i + 1
        H = graphgen.random_graph(len(G), stats["density"], run)
        # assign G attributes to H except "partition"
        pt = {n: node["partition"] for n, node in H.nodes_iter(data=True)}
        H.node = G.node
        for n in H.nodes_iter():
            H.node[n]["partition"] = pt[n]
        # stats
        res = graph_stats(H)
        res.update(agreement_stats(
            H, truth_label, comm_label, truth_weight, truth_cutoff))
        ress.append(res)
    dt = pd.DataFrame(ress)
    emi = dt["mi"].median()
    enmi = emi / math.sqrt(stats["h_comm"] * stats["h_truth"])
    ehomo = dt["homo"].median()
    ecomp = dt["comp"].median()
    evm = 2 * ehomo * ecomp / (ehomo + ecomp)
    rnd = {
        "cc": dt["cc"].median(),
        "transit": dt["transit"].median(),
        "assort": dt["assort"].median(),
        "mod": dt["mod"].median(),
        "noniso": dt["noniso"].median(),
        "prec": dt["prec"].median(),
        "recall": dt["recall"].median(),
        "fm": dt["fm"].median(),
        "apl": dt["apl"].median(),
        "homo": ehomo,
        "comp": ecomp,
        "mi": emi,
        "nmi": enmi,
        "vm": evm
    }
    for k, v in rnd.items():  # float("nan") -> None
        if math.isnan(v):
            rnd[k] = None
    # Adjusted mutual information
    try:
        ami = (stats["nmi"] - enmi) / (1 - enmi)
    except ZeroDivisionError:
        ami = None
    # Adjusted V-measure
    try:
        avm = (stats["vm"] - evm) / (1 - evm)
    except ZeroDivisionError:
        avm = None
    adj = {"ami": ami, "avm": avm}
    return adj, rnd


def random_graph_exp_mi(comm, truth, size, weight):
    """Deprecated"""
    N = size
    total = 0
    g = math.lgamma
    for c in comm:
        for t in truth:
            a = len(c["members"])
            b = len(t["members"])
            start = max([0, a + b - N]) + 1
            end = min([a, b]) + 1
            e = 0
            for nij in range(start, end):
                front = nij / N * math.log2(N * nij / a / b)
                above = g(a+1)+g(b+1)+g(N-a+1)+g(N-b+1)
                below = g(N+1)+g(nij+1)+g(a-nij+1)+g(b-nij+1)+g(N-a-b+nij+1)
                exp_weight = sum([weight[n] for n in t["members"]]) / b * nij
                e += front * math.exp(above - below) * exp_weight
            total += e
    return total


def fuzzy_ami_exp(mi, comm, truth, size, weight):
    """Deprecated"""
    exp = random_graph_exp_mi(comm, truth, size, weight)
    sqrth = math.sqrt(mi["h_comm"] * mi["h_truth"])
    ami = (mi["mi"] - exp) / (sqrth - exp)
    return {
        "exp_mi": exp,
        "ami": ami
    }
