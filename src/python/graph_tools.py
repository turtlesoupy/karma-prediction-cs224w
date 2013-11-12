import collections
import networkx as nx
import numpy as np
from util import disk_cache

@disk_cache("pagerank")
def pagerank(graph, cache_dir=None):
    return nx.pagerank(graph, weight=None)

@disk_cache("weighted_pagerank")
def weighted_pagerank(graph, cache_dir=None):
    return nx.pagerank(graph, weight='weight')

@disk_cache("network_stats")
def compute_network_stats(graph):
    num_nodes = graph.number_of_nodes()
    num_edges = graph.number_of_edges()

    strongly_connected_subgraphs = nx.algorithms.components.strongly_connected_component_subgraphs(graph)
    largest_strongly_connected = float(len(strongly_connected_subgraphs[0].number_of_nodes())) / graph.number_of_nodes()
    weakly_connected = nx.algorithms.components.weakly_connected_components(graph)
    largest_weakly_connected = float(len(weakly_connected[0])) / graph.number_of_nodes()

    average_karma = np.mean(x[1].get("karma", x[1].get("reputation", 0)))

    return {
        "Nodes": num_nodes,
        "Edges": num_edges,
        "Largest SCC Fraction": largest_strongly_connected,
        "Largest WCC Fraction": largest_weakly_connected,
        "Average Karma / Reputation": average_karma,
    }

def network_stats_table(*dicts):
    ret = [r"\begin{tabular}{r | %s}" % " ".join("c" for c in dicts)]
    ret.append("& Graph %d" % i for i,e in enumerate(dicts))
    for field in dicts[0].iterkeys():
        ret.append("%s & %s" % (field, " & ".join(d[field] for d in dicts)))
    ret.append(r"\end{tabular}")

    return "\n".join(ret)

