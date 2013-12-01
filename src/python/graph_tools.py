import collections
import matplotlib.pyplot as plt
import math
import networkx as nx
import numpy as np
import structural_holes
from igraph import Graph
from igraph.datatypes import UniqueIdGenerator
from pattern.web import plaintext
from pattern.en import tokenize, sentiment

from util import disk_cache

@disk_cache("pagerank")
def pagerank(graph, cache_dir=None):
    return nx.pagerank(graph, weight=None)

@disk_cache("weighted_pagerank")
def weighted_pagerank(graph, cache_dir=None):
    return nx.pagerank(graph, weight='weight')

@disk_cache("hits")
def hits(graph, cache_dir=None):
    return nx.hits(graph)

def convert_igraph(nx_graph):
    ig = Graph(directed=True)
    id_gen = UniqueIdGenerator()
    for node in nx_graph.nodes():
        id = id_gen.add(node)
        ig.add_vertices(id)

    for src_name, dst_name in nx_graph.edges():
        ig.add_edges((id_gen[src_name], id_gen[dst_name]))

    return ig, id_gen

@disk_cache("constraint")
def network_constraint(graph, cache_dir=None):
    return structural_holes.structural_holes(graph)

Sentiment = collections.namedtuple("Sentiment", ["avg_polarity", "std_polarity",
                                                 "avg_subjectivity", "std_subjectivity",
                                                 "sentences"])
default_sentiment = Sentiment(0, 0, 0, 0, 1)
def text_sentiment(text):
    if not text:
        return default_sentiment
    sentences = tokenize(plaintext(text))
    sentiments = [sentiment(s) for s in sentences]
    average_polarity = np.mean([s[0] for s in sentiments])
    std_polarity = np.std([s[0] for s in sentiments])
    average_subjectivity = np.mean([s[1] for s in sentiments])
    std_subjectivity = np.std([s[1] for s in sentiments])

    if math.isnan(average_polarity):
        average_polarity = 0.0
    if math.isnan(std_polarity):
        std_polarity = 0.0
    if math.isnan(average_subjectivity):
        average_subjectivity = 0.0
    if math.isnan(std_subjectivity):
        std_subjectivity = 0.0

    return Sentiment(average_polarity, std_polarity, average_subjectivity, std_subjectivity, len(sentences))

@disk_cache("user_sentiments")
def user_sentiments(user_generator, cache_dir=None):
    ret = {}
    for userkey, text in user_generator:
        if not text or len(text) <= 0:
            continue
        ret[userkey] = text_sentiment(text)

    return ret

@disk_cache("network_stats")
def compute_network_stats(graph):
    num_nodes = graph.number_of_nodes()
    num_edges = graph.number_of_edges()

    strongly_connected = nx.algorithms.components.strongly_connected_components(graph)
    largest_strongly_connected = float(len(strongly_connected[0])) / graph.number_of_nodes()
    weakly_connected = nx.algorithms.components.weakly_connected_components(graph)
    largest_weakly_connected = float(len(weakly_connected[0])) / graph.number_of_nodes()

    average_karma = np.mean([x[1].get("karma", x[1].get("reputation", 0)) for x in graph.nodes(data=True)])

    return {
        "Nodes": num_nodes,
        "Edges": num_edges,
        "Largest SCC Fraction": largest_strongly_connected,
        "Largest WCC Fraction": largest_weakly_connected,
        "Average Karma / Reputation": average_karma,
    }

def neighborhood_karma(graph, node, outbound=True):
    karmas = []
    it = graph.out_edges_iter if outbound else graph.in_edges_iter
    for src, dst in it([node]):
        neighbor = graph.node[dst if outbound else src]
        karmas.append(neighbor.get("karma", neighbor.get("reputation", 0)))
    return karmas

def network_stats_table(*dicts):
    ret = [r"\begin{tabular}{r | %s}" % " ".join("c" for c in dicts)]
    ret.append(" ".join("& Graph %d" % i for i,e in enumerate(dicts)))
    ret.append(r"\hline")
    for field in sorted(dicts[0].iterkeys()):
        ret.append("%s & %s" % (field, " & ".join(str(d[field]) for d in dicts)))
    ret.append(r"\end{tabular}")

    return "\n".join(ret)
