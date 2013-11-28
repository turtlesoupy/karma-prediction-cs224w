import collections
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
from pattern.web import plaintext
from pattern.en import tokenize, sentiment

from util import disk_cache

@disk_cache("pagerank")
def pagerank(graph, cache_dir=None):
    return nx.pagerank(graph, weight=None)

@disk_cache("weighted_pagerank")
def weighted_pagerank(graph, cache_dir=None):
    return nx.pagerank(graph, weight='weight')

UserSentiment = collections.namedtuple("UserSentiment", ["avg_polarity", "std_polarity",
                                                         "avg_subjectivity", "std_subjectivity",
                                                         "sentences"])
default_user_sentiment = UserSentiment(0, 0, 0, 0, 1)
@disk_cache("user_sentiments")
def user_sentiments(user_generator, cache_dir=None):
    ret = {}
    for userkey, text in user_generator:
        if not text or len(text) <= 0:
            continue

        sentences = tokenize(plaintext(text))
        sentiments = [sentiment(s) for s in sentences]
        average_polarity = np.mean([s[0] for s in sentiments])
        std_polarity = np.std([s[0] for s in sentiments])
        average_subjectivity = np.mean([s[1] for s in sentiments])
        std_subjectivity = np.std([s[1] for s in sentiments])
        us = UserSentiment(average_polarity, std_polarity, average_subjectivity, std_subjectivity, len(sentences))
        ret[userkey] = us
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


def scatterplot_matrix(data, names=[], **kwargs):
    """
    Plots a scatterplot matrix of subplots.  Each row of "data" is plotted
    against other rows, resulting in a nrows by nrows grid of subplots with the
    diagonal subplots labeled with "names".  Additional keyword arguments are
    passed on to matplotlib's "plot" command. Returns the matplotlib figure
    object containg the subplot grid.
    """
    numvars, numdata = data.shape
    fig, axes = plt.subplots(nrows=numvars, ncols=numvars, figsize=(8,8))
    fig.subplots_adjust(hspace=0.0, wspace=0.0)

    for ax in axes.flat:
        # Hide all ticks and labels
        ax.xaxis.set_visible(False)
        ax.yaxis.set_visible(False)

        # Set up ticks only on one side for the "edge" subplots...
        if ax.is_first_col():
            ax.yaxis.set_ticks_position('left')
        if ax.is_last_col():
            ax.yaxis.set_ticks_position('right')
        if ax.is_first_row():
            ax.xaxis.set_ticks_position('top')
        if ax.is_last_row():
            ax.xaxis.set_ticks_position('bottom')

    # Plot the data.
    for i, j in zip(*np.triu_indices_from(axes, k=1)):
        for x, y in [(i,j), (j,i)]:
            # FIX #1: this needed to be changed from ...(data[x], data[y],...)
            axes[x,y].plot(data[y], data[x], **kwargs)

    # Label the diagonal subplots...
    if not names:
        names = ['x'+str(i) for i in range(numvars)]

    for i, label in enumerate(names):
        axes[i,i].annotate(label, (0.5, 0.5), xycoords='axes fraction',
                ha='center', va='center')

    # Turn on the proper x or y axes ticks.
    for i, j in zip(range(numvars), itertools.cycle((-1, 0))):
        axes[j,i].xaxis.set_visible(True)
        axes[i,j].yaxis.set_visible(True)

    # FIX #2: if numvars is odd, the bottom right corner plot doesn't have the
    # correct axes limits, so we pull them from other axes
    if numvars%2:
        xlimits = axes[0,-1].get_xlim()
        ylimits = axes[-1,0].get_ylim()
        axes[-1,-1].set_xlim(xlimits)
        axes[-1,-1].set_ylim(ylimits)

    return fig

