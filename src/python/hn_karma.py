import os
import datetime
import sqlite3
import itertools
import networkx as nx
from util import auto_cursor, disk_cache

@disk_cache("user_karma_distribution")
@auto_cursor
def compute_user_karma_distribution(c):
    return list(c.execute("SELECT karma, COUNT(1) as amt FROM hn_users GROUP BY karma ORDER BY karma ASC"))

@disk_cache("hn_interaction_graph")
@auto_cursor
def compute_interaction_graph(c, cache_dir=None):
    graph = []
    q = """SELECT cs.username AS src_username, cd.username AS dst_username, COUNT(1) AS num_replies
            FROM hn_comments cs
            JOIN hn_comments cd ON (cs.parent_id=cd.id)
            GROUP BY src_username, dst_username"""

    res = sorted(c.execute(q), key=lambda x: x[0])

    for k, g in itertools.groupby(res, key=lambda x: x[0]):
        graph.append((str(k), [(str(dst), num_replies) for src,dst,num_replies in g]))

    return graph

@disk_cache("nx_interaction_graph")
@auto_cursor
def nx_interaction_graph(c):
    q = """SELECT username, karma, join_date FROM hn_users"""
    G = nx.DiGraph()
    print "Adding nodes"
    for username, karma, join_date in c.execute(q):
        ts = datetime.datetime.fromtimestamp(join_date)
        G.add_node(username, karma=karma, join_date=ts)

    print "Finished adding nodes"

    print "Querying db"

    q = """SELECT cs.username AS src_username, cd.username AS dst_username, COUNT(1) AS num_replies
            FROM hn_comments cs
            JOIN hn_comments cd ON (cs.parent_id=cd.id)
            GROUP BY src_username, dst_username"""

    res = sorted(c.execute(q), key=lambda x: x[0])
    print "Finished querying, adding edges"

    i = 0
    for src, g in itertools.groupby(res, key=lambda x: x[0]):
        G.add_weighted_edges_from(list(g))
        if i % 10000 == 0:
            print "Reached edge %d" % i
        i += 1

    print "Finished adding edges"

    return G
