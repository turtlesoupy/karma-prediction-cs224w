import os
import datetime
import sqlite3
import itertools
import networkx as nx
import numpy as np
import nltk
import codecs
from util import auto_cursor, disk_cache, text2tokens, STOP_WORDS

@disk_cache("user_karma_distribution")
@auto_cursor
def compute_user_karma_distribution(c):
    return list(c.execute("SELECT karma, COUNT(1) as amt FROM hn_users GROUP BY karma ORDER BY karma ASC"))

@auto_cursor
def yield_user_text(c):
    q = """SELECT username, GROUP_CONCAT(text, " ") FROM hn_comments GROUP BY username"""
    for user, text in c.execute(q):
        yield (user, text or "BLANK")

def write_user_text_tokens(db_path, output_file):
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        q = """SELECT username, GROUP_CONCAT(text, " ") FROM hn_comments GROUP BY username"""
        with codecs.open(output_file, encoding='utf-8', mode="w") as f:
            i = 0
            for user, text in c.execute(q):
                f.write("%s\t%s\n" % (user, "\t".join(text2tokens(text))))
                if i % 1000 == 0:
                    print "Reached user %d" % i
                i += 1

@disk_cache("nx_interaction_graph")
@auto_cursor
def nx_interaction_graph(c):
    q = """SELECT username, karma, join_date FROM hn_users"""
    G = nx.DiGraph()
    print "Adding nodes"
    for username, karma, join_date in c.execute(q):
        ts = datetime.datetime.fromtimestamp(join_date)
        G.add_node(username, karma=max(karma, 0), join_date=ts)

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
            print "Reached edges for node %d" % i
        i += 1

    print "Finished adding edges"

    return G
