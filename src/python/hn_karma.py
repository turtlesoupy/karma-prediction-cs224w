import os
import sqlite3
import itertools
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
