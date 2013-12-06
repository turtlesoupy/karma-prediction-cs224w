import sys
sys.path.append("../src/python")

import hn_karma
import su_reputation
import networkx as nx
import os

HN_DB_FILE = "../data/hn.db"
SU_DB_FILE = "../data/superuser.db"
CACHE_DIR = "../data/cache"
OUTPUT_DIR = "../data"

hn_interaction_graph = hn_karma.nx_interaction_graph(HN_DB_FILE, cache_dir=CACHE_DIR)
hn_interaction_graph_relabelled = nx.convert_node_labels_to_integers(hn_interaction_graph)
nx.write_adjlist(hn_interaction_graph_relabelled, os.path.join(OUTPUT_DIR, "hn.txt"))
su_interaction_graph = su_reputation.nx_interaction_graph(SU_DB_FILE, cache_dir=CACHE_DIR)
nx.write_adjlist(su_interaction_graph, os.path.join(OUTPUT_DIR, "su.txt"))
