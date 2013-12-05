import os
import datetime
import sqlite3
import itertools
import networkx as nx
import dateutil.parser
from util import auto_cursor, disk_cache, STOP_WORDS

from gensim.corpora.dictionary import Dictionary
from gensim.corpora.textcorpus import TextCorpus
from gensim.corpora.mmcorpus import MmCorpus

import pattern.en
import pattern.web

class SUUserCorpus(TextCorpus):
    @classmethod
    def save_corpus(cls, db_path, corpus_file, dictionary_path):
        corpus = SUUserCorpus(db_path)
        corpus.dictionary.filter_extremes(no_below=20, no_above=0.1, keep_n=100000)
        MmCorpus.serialize(corpus_file, progress_cnt=10000)
        corpus.dictionary.save_as_text(dictionary_path)

    def __init__(self, db_filename):
        self.db_filename = db_filename
        self.min_word_length = 3
        self.max_word_length = 15
        self.dictionary = Dictionary(self.get_texts())

    def get_texts(self):
        for user, text in yield_user_text(self.db_filename):
            sentences = pattern.en.parse(pattern.web.plaintext(text), tags=False, chunks=False)
            yield [token.lower() for token in itertools.chain.from_iterable(sentences)
                if len(token) >= self.min_word_length and len(token) <= self.max_word_length and token.lower() not in STOP_WORDS]

@auto_cursor
def yield_user_text(c):
    q = """SELECT OwnerUserId, GROUP_CONCAT(Body, ' ') FROM posts"""
    for e in c.execute(q):
        yield e

@disk_cache("su_nx_interaction_graph")
@auto_cursor
def nx_interaction_graph(c, cache_dir=None):
    q = """SELECT DownVotes, DisplayName, Reputation, Views, CreationDate, Id, Upvotes FROM users"""
    G = nx.DiGraph()
    for downvotes, display_name, reputation, views, creation_date_str, id, upvotes in c.execute(q):
        creation_date = dateutil.parser.parse(creation_date_str)
        G.add_node(id, downvotes=downvotes, display_name=display_name, reputation=reputation,
                   views=views, creation_date=creation_date, upvotes=upvotes)

    print "Finished adding nodes"

    print "Querying db"

    q = """SELECT ps.OwnerUserId AS src_user_id,
                  pd.OwnerUserId AS dst_user_id,
                  COUNT(1) AS num_replies
            FROM posts ps
            JOIN posts pd ON (ps.ParentId=pd.Id)
            WHERE
                    ps.ParentId IS NOT NULL
                AND ps.OwnerUserId IS NOT NULL
                AND pd.OwnerUserId IS NOT NULL
            GROUP BY src_user_id, dst_user_id"""

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
