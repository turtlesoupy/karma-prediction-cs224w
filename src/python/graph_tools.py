import collections
import matplotlib.pyplot as plt
import math
import networkx as nx
import numpy as np
import codecs
from multiprocessing import Pool
from pattern.web import plaintext
from pattern.en import tokenize, sentiment

from util import auto_cursor, disk_cache, STOP_WORDS

from gensim.corpora.dictionary import Dictionary
from gensim.corpora.textcorpus import TextCorpus
from gensim.corpora.mmcorpus import MmCorpus
from gensim.models import LdaModel

#LDA ZONE
class UserCorpus(TextCorpus):
    @classmethod
    def save_corpus(cls, tokens_file, corpus_file, dictionary_path):
        print "Instantiating corpus"
        corpus = UserCorpus(tokens_file)
        print "Filtering extremes"
        corpus.dictionary.filter_extremes(no_below=20, no_above=0.1, keep_n=100000)
        print "Serializing corpus"
        MmCorpus.serialize(corpus_file, corpus, progress_cnt=10000)
        print "Serializing dictionary"
        corpus.dictionary.save_as_text(dictionary_path)

    def __init__(self, tokens_file):
        self.tokens_file = tokens_file
        print "Creating dictionary"
        self.dictionary = Dictionary(self.get_texts())

    def get_texts(self):
        with codecs.open(self.tokens_file, encoding="utf-8", mode="r") as f:
            i = 0
            for line in f:
                tokens = line.strip().split()[1:]
                if i % 1000 == 0:
                    print "Reached text %d" % i
                yield tokens
                i += 1

@disk_cache("lda")
def run_lda(corpus_file, dictionary_path, topics=10):
    id2word = Dictionary.load_from_text(dictionary_path)
    mm = MmCorpus(corpus_file)
    print mm
    lda = LdaModel(corpus=mm, id2word=id2word, num_topics=topics)
    return lda

@disk_cache("user_lda")
def user_lda(lda, dictionary_path, textyielder):
    id2word = Dictionary.load_from_text(dictionary_path)
    ret = {}
    for user, text in text_yielder():
        bow = id2word.doc2bow(UserCorpus.text2tokens(text))
        ret[user] = lda[bow]
    return ret

@disk_cache("lda_rank")
def lda_rank(graph, user_lda):
    ret = {}
    n_topics = len(next(user_lda.itervalues()))

    def pagerank_topic(i):
        personalization_dict = {}
        for user, lda_probs in user_lda.iteritems():
            personalization_dict[user] = lda_probs[i]
        pr = nx.pagerank(graph, personalization_dict=personalization_dict, weight='weight')
        return pr

    p = Pool()
    for pr in p.map(pagerank_topic, range(n_topics)):
        for user, pr in pr.iteritems():
            if user not in ret:
                ret[user] = []
            ret[user].append(pr)
    p.close()
    p.terminate()

    return ret

@disk_cache("pagerank")
def pagerank(graph, cache_dir=None):
    return nx.pagerank(graph, weight=None)

@disk_cache("weighted_pagerank")
def weighted_pagerank(graph, cache_dir=None):
    return nx.pagerank(graph, weight='weight')

@disk_cache("hits")
def hits(graph, cache_dir=None):
    return nx.hits(graph)

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
