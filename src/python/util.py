import os
import sqlite3
import hashlib
import json
import cPickle as pickle
import functools
import inspect
import numpy as np
import nltk
import itertools

def text2tokens(text, min_word_length=4, max_word_length=15):
    if not text:
        return []

    words = [e.lower() for e in itertools.chain.from_iterable(nltk.tokenize.word_tokenize(sentence)
                                                              for sentence in nltk.sent_tokenize(nltk.clean_html(text)))]
    return [token for token in words if len(token) >= min_word_length
                                       and len(token) <= max_word_length
                                       and token.lower() not in STOP_WORDS]


def xs(seq): return list(ixs(seq))
def ixs(seq): return (e[0] for e in seq)
def ys(seq): return list(iys(seq))
def iys(seq): return (e[1] for e in seq)

def kl(p, q):
    """Kullback-Leibler divergence D(P || Q) for discrete distributions

    Parameters
    ----------
    p, q : array-like, dtype=float, shape=n
        Discrete probability distributions.
    """
    return np.sum(np.where(p != 0, p * np.log(p / q), 0))

def kl_uniform(p):
    x, = p.shape
    q = np.asarray([1.0 / x] * x)
    return kl(p, q)

def mkccdf(ys):
    return [1 - e for e in mkcdf(ys)]

def mkcdf(ys):
    if ys == []:
        return []

    y = np.cumsum(ys)
    return [float(e) / y[-1] for e in y]


def mkpdf(ys):
    s = sum(ys)
    return [float(e) / s for e in ys]


# Decorators
def auto_cursor(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        db_path = args[0]
        with sqlite3.connect(db_path) as conn:
            c = conn.cursor()
            return f(c=c, *args[1:], **kwargs)
    return wrapper

def disk_cache(filename_base):
    def decorator(method):
        @functools.wraps(method)
        def wrapper(*args, **kwargs):
            if kwargs.get("cache_dir", None) is None:
                return method(*args, **kwargs)

            cache_dir = kwargs["cache_dir"]
            cache_key = kwargs.get("cache_key", None)
            del kwargs["cache_dir"]
            if cache_key:
                del kwargs["cache_key"]

            arg_hsh = hashlib.md5()
            arg_hsh.update(inspect.getsource(method))
            if cache_key is None:
                arg_hsh.update(json.dumps(args))
                arg_hsh.update(json.dumps(kwargs, sort_keys=True))
                cache_file = os.path.join(cache_dir, "%s-%s.pickle" % (filename_base, arg_hsh.hexdigest()))
            else:
                arg_hsh.update(cache_key)
                cache_file = os.path.join(cache_dir, "%s-%s-%s.pickle" % (filename_base, cache_key, arg_hsh.hexdigest()))

            if os.path.exists(cache_file):
                print "Using cache at %s" % cache_file
                with open(cache_file) as f:
                    return pickle.load(f)

            ret = method(*args, **kwargs)
            with open(cache_file, "wb") as f:
                pickle.dump(ret, f)
            return ret
        return wrapper
    return decorator

STOP_WORDS = set(["n't", "a","a's","able","about","above","according","accordingly","across","actually","after","afterwards","again","against","ain't","all","allow","allows","almost","alone","along","already","also","although","always","am","among","amongst","an","and","another","any","anybody","anyhow","anyone","anything","anyway","anyways","anywhere","apart","appear","appreciate","appropriate","are","aren't","around","as","aside","ask","asking","associated","at","available","away","awfully","b","be","became","because","become","becomes","becoming","been","before","beforehand","behind","being","believe","below","beside","besides","best","better","between","beyond","both","brief","but","by","c","c'mon","c's","came","can","can't","cannot","cant","cause","causes","certain","certainly","changes","clearly","co","com","come","comes","concerning","consequently","consider","considering","contain","containing","contains","corresponding","could","couldn't","course","currently","d","definitely","described","despite","did","didn't","different","do","does","doesn't","doing","don't","done","down","downwards","during","e","each","edu","eg","eight","either","else","elsewhere","enough","entirely","especially","et","etc","even","ever","every","everybody","everyone","everything","everywhere","ex","exactly","example","except","f","far","few","fifth","first","five","followed","following","follows","for","former","formerly","forth","four","from","further","furthermore","g","get","gets","getting","given","gives","go","goes","going","gone","got","gotten","greetings","h","had","hadn't","happens","hardly","has","hasn't","have","haven't","having","he","he's","hello","help","hence","her","here","here's","hereafter","hereby","herein","hereupon","hers","herself","hi","him","himself","his","hither","hopefully","how","howbeit","however","i","i'd","i'll","i'm","i've","ie","if","ignored","immediate","in","inasmuch","inc","indeed","indicate","indicated","indicates","inner","insofar","instead","into","inward","is","isn't","it","it'd","it'll","it's","its","itself","j","just","k","keep","keeps","kept","know","knows","known","l","last","lately","later","latter","latterly","least","less","lest","let","let's","like","liked","likely","little","look","looking","looks","ltd","m","mainly","many","may","maybe","me","mean","meanwhile","merely","might","more","moreover","most","mostly","much","must","my","myself","n","name","namely","nd","near","nearly","necessary","need","needs","neither","never","nevertheless","new","next","nine","no","nobody","non","none","noone","nor","normally","not","nothing","novel","now","nowhere","o","obviously","of","off","often","oh","ok","okay","old","on","once","one","ones","only","onto","or","other","others","otherwise","ought","our","ours","ourselves","out","outside","over","overall","own","p","particular","particularly","per","perhaps","placed","please","plus","possible","presumably","probably","provides","q","que","quite","qv","r","rather","rd","re","really","reasonably","regarding","regardless","regards","relatively","respectively","right","s","said","same","saw","say","saying","says","second","secondly","see","seeing","seem","seemed","seeming","seems","seen","self","selves","sensible","sent","serious","seriously","seven","several","shall","she","should","shouldn't","since","six","so","some","somebody","somehow","someone","something","sometime","sometimes","somewhat","somewhere","soon","sorry","specified","specify","specifying","still","sub","such","sup","sure","t","t's","take","taken","tell","tends","th","than","thank","thanks","thanx","that","that's","thats","the","their","theirs","them","themselves","then","thence","there","there's","thereafter","thereby","therefore","therein","theres","thereupon","these","they","they'd","they'll","they're","they've","think","third","this","thorough","thoroughly","those","though","three","through","throughout","thru","thus","to","together","too","took","toward","towards","tried","tries","truly","try","trying","twice","two","u","un","under","unfortunately","unless","unlikely","until","unto","up","upon","us","use","used","useful","uses","using","usually","uucp","v","value","various","very","via","viz","vs","w","want","wants","was","wasn't","way","we","we'd","we'll","we're","we've","welcome","well","went","were","weren't","what","what's","whatever","when","whence","whenever","where","where's","whereafter","whereas","whereby","wherein","whereupon","wherever","whether","which","while","whither","who","who's","whoever","whole","whom","whose","why","will","willing","wish","with","within","without","won't","wonder","would","would","wouldn't","x","y","yes","yet","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves","z","zero"])
