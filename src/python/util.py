import os
import sqlite3
import hashlib
import json
import cPickle as pickle
import functools
import inspect
import numpy as np

def xs(seq): return list(ixs(seq))
def ixs(seq): return (e[0] for e in seq)
def ys(seq): return list(iys(seq))
def iys(seq): return (e[1] for e in seq)

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

