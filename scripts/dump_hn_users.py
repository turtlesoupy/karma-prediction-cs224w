import gzip
import itertools
import json
import os
import requests
import time
import urllib

from dump_hn_discussions import yield_submissions, dump_path


USER_FILE = "/Users/tdimson/personal/cs224w-karma-prediction/data/hn/unprocessed/users.json"
RECRAWL_WAIT = 0.4

def yield_users(users_set):
    headers = {
        'User-Agent': "Cosbynator's karma prediction assignment",
        'From': 'tdimson@cs.stanford.edu',
    }
    users = sorted(users_set)
    for user_chunk in chunk(users, 100):
        nice_chunk = [e for e in user_chunk if e]
        if len(nice_chunk) == 0:
            continue

        url = "http://api.thriftdb.com/api.hnsearch.com/users/_search?q=%s&limit=100" % "+OR+".join(nice_chunk)
        r = requests.get(url, headers=headers)
        j = r.json()
        if "results" not in j:
            continue

        results = j["results"]
        for d in results:
            if "item" in d:
                yield d["item"]
        time.sleep(RECRAWL_WAIT)

def chunk(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

if __name__ == "__main__":
    need_to_crawl_users = set()
    for submission in yield_submissions():
        if "username" in submission:
            need_to_crawl_users.add(submission["username"])

        if submission["num_comments"] <= 0:
            continue

        id = submission["_id"]
        path = dump_path(id)
        if not os.path.exists(path):
            continue

        with gzip.open(path, "r") as f:
            s = f.read()
            if len(s.strip()) == 0:
                print "Need to redump %s" % path
                continue
            items = json.loads(s)

        for item in items:
            need_to_crawl_users.add(item['username'])

        if len(need_to_crawl_users) > 100000:
            break

    if os.path.exists(USER_FILE):
        with open(USER_FILE, "r") as f:
            for line in f:
                uj = json.loads(line)
                if uj["username"] in need_to_crawl_users:
                    need_to_crawl_users.remove(uj["username"])

    print "Crawling %s users" % len(need_to_crawl_users)
    with open(USER_FILE, "a") as f:
        for i, user in enumerate(yield_users(need_to_crawl_users)):
            json.dump(user, f)
            f.write("\n")
            if i % 1000 == 0:
                print "%s/%s" % ((i + 1), len(need_to_crawl_users))
                f.flush()
