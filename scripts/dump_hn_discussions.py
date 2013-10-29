import gzip
import json
import os
import requests
import time
import urllib

SUBMISSION_FILE = "/Users/tdimson/personal/cs224w-karma-prediction/data/hn/unprocessed/submissions.json"
DUMP_PATH_BASE = "/Users/tdimson/personal/cs224w-karma-prediction/data/hn/unprocessed/discussions/"
RECRAWL_WAIT = 0.4

def dump_path(discussion_id):
    s = str(discussion_id)
    return os.path.join(DUMP_PATH_BASE, s[-2:], s[-4:-2], "%s.json.gz" % s)

def yield_submissions():
    with open(SUBMISSION_FILE) as f:
        for json_string in f:
            yield json.loads(json_string)

def fetch_discussion(discussion_id):
    headers = {
        'User-Agent': "Cosbynator's karma prediction assignment",
        'From': 'tdimson@cs.stanford.edu',
    }


    items = []

    limit = 100
    offset = 0
    iterations = 0
    while offset <= 900 and iterations < 15:
        url = "http://api.thriftdb.com/api.hnsearch.com/items/_search?limit=%s&filter[fields][discussion.sigid]=%s&start=%s" % (limit, discussion_id, offset)

        r = requests.get(url, headers=headers)
        j = r.json()
        if "results" not in j:
            break

        results = j["results"]
        items.extend([i["item"] for i in results if "item" in i])

        if len(results) < 100:
            break

        iterations += 1
        offset += 100
        time.sleep(RECRAWL_WAIT)
        print "\tFetching more from %s: %s" % (discussion_id, offset)
    return items



if __name__ == "__main__":
    for i, discussion in enumerate(yield_submissions()):
        num_comments = discussion['num_comments']
        id = discussion['_id']
        if num_comments <= 0:
            continue
        path = dump_path(id)

        if os.path.exists(path):
            continue

        d = os.path.dirname(path)
        if not os.path.exists(d):
            os.makedirs(d)

        items = fetch_discussion(id)
        with gzip.open(path, "w", 7) as f:
            json.dump(items, f)

        print "(%d) Wrote %s" % (i, path)
        time.sleep(RECRAWL_WAIT)
