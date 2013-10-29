import json
import requests
import time
import urllib

SUBMISSION_FILE = "/Users/tdimson/personal/cs224w-karma-prediction/data/hn/unprocessed/submissions.json"
RECRAWL_WAIT = 0.75

def submission_crawl_url(end_ts):
    if end_ts:
        q = "{* TO %s}" % end_ts
        return "http://api.thriftdb.com/api.hnsearch.com/items/_search?filter[fields][type]=submission&sortby=create_ts%%20desc&filter[fields][create_ts]=%s&limit=100" % urllib.quote_plus(q)
    else:
        return "http://api.thriftdb.com/api.hnsearch.com/items/_search?filter[fields][type]=submission&sortby=create_ts%20desc&limit=100"


last_ts = None
num_crawled = 0

headers = {
    'User-Agent': "Cosbynator's karma prediction assignment",
    'From': 'tdimson@cs.stanford.edu',
}

while True:
    with open(SUBMISSION_FILE, 'a') as f:
        r = requests.get(submission_crawl_url(last_ts), headers=headers)
        j = r.json()
        if "results" in j:
            results = j["results"]
            for result in results:
                if "item" in result and "create_ts" in result["item"]:
                    last_ts = result["item"]["create_ts"]
                    json.dump(result["item"], f)
                    f.write("\n")
                num_crawled += 1
            f.flush()
            print "Crawled %s items" % num_crawled
    time.sleep(RECRAWL_WAIT)
