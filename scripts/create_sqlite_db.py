import calendar
import dateutil.parser
import gzip
import itertools
import json
import os
import sqlite3
import sys

from dump_hn_discussions import yield_submissions, dump_path

def iso2epoch(iso):
    return calendar.timegm(dateutil.parser.parse(iso).timetuple())

def create_tables(conn):
    c = conn.cursor()

    print "Creating tables..."
    c.execute("""
    CREATE TABLE hn_submissions
              (id STRING PRIMARY KEY, submitter STRING NOT NULL, hn_id INTEGER NOT NULL,
              title STRING NOT NULL, url STRING, text STRING,
              num_comments INTEGER NOT NULL, points INTEGER NOT NULL, create_date INTEGER NOT NULL)
    """)

    c.execute("""
    CREATE TABLE hn_users
              (username STRING PRIMARY KEY, karma INTEGER NOT NULL, about STRING, join_date INTEGER NOT NULL)
    """)

    c.execute("""
    CREATE TABLE hn_comments
              (id STRING PRIMARY KEY, hn_submission_id STRING, parent_id STRING NOT NULL, username STRING NOT NULL,
               hn_id INTEGER NOT NULL,
               points INTEGER NOT NULL, text STRING NOT NULL, create_date INTEGER NOT NULL)
    """)

    c.execute("""
    CREATE INDEX hn_comments_submission_idx ON hn_comments (hn_submission_id, parent_id)
    """)

    conn.commit()


def chunk(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

def iunique(iterable, key=None):
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in ifilterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element

def populate_users(conn, users_file="/Users/tdimson/personal/cs224w-karma-prediction/data/hn/unprocessed/users.json"):
    def line2insert(line):
        o = json.loads(line)
        return (
            o["username"], o["karma"], o["about"], iso2epoch(o["create_ts"])
        )

    print "Inserting users"
    c = conn.cursor()
    c.execute("PRAGMA synchronous=OFF")
    with open(users_file) as f:
        insert_tuples = iunique((line2insert(e) for e in f), key=lambda x:x[0])
        chunks = chunk(insert_tuples, 10000)
        clean = ([e for e in chunk if e] for chunk in chunks)
        for i, bulk in enumerate(clean):
            c.executemany("INSERT INTO hn_users(username, karma, about, join_date) VALUES (?,?,?,?)", bulk)
            conn.commit()
            sys.stdout.write("\r%d" % (i * 10000))
            sys.stdout.flush()
    print "Inserted"

def populate_submissions(conn):
    def submission2insert(o):
        return (
            o["_id"], o["username"], o["id"], o["title"], o["url"], o["text"], o["num_comments"], o["points"], iso2epoch(o["create_ts"])
        )

    print "Inserting submissions"
    c = conn.cursor()
    c.execute("PRAGMA synchronous=OFF")
    insert_tuples = iunique((submission2insert(e) for e in yield_submissions()), key=lambda x:x[0])
    chunks = chunk(insert_tuples, 10000)
    clean = ([e for e in chunk if e] for chunk in chunks)
    for i, bulk in enumerate(clean):
        c.executemany("""INSERT INTO hn_submissions(id, submitter, hn_id, title, url, text, num_comments, points, create_date)
                       VALUES (?,?,?,?,?,?,?,?,?)""", bulk)
        conn.commit()
        sys.stdout.write("\r%d" % (i * 10000))
        sys.stdout.flush()
    print "Done"

def populate_comments(conn):
    def comment2insert(o):
        return (
            o["_id"], o["discussion"]["sigid"], o["parent_sigid"], o["username"], o["id"], o["points"], o["text"], iso2epoch(o["create_ts"]), o["text"]
        )

    print "Inserting comments"
    c = conn.cursor()
    c.execute("PRAGMA synchronous=OFF")
    num_c = 0
    for i, submission in enumerate(yield_submissions()):
        path = dump_path(submission["_id"])
        if not os.path.exists(path):
            continue

        with gzip.open(path, "r") as f:
            comments = json.loads(f.read())

        insert_comments = [comment2insert(com) for com in comments]
        c.executemany("""INSERT INTO hn_comments(id, hn_submission_id, parent_id, username, hn_id, points, text, create_date, text)
                       VALUES (?,?,?,?,?,?,?,?,?)""", insert_comments)
        conn.commit()
        num_c += len(insert_comments)

        if i % 1000 == 0:
            sys.stdout.write("\r%d" % num_c)
            sys.stdout.flush()

    print "Inserted"





def main():
    if len(sys.argv) <= 1:
        raise Exception("Specify database file to output")

    db_file = sys.argv[1]

    if os.path.exists(db_file):
        raise Exception("%s already exists" % db_file)

    with sqlite3.connect(db_file) as conn:
        create_tables(conn)
        populate_users(conn)
        populate_submissions(conn)
        populate_comments(conn)

if __name__ == "__main__":
    main()

