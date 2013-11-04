import sqlite3

def compute_user_karma_distribution(db_path):
    with sqlite3.connect(db_path) as conn:
        c = conn.cursor()
        unnormalized = list(c.execute("SELECT karma, COUNT(1) as amt FROM hn_users GROUP BY karma ORDER BY karma ASC"))
        normalizer = sum(e[1] for e in unnormalized)
        normalized = [(karma, float(amt) / normalizer) for karma, amt in unnormalized]
        return normalized
