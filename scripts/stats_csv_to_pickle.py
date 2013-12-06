#!/usr/bin/env python

import pickle
import csv

ID_KEY = "#NodeId"
CONSTRAINT_KEY = "NetworkConstraint"

def read_constraint_into_dict(filename):
    constraint_dict = {}
    with open(filename, "r") as f:
        # Skip first two lines
        for i in range(2): f.next()
        csv_reader = csv.DictReader(f, delimiter="\t")
        for row in csv_reader:
            constraint_dict[int(row[ID_KEY])] = float(row[CONSTRAINT_KEY])
    return constraint_dict

def main():
    su_constraint_dict = read_constraint_into_dict("../data/su_stats.txt")
    pickle.dump(su_constraint_dict, open("../data/su_constraint.p", "wb"))

if __name__ == '__main__':
    main()
