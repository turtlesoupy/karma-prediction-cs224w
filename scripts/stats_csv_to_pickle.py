#!/usr/bin/env python

import cPickle as pickle
import csv

ID_KEY = "#NodeId"
CONSTRAINT_KEY = "NetworkConstraint"
CLOSENESS_KEY = "Closeness"
BETWEENNESS_KEY = "Betweennes"

def read_dicts(filename):
    constraint_dict = {}
    closeness_dict = {}
    betweeness_dict = {}
    with open(filename, "r") as f:
        # Skip first two lines
        for i in range(2): f.next()
        csv_reader = csv.DictReader(f, delimiter="\t")
        for row in csv_reader:
            constraint_dict[int(row[ID_KEY])] = float(row[CONSTRAINT_KEY])
            closeness_dict[int(row[ID_KEY])] = float(row[CLOSENESS_KEY])
            betweeness_dict[int(row[ID_KEY])] = float(row[BETWEENNESS_KEY])

    return constraint_dict, closeness_dict, betweeness_dict

def main():
    constraint, closeness, betweeness = read_dicts("su_stats.txt")
    pickle.dump(constraint, open("su_constraint.pickle", "wb"), -1) 
    pickle.dump(closeness, open("su_closeness.pickle", "wb"), -1) 
    pickle.dump(betweeness, open("su_betweenness.pickle", "wb"), -1) 
    
if __name__ == '__main__':
    main()
