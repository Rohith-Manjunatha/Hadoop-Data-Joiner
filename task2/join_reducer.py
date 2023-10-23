#!/usr/bin/python

import sys

current_common_column = None
dataset1 = []
dataset2 = []

for line in sys.stdin:
    line = line.strip()
    common_column, dataset_identifier, values = line.split("\t", 2)

    if current_common_column == common_column:
        if dataset_identifier == "D1":
            dataset1.append(values)
        else:
            dataset2.append(values)
    else:
        if current_common_column:
            for row1 in dataset1:
                for row2 in dataset2:
                    print(f"{row1}\t{row2}")
        current_common_column = common_column
        dataset1 = []
        dataset2 = []

# Output the final join results
for row1 in dataset1:
    for row2 in dataset2:
        print(f"{row1}\t{row2}")
