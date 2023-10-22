#!/usr/bin/env python

import sys

current_common_column = None
dataset1_rows = []
dataset2_rows = []

for line in sys.stdin:
    line = line.strip()
    common_column, dataset_identifier, values = line.split("\t", 2)

    if current_common_column == common_column:
        if dataset_identifier == "D1":
            dataset1_rows.append(values)
        else:
            dataset2_rows.append(values)
    else:
        if current_common_column:
            for row1 in dataset1_rows:
                for row2 in dataset2_rows:
                    print(f"{row1}\t{row2}")
        current_common_column = common_column
        dataset1_rows = []
        dataset2_rows = []

# Output the final join results
for row1 in dataset1_rows:
    for row2 in dataset2_rows:
        print(f"{row1}\t{row2}")
