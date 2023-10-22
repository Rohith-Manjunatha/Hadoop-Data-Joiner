#!/usr/bin/python

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
            # Secondary sorting based on the "AGE" column (index 2 in dataset 1 and index 5 in dataset 2)
            dataset1_rows.sort(key=lambda x: x.split(",")[2])
            dataset2_rows.sort(key=lambda x: x.split(",")[5])

            for row1 in dataset1_rows:
                for row2 in dataset2_rows:
                    print(f"{row1}\t{row2}")
        current_common_column = common_column
        dataset1_rows = []
        dataset2_rows = []

# Output the final join results with secondary sorting
dataset1_rows.sort(key=lambda x: x.split(",")[2])
dataset2_rows.sort(key=lambda x: x.split(",")[5])
for row1 in dataset1_rows:
    for row2 in dataset2_rows:
        print(f"{row1}\t{row2}")

