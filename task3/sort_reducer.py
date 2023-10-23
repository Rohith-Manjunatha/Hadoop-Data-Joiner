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
            # Secondary sorting based on the "AGE" column (index 2 in dataset 1 and index 5 in dataset 2)
            dataset1.sort(key=lambda x: x.split(",")[2])
            dataset2.sort(key=lambda x: x.split(",")[5])

            for row1 in dataset1:
                for row2 in dataset2:
                    print(f"{row1}\t{row2}")
        current_common_column = common_column
        dataset1 = []
        dataset2 = []

# Output the final join results with secondary sorting
dataset1.sort(key=lambda x: x.split(",")[2])
dataset2.sort(key=lambda x: x.split(",")[5])
for row1 in dataset1:
    for row2 in dataset2:
        print(f"{row1}\t{row2}")

