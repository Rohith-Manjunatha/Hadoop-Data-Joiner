#!/usr/bin/python


import sys

for line in sys.stdin:
    line = line.strip()
    columns = line.split(",")

    # Use columns[1] and columns[4] as the common column values
    common_column1 = columns[1]
    common_column2 = columns[4]

    # Emit key-value pairs with dataset identifiers
    dataset_identifier1 = "D1"
    dataset_identifier2 = "D2"
    print(f"{common_column1}\t{dataset_identifier1}\t{line}")
    print(f"{common_column2}\t{dataset_identifier2}\t{line}")

