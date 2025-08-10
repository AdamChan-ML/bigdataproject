#!/usr/bin/env python3
import sys

current_product = None
count = 0

for line in sys.stdin:
    product_id, value = line.strip().split('\t')
    value = int(value)

    if product_id == current_product:
        count += value
    else:
        if current_product:
            print(f"{current_product}\t{count}")
        current_product = product_id
        count = value

# Output the last product
if current_product:
    print(f"{current_product}\t{count}")