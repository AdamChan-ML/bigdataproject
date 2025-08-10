#!/usr/bin/env python3
import sys
import re

# Match both /product/ID and /m/product/ID patterns
pattern = re.compile(r'GET\s+/(m/)?product/(\d+)')

for line in sys.stdin:
    match = pattern.search(line)
    if match:
        product_id = match.group(2)
        print(f"{product_id}\t1")