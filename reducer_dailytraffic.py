#!/usr/bin/env python3
import sys

current_date = None
total_requests = 0

for line in sys.stdin:
    date, count = line.strip().split("\t")
    count = int(count)
    
    if current_date == date:
        total_requests += count
    else:
        if current_date:
            print(f"{current_date}\t{total_requests}")
        current_date = date
        total_requests = count

# Output last date
if current_date:
    print(f"{current_date}\t{total_requests}")
