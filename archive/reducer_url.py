#!/usr/bin/env python3
import sys

current_url = None
total_requests = 0

for line in sys.stdin:
    url, count = line.strip().split("\t")
    count = int(count)
    
    if current_url == url:
        total_requests += count
    else:
        if current_url:
            print(f"{current_url}\t{total_requests}")
        current_url = url
        total_requests = count

# Print the final one
if current_url:
    print(f"{current_url}\t{total_requests}")
