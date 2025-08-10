#!/usr/bin/env python3
import sys
import re

# IPs to filter out (blacklist)
blacklist_ips = {
    "66.249.66.194",  # example entries – replace with actual IPs
    "91.99.72.15",
    "5.160.157.20"
}

# Regex to match: IP and product ID (mobile or not)
pattern = re.compile(r'^(\d+\.\d+\.\d+\.\d+).+GET\s+/(m/)?product/(\d+)')

for line in sys.stdin:
    match = pattern.search(line)
    if match:
        ip = match.group(1)
        if ip in blacklist_ips:
            continue  # Skip this IP
        product_id = match.group(3)
        print(f"{product_id}\t{ip}")