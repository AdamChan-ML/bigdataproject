#!/usr/bin/env python3
import sys
from collections import defaultdict

current_product = None
ip_counts = defaultdict(int)

def emit_top_10(product_id, ip_counts):
    sorted_ips = sorted(ip_counts.items(), key=lambda x: (-x[1], x[0]))[:10]
    for ip, count in sorted_ips:
        print(f"{product_id}\t{ip}\t{count}")

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    parts = line.split("\t")
    if len(parts) != 2:
        continue

    product_id, ip = parts

    if current_product is None:
        current_product = product_id

    if product_id != current_product:
        emit_top_10(current_product, ip_counts)
        ip_counts = defaultdict(int)
        current_product = product_id

    ip_counts[ip] += 1

# Final product group
if current_product:
    emit_top_10(current_product, ip_counts)
