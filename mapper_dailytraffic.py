#!/usr/bin/env python3
import sys
import re

# Example line: 127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] ...
log_pattern = re.compile(r'\[(\d{2}/[A-Za-z]+/\d{4}):')

for line in sys.stdin:
    match = log_pattern.search(line)
    if match:
        date = match.group(1)
        print(f"{date}\t1")