#!/usr/bin/env python3
import sys
import re

# Regex to extract full timestamp from log line (e.g., [10/Oct/2000:13:55:36 -0700])
log_pattern = re.compile(r'\[(\d{2}/[A-Za-z]{3}/\d{4}):(\d{2}):\d{2}:\d{2}')

def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        match = log_pattern.search(line)
        if match:
            hour = match.group(2)  # Group 2 = hour (00-23)
            print(f"{hour}\t1")

if __name__ == "__main__":
    main()
