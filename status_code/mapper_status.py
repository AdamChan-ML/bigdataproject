#!/usr/bin/env python3
import sys
import re

# Regex to extract the status code
# Matches: "GET /path HTTP/1.1" 200
status_code_pattern = re.compile(r'"\w+\s[^\s]+\sHTTP/\d\.\d"\s(\d{3})')

def main():
    for line in sys.stdin:
        match = status_code_pattern.search(line)
        if match:
            status_code = match.group(1)
            print(f"{status_code}\t1")

if __name__ == "__main__":
    main()
