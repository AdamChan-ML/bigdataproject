#!/usr/bin/env python3
import sys
import re

# Regex to extract the URL from the request string
# Example match: "GET /index.html HTTP/1.0"
request_pattern = re.compile(r'"(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH)\s+([^\s]+)\s+HTTP/\d\.\d"')

def main():
    for line in sys.stdin:
        line = line.strip()
        match = request_pattern.search(line)
        if match:
            url = match.group(2)
            print(f"{url}\t1")

if __name__ == "__main__":
    main()
