import sys
import re

# Regular expression to extract date from web server log format
# We only want the date part: 10/Oct/2000
log_pattern = re.compile(r'\[(\d{2}/[A-Za-z]+/\d{4}):')

# Process each line from input (web server log file)
for line in sys.stdin:
    # Try to find the date pattern in the current log line
    match = log_pattern.search(line)
    if match:
        # Extract the date from the matched group (DD/MMM/YYYY format)
        date = match.group(1)
        # Output key-value pair: date as key, 1 as count value
        # This will be sent to the reducer for aggregation
        print(f"{date}\t1")

        