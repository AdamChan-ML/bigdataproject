import sys

# Initialize variables to track current date and running total
current_date = None
total_requests = 0

# Process each line from mapper output (date\tcount format)
for line in sys.stdin:
    # Parse the input: split by tab to get date and count
    date, count = line.strip().split("\t")
    count = int(count)
    
    # Check if this is the same date we're currently processing
    if current_date == date:
        # Same date: add to the running total
        total_requests += count
    else:
        # New date encountered: output the previous date's total (if exists)
        if current_date:
            print(f"{current_date}\t{total_requests}")
        # Start tracking the new date
        current_date = date
        total_requests = count

# Output the final date's total (since the loop doesn't output the last group)
if current_date:
    print(f"{current_date}\t{total_requests}")

