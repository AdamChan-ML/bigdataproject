import sys

current_code = None
total = 0

for line in sys.stdin:
    code, count = line.strip().split("\t")
    count = int(count)

    if current_code == code:
        total += count
    else:
        if current_code:
            print(f"{current_code}\t{total}")
        current_code = code
        total = count

if current_code:
    print(f"{current_code}\t{total}")
