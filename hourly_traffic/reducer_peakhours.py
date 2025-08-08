import sys
from itertools import groupby
from operator import itemgetter

def read_mapper_output(file):
    for line in file:
        try:
            key, count = line.strip().split("\t")
            yield key, int(count)
        except ValueError:
            continue

def main():
    data = read_mapper_output(sys.stdin)
    for current_key, group in groupby(data, key=itemgetter(0)):
        total = sum(count for _, count in group)
        print(f"{current_key}\t{total}")

if __name__ == "__main__":
    main()
