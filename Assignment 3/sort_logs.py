import sys

with open(sys.argv[1]) as f:
    lines = f.readlines()
    for line in sorted(lines, key=lambda line: line.split()[0]):
        print line,