import random
import sys

N = int(sys.argv[1])
lam = 2
alpha = 2
M = 50

print N, lam, alpha, M

for i in range(N):
    a = []
    for j in range(N):
        if i != j:
            a.append(j)
    random.shuffle(a)
    print str(N-1) + " ",
    for j in a:
        print str(j) + " ",
    print