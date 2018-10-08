import random
import sys

N = int(sys.argv[1])
A = 1000000
T = 1000
lam = 2

print N, A, T, lam

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
    print


for i in range(N):
    if i == 0:
        print 1, 0
    else:
        print 1, i-1