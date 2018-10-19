import random
import sys

N = int(sys.argv[1])
A = 1000000
T = 10000
lam = 2

print N, A, T, lam

for i in range(N):
    a = [ (i+1)%N ]
    for j in range(N/2):
        r = random.randint(1,100000)%N
        while r == i:
            r = random.randint(1,100000)%N
        a.append(r)

    a = list(set(a))
    random.shuffle(a)
    print str(len(a)) + " ",
    for j in a:
        print str(j) + " ",
    print
    print


for i in range(N):
    if i == 0:
        print 1, 0
    else:
        print 1, i-1