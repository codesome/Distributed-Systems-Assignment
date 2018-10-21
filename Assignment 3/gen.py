import random
import sys

n = int(sys.argv[1])
k = 5
alpha = 10
beta = 2

print n, k, alpha, beta

a = range(n)

random.shuffle(a)

done = []
parents = {}

for i in a:
    if len(done) is 0:
        parents[i] = i
        done.append(i)
        continue
    
    secure_random = random.SystemRandom()
    p = secure_random.choice(done)
    parents[i] = p
    done.append(i)
    
for i in range(n):
    print parents[i]