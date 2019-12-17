import time

a = 1
for i in range(1,1000000):
    print(i)
    a *= 1.1
    print(a)
    print('-------')
    time.sleep(1)