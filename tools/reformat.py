import sys, os

fr = open(sys.argv[1], 'r')
TMPNUM = 31
tmpw = [open(f'tmp{i}', 'w') for i in range(TMPNUM)]

print('***** Phase 1')

cnt = 0
for line in fr :
    cnt += 1
    if (cnt%10000000 == 0) :
        print(cnt)
    if line[0]=='%' :
        continue
    l = line.split()
    if len(l)<2 :
        continue
    x, y = int(l[0]), int(l[1])
    if (x==y) :
        continue
    if x>y :
        x, y = y, x
    h = ((x*998244353)^(y*1000000007))%TMPNUM
    tmpw[h].write(f'{x} {y}\n')

print('***** Phase 2')

fw = open(sys.argv[2], 'w')
fw.write('u,v\n')

cnt = 0
for i in range(TMPNUM) :
    print('** file', i)
    tmpw[i].close()
    tmpr = open(f'tmp{i}', 'r')
    S = set()
    for line in tmpr :
        cnt += 1
        if (cnt%10000000 == 0) :
            print(cnt)
        l = line.split()
        x, y = int(l[0]), int(l[1])
        if (x,y) in S :
            continue
        S.add((x,y))
        fw.write(f'{x},{y}\n')
    tmpr.close()
    os.remove(f'tmp{i}')

fw.close()