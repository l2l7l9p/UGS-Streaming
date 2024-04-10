import sys

fr = open(sys.argv[1], 'r')
fw = open(sys.argv[2], 'w')
fw.write('u,v\n')
S = set()
for line in fr :
    if line[0]=='%' :
        continue
    l = line.split()
    if len(l)<2 :
        continue
    x, y = int(l[0]), int(l[1])
    if x>y :
        x, y = y, x
    if (x==y) or ((x,y) in S) :
        continue
    S.add((x,y))
    fw.write(f'{x},{y}\n')