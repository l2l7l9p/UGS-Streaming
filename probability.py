import sys

samples = []

with open(sys.argv[1],'r') as fr :
    for line in fr :
        t = line.find('Samples')
        if t>-1 :
            exec(line[t::])
            samples.extend(Samples)

target = len(samples)
for i in range(target) :
    samples[i].sort()
samples.sort()

fw = open('probability.txt','w')

cnt = 0
for i in range(target) :
    if i==0 or samples[i]==samples[i-1] :
        cnt += 1
    else :
        fw.write(f'{samples[i-1]}: {cnt}/{target}\n')
        cnt = 1
fw.write(f'{samples[-1]}: {cnt}/{target}')