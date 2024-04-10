# from pyspark.sql import SparkSession
# from pyspark.sql import functions as sf
# from pyspark.sql.types import *
import math
import random
from bisect import bisect_left, bisect
import yaml, csv, logging, sys


class DSU_ELEMENT() :
    def __init__(self, root, size, last_saturated) :
        self.root, self.size, self.last_saturated = root, size, last_saturated
    
    def merge(self, other) :
        other.root = self.root
        self.size += other.size
        self.last_saturated = max(self.last_saturated, other.last_saturated)

class DSU() :
    def __init__(self, n, k, last_saturated) :
        self.n = n
        self.inf = k
        self.a = [None] + [DSU_ELEMENT(i, 1, last_saturated.get(i,0)) for i in range(1,n+1)]
    
    def get_root(self, x) :
        if self.a[x].root==x :
            return x
        else :
            self.a[x].root = self.get_root(self.a[x].root)
            return self.a[x].root
    
    def merge(self, x, y) :
        x_root = self.get_root(x)
        y_root = self.get_root(y)
        if x_root!=y_root :
            self.a[x_root].merge(self.a[y_root])
    
    def enlarge_size(self, x) :
        root = self.get_root(x)
        self.a[x].size += self.inf


def next_bit(S, size) :
    for i in range(size) :
        if ((S>>i)&1) :
            yield i


class Edge_list_local() :
    def __init__(self, edge_list_file) :
        self.edge_list_file = edge_list_file
        self.passes = 0
    
    def get_edge(self) :
        self.passes += 1
        with open(self.edge_list_file, 'r') as f :
            edge_list = csv.reader(f, delimiter=',')
            next(edge_list)
            for row in edge_list :
                yield int(row[0]), int(row[1])
    
    def end(self) :
        x = 0

class Edge_list_DataFrame() :
    def __init__(self, edge_list_file) :
        self.spark = SparkSession.builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        self.edge_list = self.spark.read.csv(edge_list_file, header=True, inferSchema=True)
        self.passes = 0
    
    def get_edge(self) :
        self.passes += 1
        for e in self.edge_list.toLocalIterator() :
            yield e['u'], e['v']
    
    def end(self) :
        self.spark.stop()

class Edge_list_StreamingDataFrame() :
    # TODO
    def __init__(self, edge_list_file) :
        x = 0

class Edge_list_memory() :
    def __init__(self, edge_list_file) :
        with open(edge_list_file, 'r') as f :
            edge_list = csv.reader(f, delimiter=',')
            next(edge_list)
            self.edge_list = list(edge_list)
        self.passes = 0
    
    def get_edge(self) :
        self.passes += 1
        for e in self.edge_list :
            yield int(e[0]), int(e[1])
    
    def end(self) :
        x = 0


class Graph() :
    def __init__(self, n, edgelistmode, edge_list_file) :
        self.n = n
        edge_list_class = {'local': Edge_list_local,
                           'DataFrame': Edge_list_DataFrame,
                           'StreamingDataFrame': Edge_list_StreamingDataFrame,
                           'memory': Edge_list_memory}
        self.edge_list = edge_list_class[edgelistmode](edge_list_file)
    
    def get_degree_G(self, vertex_list) :
        degree = {x: 0 for x in vertex_list}
        for x, y in self.edge_list.get_edge() :
            if (x in vertex_list and y in vertex_list) :
                degree[x] += 1
                degree[y] += 1
        return degree
    
    def get_degree_H(self, partition) :
        degree = {x: 0 for x in partition}
        for x, y in self.edge_list.get_edge() :
            if (partition.get(x,-1)==partition.get(y,-2)) :
                degree[x] += 1
                degree[y] += 1
        return degree
    
    def get_degree_dvGv(self, k) :
        degree = {x: 0 for x in range(1, self.n+1)}
        unsaturated_edge_list = {x: set() for x in range(1, self.n+1)}
        for x, y in self.edge_list.get_edge() :
            if (self.DD_ranking[x]>self.DD_ranking[y]) :
                x, y = y, x
            degree[x] += 1
            if degree[x]<=k-2 :
                unsaturated_edge_list[x].add(y)
        return degree, unsaturated_edge_list
    
    def get_degree_duGv(self, v, samples) :
        L, S, cnt = [set() for _ in range(self.n+1)], {}, [None]*(self.n+1)
        batch_size = len(samples)
        for j in range(batch_size) :
            for x in samples[j] :
                L[x].add(self.DD_ranking[v[j]])
                S.update({(x,y): False for y in samples[j]})
        for u in range(1, self.n+1) :
            L[u] = sorted(list(L[u]))
            cnt[u] = [0]*(len(L[u])+1)
        for x, y in self.edge_list.get_edge() :
            cnt[x][bisect(L[x], self.DD_ranking[y])-1] += 1
            cnt[y][bisect(L[y], self.DD_ranking[x])-1] += 1
            if (x,y) in S :
                S[(x,y)] = S[(y,x)] = True
        for u in range(1, self.n+1) :
            for l in range(len(L[u])-2, -1, -1) :
                cnt[u][l] += cnt[u][l+1]
        degree_duGv = {(self.DD_ordering[L[u][vi]], u): cnt[u][vi] for u in range(1, self.n+1) for vi in range(len(L[u]))}
        sampled_edge_list = [{(x,y) for x in samples[j] for y in samples[j] if (x<y and S.get((x,y),False)==True)} for j in range(batch_size)]
        return degree_duGv, sampled_edge_list
    
    def get_DD_ordering(self, epsilon) :
        logging.info('Computing approx DD-ordering...')
        
        vertices = set(range(1, self.n+1))
        
        DD_ordering = []
        alpha = epsilon/2
        beta = 2/epsilon+2+epsilon
        l = math.ceil(2*(1+beta))
        degree_G = self.get_degree_G(vertices)
        while (len(vertices)>0) :
            # PEEL
            max_degree = max(degree_G.values())
            logging.info('** DD_ordering while-loop with max degree %d', max_degree)
            H = {x[0] for x in degree_G.items() if x[1]>=max_degree/(1+alpha)}
            while (len(H)>0) :
                # SHAVE
                partition = {x: random.randint(0,l-1) for x in H}
                degree_H = self.get_degree_H(partition)
                R = {x[0] for x in degree_H.items() if x[1]<=max_degree/(1+beta)}
                H = H-R
                group = [set() for i in range(l)]
                for x in R :
                    group[partition[x]].add(x)
                for i in range(l) :
                    Si = set(filter(lambda x: degree_G[x]>=max_degree/(1+alpha), group[i]))
                    if (len(Si)==0) :
                        continue
                    DD_ordering.extend(list(Si))
                    vertices = vertices-Si
                    degree_G = self.get_degree_G(vertices)
        
        logging.info("Done.")
        return DD_ordering
    
    def get_indicator(self, degree_dvGv, unsaturated_edge_list, k) :
        last_saturated = {x: 0 for x in range(1, self.n+1)}
        for x, y in self.edge_list.get_edge() :
            if (self.DD_ranking[x]>self.DD_ranking[y]) :
                x, y = y, x
            if degree_dvGv[x]>k-2 and degree_dvGv[y]<=k-2 :
                last_saturated[y]=max(last_saturated[y], self.DD_ranking[x])
        indicator = {x: True for x in range(1,self.n+1)}
        dsu = DSU(self.n, k, last_saturated)
        for i in range(self.n-1, -1, -1) :
            x = self.DD_ordering[i]
            if degree_dvGv[x]<=k-2 :
                for y in unsaturated_edge_list.get(x, []) :
                    dsu.merge(x,y)
                indicator[x] = (dsu.a[x].size>=k or dsu.a[x].last_saturated>=i)
            else :
                dsu.enlarge_size(x)
        return indicator
    
    def sampling_preprocess(self, k, epsilon, ddorder = None) :
        if ddorder is None :
            self.DD_ordering = self.get_DD_ordering(epsilon)
        else :
            self.DD_ordering = ddorder
        self.passes_DD_ordering = self.edge_list.passes
        self.DD_ranking = {x: i for i, x in enumerate(self.DD_ordering)}
        degree_dvGv, unsaturated_edge_list = self.get_degree_dvGv(k)
        indicator = self.get_indicator(degree_dvGv, unsaturated_edge_list, k)
        b = {x: (pow(degree_dvGv[x],k-1) if indicator[x] else 0) for x in range(1, self.n+1)}
        Z = sum(b.values())
        self.p = {x: b[x]/Z for x in range(1, self.n+1)}
        self.Gamma = 1/(math.factorial(k-1)*Z*pow(1+epsilon,k-1))
    
    def RAND_GROW(self, v, k, batch_size) :
        samples = [{v[i]} for i in range(batch_size)]
        for i in range(k-1) :
            cnt, sampled_edge = [0]*batch_size, [None]*batch_size
            for x, y in self.edge_list.get_edge() :
                for j in range(batch_size) :
                    if (x in samples[j]) and (not (y in samples[j])) and (self.DD_ranking[y]>=self.DD_ranking[v[j]]) \
                        or (y in samples[j]) and (not (x in samples[j])) and (self.DD_ranking[x]>=self.DD_ranking[v[j]]) :
                            cnt[j] += 1
                            if random.randint(1,cnt[j])==1 :
                                sampled_edge[j] = [x,y]
            for j in range(batch_size) :
                samples[j].update(sampled_edge[j])
        return samples
    
    def RAND_GROW_fast(self, v, k, batch_size) :
        samples = [[v[i]] for i in range(batch_size)]
        for i in range(2,k+1) :
            degree_duGv, sampled_edge_list = self.get_degree_duGv(v, samples)
            L, Lv, cnt, num_index  = [{} for _ in range(self.n+1)], [None], [None], [None]
            for j in range(batch_size) :
                degree_duS = {u: len(list(filter(lambda e: e[0]==u or e[1]==u, sampled_edge_list[j]))) for u in samples[j]}
                u = random.choices(samples[j], weights=[degree_duGv[(v[j], u)]-degree_duS[u] for u in samples[j]])[0]
                up_index = list(zip(random.sample(range(1, degree_duGv[(v[j], u)]+1), degree_duS[u]+1), [j]*(degree_duS[u]+1)))
                if self.DD_ranking[v[j]] in L[u] :
                    L[u][self.DD_ranking[v[j]]].extend(up_index)
                else :
                    L[u][self.DD_ranking[v[j]]] = up_index
            for u in range(1, self.n+1) :
                L[u] = list(sorted(L[u].items()))
                num_index.append([0]+[degree_duGv[(self.DD_ordering[L[u][l][0]], u)] for l in range(len(L[u])-1,-1,-1)])
                Lv.append([L[u][l][0] for l in range(len(L[u]))])
                for l, (vx, jx) in enumerate(L[u]) :
                    jx.sort(reverse=True)
                    while l+1<len(L[u]) and len(jx)>0 and jx[-1][0]<=num_index[u][-(l+2)] :
                        L[u][-bisect_left(num_index[u], jx[-1][0])][1].append(jx[-1])
                        jx.pop()
                cnt.append([0]*(len(L[u])+1))
            for x, y in self.edge_list.get_edge() :
                it = bisect(Lv[x], self.DD_ranking[y])-1
                if it>-1 :
                    cnt[x][it] += 1
                    while len(L[x][it][1])>0 and L[x][it][1][-1][0]-num_index[x][-it-2]==cnt[x][it] :
                        j = L[x][it][1][-1][1]
                        if (not (y in samples[j])) and len(samples[j])<i :
                            samples[j].append(y)
                        L[x][it][1].pop()
                it = bisect(Lv[y], self.DD_ranking[x])-1
                if it>-1 :
                    cnt[y][it] += 1
                    while len(L[y][it][1])>0 and L[y][it][1][-1][0]-num_index[y][-it-2]==cnt[y][it] :
                        j = L[y][it][1][-1][1]
                        if (not (x in samples[j])) and len(samples[j])<i :
                            samples[j].append(x)
                        L[y][it][1].pop()
        
        return samples
    
    def PROB(self, v, samples, k, batch_size) :
        sampled_edge_list = [set() for i in range(batch_size)]
        degree_duGv = [{x: 0 for x in samples[i]} for i in range(batch_size)]
        for x, y in self.edge_list.get_edge() :
            for j in range(batch_size) :
                if (x in samples[j]) and (y in samples[j]) :
                    sampled_edge_list[j].add((x,y))
                if (x in samples[j]) and (self.DD_ranking[v[j]]<=self.DD_ranking[y]) :
                    degree_duGv[j][x] += 1
                if (y in samples[j]) and (self.DD_ranking[v[j]]<=self.DD_ranking[x]) :
                    degree_duGv[j][y] += 1
        for j in range(batch_size) :
            samples_j = list(samples[j].difference({v[j]}))
            q = [1]+[0]*((1<<(k-1))-1)
            for S in range(1<<(k-1)) :
                Sset = {samples_j[x] for x in next_bit(S,k)}.union({v[j]})
                for x in next_bit(S^(-1),k-1) :
                    ci = sum([degree_duGv[j][u] for u in Sset])-2*len(list(filter(lambda e: (e[0] in Sset) and (e[1] in Sset), sampled_edge_list[j])))
                    ni = len(list(filter(lambda e: e[0]==samples_j[x] and e[1] in Sset or e[1]==samples_j[x] and e[0] in Sset, sampled_edge_list[j])))
                    q[S|(1<<x)] += q[S]*ni/ci
            
            if random.random()>self.Gamma/(self.p[v[j]]*q[-1]) :
                samples[j] = None
    
    def PROB_fast(self, v, samples, k, batch_size) :
        degree_duGv, sampled_edge_list = self.get_degree_duGv(v, samples)
        for j in range(batch_size) :
            samples_j = list(set(samples[j]).difference({v[j]}))
            q = [1]+[0]*((1<<(k-1))-1)
            for S in range(1<<(k-1)) :
                Sset = {samples_j[x] for x in next_bit(S,k)}.union({v[j]})
                for x in next_bit(S^(-1),k-1) :
                    ci = sum([degree_duGv[(v[j],u)] for u in Sset])-2*len(list(filter(lambda e: (e[0] in Sset) and (e[1] in Sset), sampled_edge_list[j])))
                    ni = len(list(filter(lambda e: e[0]==samples_j[x] and e[1] in Sset or e[1]==samples_j[x] and e[0] in Sset, sampled_edge_list[j])))
                    q[S|(1<<x)] += q[S]*ni/ci
            
            if random.random()>self.Gamma/(self.p[v[j]]*q[-1]) :
                samples[j] = None
    
    def sample_one_batch(self, k, batch_size) :
        '''
        Do sampling (batch_size instances once a time).
        The result might contain failed samples, represented by None.
        '''
        v = random.choices(range(1, self.n+1), weights=[self.p[i] for i in range(1, self.n+1)], k=batch_size)
        samples = self.RAND_GROW_fast(v, k, batch_size)
        self.PROB_fast(v, samples, k, batch_size)
        # samples = self.RAND_GROW(v, k, batch_size)
        # self.PROB(v, samples, k, batch_size)
        return samples
    
    def sample(self, k, batch_size, target) :
        '''
        Do sampling (batch_size instances once a time) until there is a successful sample
        '''
        logging.info('Start sampling...')
        samples = []
        batch_count = 0
        while True:
            samples_batch = self.sample_one_batch(k, batch_size)
            for i, s in enumerate(samples_batch):
                if not (s is None):
                    samples.append(s)
                    if len(samples) == target :
                        logging.info('Sampled successfully after %d trials', batch_count*batch_size+i+1)
                        return samples
            batch_count += 1
            if batch_count%1==0 :
                logging.info('The %d-th batch. Totally %d trials, %d successful trials.', batch_count, batch_count*batch_size, len(samples))


def load_yaml() :
    with open("config.yml", "r") as f :
        re = yaml.load(f, Loader=yaml.FullLoader)
    return re

if __name__ == '__main__' :
    args = load_yaml()
    # if pyyaml can not be installed, you can replace it with the following, and set appropriate parameters:
    # args = {'n': 100,
            # 'edgelistfile': 'edge_list.csv',
            # 'edgelistmode': 'local',
            # 'k': 4,
            # 'epsilon': 0.1,
            # 'batch_size': 10,
            # 'target': 30}
    logging.basicConfig(level=logging.INFO,
                        handlers=[logging.FileHandler('logs.log'), logging.StreamHandler(sys.stdout)],
                        format="[%(asctime)s %(levelname)s] %(message)s")
    
    graph = Graph(args['n'], args['edgelistmode'], args['edgelistfile'])
    
    graph.sampling_preprocess(args['k'], args['epsilon'])
    logging.info('Number of passes for DD-ordering = %s', graph.passes_DD_ordering)
    
    samples = graph.sample(args['k'], args['batch_size'], args['target'])
    logging.info('Samples = %s', samples)
    logging.info('Number of passes in total = %s', graph.edge_list.passes)
    logging.info('Number of passes for DD-ordering = %s', graph.passes_DD_ordering)
    logging.info('Number of passes for sampling = %s', graph.edge_list.passes - graph.passes_DD_ordering)
    
    graph.edge_list.end()