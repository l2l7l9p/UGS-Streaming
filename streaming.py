import math, random, heapq
from bisect import bisect_left, bisect
import numpy as np
from edge import *
import logging


class DSU() :
    def __init__(self, n, last_heavy) :
        self.n = n
        self.a = np.array([(i, 1, last_heavy[i]) for i in range(n+1)], dtype=[('root',int), ('size',int), ('last_heavy',int)])
    
    def get_root(self, x) :
        if self.a[x]['root']==x :
            return x
        else :
            self.a[x]['root'] = self.get_root(self.a[x]['root'])
            return self.a[x]['root']
    
    def merge(self, x, y) :
        x_root = self.get_root(x)
        y_root = self.get_root(y)
        if x_root!=y_root :
            self.a[y_root]['root'] = x_root
            self.a[x_root]['size'] += self.a[y_root]['size']
            self.a[x_root]['last_heavy'] = max(self.a[x_root]['last_heavy'], self.a[y_root]['last_heavy'])


def next_bit(S, size) :
    for i in range(size) :
        if ((S>>i)&1) :
            yield i


class Query() :
    def __init__(self, u) :
        self.u = u
        self.v_list, self.queries = set(), set()
    
    def build_cnt(self) :
        self.v_list = sorted(list(self.v_list))
        self.cnt = [0]*(len(self.v_list)+1)
    
    def build_queries_for_v(self, degree_duGv, DD_order) :
        self.num_index = [degree_duGv[(DD_order[v], self.u)] for v in self.v_list]+[0]
        self.queries_for_v = [[] for v in self.v_list]
        vi = 0
        for q in sorted(list(self.queries), reverse=True) :
            while vi+1<len(self.v_list) and q<=self.num_index[vi+1] :
                vi += 1
            self.queries_for_v[vi].append(q-self.num_index[vi+1])
        self.queries = None


class Graph() :
    def __init__(self, n, edgelistmode, edge_list_file, MAX_EDGES) :
        self.n = n
        edge_list_class = {'local': Edge_list_local,
                           'DataFrame': Edge_list_DataFrame,
                           'StreamingDataFrame': Edge_list_StreamingDataFrame,
                           'memory': Edge_list_memory}
        self.edge_list = edge_list_class[edgelistmode](edge_list_file)
        self.MAX_EDGES = MAX_EDGES
    
    def get_degree_G(self, vertices, degree) :
        # get global degree
        degree.fill(0)
        for x, y in self.edge_list.get_edge() :
            if (x in vertices and y in vertices) :
                degree[x] += 1
                degree[y] += 1
    
    def get_degree_H(self, partition) :
        # get degree within each partition
        if (len(partition)<self.n/10) :
            degree = {x: 0 for x in partition}
        else :
            degree = np.zeros(self.n+1, dtype=int)
        for x, y in self.edge_list.get_edge() :
            if (partition.get(x,-1)==partition.get(y,-2)) :
                degree[x] += 1
                degree[y] += 1
        return degree
    
    def PEEL_approx(self, vertices, degree_G, epsilon, alpha, max_degree) :
        beta = 2/epsilon+2+epsilon
        l = math.ceil(2*(1+beta))
        threshold_lb, threshold_ub = max_degree/(1+alpha), max_degree/(1+beta)
        H = {x for x in vertices if degree_G[x]>=threshold_lb}
        while H :
            # SHAVE
            logging.debug("*** SHAVE for %d vertices", len(H))
            partition = {x: random.randint(0,l-1) for x in H}
            degree_H = self.get_degree_H(partition)
            R = {x for x in H if degree_H[x]<=threshold_ub}
            H -= R
            group = [[] for i in range(l)]
            for x in R :
                group[partition[x]].append(x)
            partition, degree_H = None, None
            for i in range(l) :
                Si = set(x for x in group[i] if degree_G[x]>=threshold_lb)
                if (not Si) :
                    continue
                self.DD_order.extend(Si)
                vertices -= Si
                self.get_degree_G(vertices, degree_G)
    
    def PEEL_bruteforce(self, vertices, edges, heap, threshold_lb, degree) :
        while heap and -heap[0][0]>=threshold_lb:
            top = heapq.heappop(heap)
            if (-top[0]!=degree[top[1]]) :
                continue
            self.DD_order.append(top[1])
            vertices.remove(top[1])
            for y in edges[top[1]] :
                if y in vertices :
                    degree[y] -= 1
                    heapq.heappush(heap,(-degree[y], y))
    
    def PEEL_heuristic(self, vertices, heap, degree, threshold_lb) :
        if (len(heap)<self.n/10) :
            edges = {x: [] for d,x in heap}
        else :
            edges = [[] for x in range(self.n+1)]
        for x, y in self.edge_list.get_edge() :
            if (x in vertices and y in vertices and (-degree[x],x)<=heap[-1] and (-degree[y],y)<=heap[-1]) :
                edges[x].append(y)
                edges[y].append(x)
        self.PEEL_bruteforce(vertices, edges, heap, threshold_lb, degree)
    
    def remove_isolated_vertices(self, vertices, vertices_isolated, degree) :
        transfer = {v for v in vertices if degree[v]==0}
        vertices -= transfer
        vertices_isolated.extend(transfer)
        logging.debug('*** Remove %d isolated vertices', len(transfer))
    
    def compute_DD_order(self, epsilon, mode) :
        logging.info('Computing approx DD-ordering...')
        
        vertices = set(range(1, self.n+1))
        vertices_isolated, self.DD_order = [], []
        degree_G = np.zeros(self.n+1, dtype=int)
        self.get_degree_G(vertices, degree_G)
        alpha = epsilon/2
        
        while vertices:
            if 'heuristic' in mode:
                # compute the number of vertices and degree that heuristic can reduce
                vertex_list = sorted([(-degree_G[x],x) for x in vertices])
                if (len(vertices)<self.n/10) :
                    degree_dvGRv = {x: 0 for x in vertices}
                else :
                    degree_dvGRv = np.zeros(self.n+1, dtype=int)
                for x, y in self.edge_list.get_edge() :
                    if (x in vertices and y in vertices) :
                        degree_dvGRv[x if (-degree_G[y],y)<(-degree_G[x],x) else y] += 1
                prefix_sum_degree = 0
                prefix_ind = len(vertex_list)
                for i, (d,x) in enumerate(vertex_list) :
                    prefix_sum_degree+=degree_dvGRv[x]
                    if (prefix_sum_degree>self.MAX_EDGES) :
                        prefix_ind = i
                        break
                degree_dvGRv = None
                # decide if we run heuristic
                max_degree = -vertex_list[0][0]
                threshold_lb = max_degree/(1+alpha)
                if (prefix_ind==len(vertex_list) or (-vertex_list[prefix_ind][0])<=threshold_lb) :
                    logging.info('** PEEL_heuristic for %d vertices, max degree %d, select %d vertices', len(vertices), max_degree, prefix_ind)
                    self.PEEL_heuristic(vertices, vertex_list[0:prefix_ind], degree_G,
                                    ((-vertex_list[prefix_ind][0])/(1+epsilon) if prefix_ind<len(vertex_list) else 0) )
                    self.get_degree_G(vertices, degree_G)
                    self.remove_isolated_vertices(vertices, vertices_isolated, degree_G)
                    continue
            
            if 'approx' in mode:
                max_degree = degree_G.max()
                logging.info('** PEEL_approx for %d vertices, max degree %d', len(vertices), max_degree)
                self.PEEL_approx(vertices, degree_G, epsilon, alpha, max_degree)
            
            self.remove_isolated_vertices(vertices, vertices_isolated, degree_G)
        
        self.DD_order.extend(vertices_isolated)
    
    def get_b(self, k) :
        # compute d(v|G(v))
        degree_dvGv, light_edge_list, last_heavy = np.zeros(self.n+1, dtype=int), [[] for x in range(self.n+1)], [-1]*(self.n+1)
        for x, y in self.edge_list.get_edge() :
            if (self.DD_ranking[x]>self.DD_ranking[y]) :
                x, y = y, x
            degree_dvGv[x] += 1
            if degree_dvGv[x]<=k-2 :
                light_edge_list[x].append(y)
            else :
                last_heavy[x] = self.DD_ranking[x]
                if not (light_edge_list[x] is None) :
                    for z in light_edge_list[x] :
                        last_heavy[z]=max(last_heavy[z], self.DD_ranking[x])
                    light_edge_list[x] = None
                last_heavy[y]=max(last_heavy[y], self.DD_ranking[x])
        
        # compute b[v] <- d(v|G(v))^{k-1} if the bucket of v is not empty, otherwise 0
        b = [0]*(self.n+1)
        dsu = DSU(self.n, last_heavy)
        for i in range(self.n-1, -1, -1) :
            x = self.DD_order[i]
            if degree_dvGv[x]<=k-2 :
                for y in light_edge_list[x] :
                    dsu.merge(x,y)
                b[x] = pow(int(degree_dvGv[x]),k-1) if (dsu.a[x]['size']>=k or dsu.a[x]['last_heavy']>=i) else 0
            else :
                b[x] = pow(int(degree_dvGv[x]),k-1)
        return b
    
    def sampling_preprocess(self, k, epsilon, ddorder = None, DD_order_mode = 'approx-heuristic') :
        if ddorder is None :
            self.compute_DD_order(epsilon, DD_order_mode)
            logging.info('Number of passes for DD-ordering = %s', self.edge_list.passes)
        else :
            self.DD_order = ddorder
        logging.info('Sampling preprocessing...')
        self.DD_ranking = np.empty(self.n+1, dtype=int)
        self.DD_ranking[self.DD_order] = np.arange(self.n)
        b = self.get_b(k)
        Z = sum(b)
        self.p = [b[x]/Z for x in range(self.n+1)]
        self.Gamma = 1/(math.factorial(k-1)*Z*pow(1+epsilon,k-1))
        logging.debug('*** max(p) = %f', max(self.p))
    
    def get_degree_duGv(self, samples) :
        L, edges_internal = {}, set()
        edges_internal_queries = {(x,y) for sample in samples for x in sample for y in sample if x<y}
        for sample in samples :
            for u in sample :
                if not (u in L) :
                    L[u] = Query(u)
                L[u].v_list.add(self.DD_ranking[sample[0]])
        for u, Lu in L.items() :
            Lu.build_cnt()
        for x, y in self.edge_list.get_edge() :
            if x in L: L[x].cnt[bisect(L[x].v_list, self.DD_ranking[y])-1] += 1
            if y in L: L[y].cnt[bisect(L[y].v_list, self.DD_ranking[x])-1] += 1
            if (x,y) in edges_internal_queries or (y,x) in edges_internal_queries :
                edges_internal.add((x,y))
                edges_internal_queries.discard((x,y))
                edges_internal_queries.discard((y,x))
        for u, Lu in L.items() :
            for l in range(len(Lu.v_list)-2, -1, -1) :
                Lu.cnt[l] += Lu.cnt[l+1]
        degree_duGv = {(self.DD_order[v], u): Lu.cnt[i] for u, Lu in L.items() for i,v in enumerate(Lu.v_list)}
        return degree_duGv, edges_internal
    
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
        samples = [list(x) for x in samples]
        return samples
    
    def RAND_GROW_fast(self, v, k, batch_size) :
        samples = [[v[i]] for i in range(batch_size)]
        for i in range(2,k+1) :
            degree_duGv, edges_internal = self.get_degree_duGv(samples)
            L, original_query, answers, up  = {}, [None]*batch_size, {}, np.zeros(batch_size, dtype=int)
            # set up queries
            for j in range(batch_size) :
                degree_duS = {x: sum(1 for y in samples[j] if (x,y) in edges_internal or (y,x) in edges_internal) for x in samples[j]}
                u = random.choices(samples[j], weights=[degree_duGv[(v[j], u)]-degree_duS[u] for u in samples[j]])[0]
                up[j] = u
                original_query[j] = random.sample(range(1, degree_duGv[(v[j], u)]+1), degree_duS[u]+1)
                if not (u in L) :
                    L[u] = Query(u)
                L[u].v_list.add(self.DD_ranking[v[j]])
                L[u].queries.update(original_query[j])
            edges_internal = None
            # move queries to right v
            for u, Lu in L.items() :
                Lu.build_cnt()
                Lu.build_queries_for_v(degree_duGv, self.DD_order)
            degree_duGv = None
            # answer to queries
            for xx, yy in self.edge_list.get_edge() :
                for x, y in [(xx,yy), (yy,xx)] :
                    if x in L :
                        it = bisect(L[x].v_list, self.DD_ranking[y])-1
                        if it>-1 :
                            L[x].cnt[it] += 1
                            q = L[x].queries_for_v[it]
                            while len(q)>0 and q[-1]==L[x].cnt[it]+L[x].num_index[it+1] :
                                answers[(x,q[-1])] = y
                                q.pop()
            L = None
            # sample by answers
            for j in range(batch_size) :
                for eid in original_query[j] :
                    u = answers.get((up[j], eid),-1)
                    if u!=-1 and not (u in samples[j]) :
                        samples[j].append(u)
                        break
        
        return samples
    
    def PROB(self, v, samples, k, batch_size) :
        degree_duGv, edges_internal = self.get_degree_duGv(samples)
        for j in range(batch_size) :
            # compute the probability of samples[j], storing it in q[-1]
            sampled_edge_list = {(x,y) for x in samples[j] for y in samples[j] if (x,y) in edges_internal}
            samples_j = samples[j][1:k]
            q = [1]+[0]*((1<<(k-1))-1)
            for S in range(1<<(k-1)) :
                Sset = {samples_j[x] for x in next_bit(S,k)}.union({v[j]})
                ci = sum(degree_duGv[(v[j],u)] for u in Sset)-2*sum(1 for e in sampled_edge_list if e[0] in Sset and e[1] in Sset)
                for x in next_bit(S^(-1),k-1) :
                    ni = sum(1 for y in Sset if (samples_j[x],y) in sampled_edge_list or (y,samples_j[x]) in sampled_edge_list)
                    q[S|(1<<x)] += q[S]*ni/ci
            
            if random.random()>self.Gamma/(self.p[v[j]]*q[-1]) :
                samples[j] = None
    
    def sample_one_batch(self, k, batch_size) :
        # Sample batch_size trials in a batch.
        # The result might contain failed samples, represented by None.
        v = random.choices(range(self.n+1), weights=self.p, k=batch_size)
        # samples = self.RAND_GROW(v, k, batch_size)
        samples = self.RAND_GROW_fast(v, k, batch_size)
        self.PROB(v, samples, k, batch_size)
        return samples
    
    def sample(self, k, target) :
        # Sample batch_size trials once a batch until the number of successful samples reaches target.
        batch_size, batch_count = self.MAX_EDGES//(k*k), 0
        samples = []
        logging.info('Start sampling with batch size %d ...',batch_size)
        while True:
            samples_batch = self.sample_one_batch(k, batch_size)
            for i, s in enumerate(samples_batch):
                if not (s is None):
                    samples.append(s)
                    if len(samples) == target :
                        logging.info('Sampled successfully after %d trials', batch_count*batch_size+i+1)
                        return samples
            batch_count += 1
            logging.info('The %d-th batch. Totally %d trials, %d successful trials.', batch_count, batch_count*batch_size, len(samples))