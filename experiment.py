from streaming import Graph
import math
import random
from itertools import permutations
import yaml
import csv
import logging
import sys
import tracemalloc


def set_logger() :
    logging.basicConfig(level=logging.INFO,
                        handlers=[logging.FileHandler(f'logs{sys.argv[2]}.log'), logging.StreamHandler(sys.stdout)],
                        format="[%(asctime)s %(levelname)s] %(message)s")
    
    lfh = logging.FileHandler(f'data{sys.argv[2]}.log')
    lfh.setFormatter(logging.Formatter('%(message)s'))
    l = logging.getLogger('data')
    l.setLevel(logging.INFO)
    l.addHandler(lfh)
    return l

def load_yaml() :
    with open(f'config{sys.argv[2]}.yml', "r") as f :
        re = yaml.load(f, Loader=yaml.FullLoader)
    return re

if __name__ == '__main__' :
    args = load_yaml()
    args['testmode'] = sys.argv[1]
    data_logger = set_logger()
    random.seed(10)
     
    graph = Graph(args['n'], args['edgelistmode'], args['edgelistfile'])
    
    if args['testmode']=='ddordering' :
        # epsilon_list, passes_list = [i/10 for i in range(1, 11)]+[i/10 for i in range(15,61,5)], []
        epsilon_list, passes_list = [1, 1.2, 1.4, 1.6, 1.8, 2, 2.2, 2.4, 2.6, 2.8, 3]+[i/10 for i in range(35,61,5)], []
        for epsilon in epsilon_list :
            logging.info('********* Testing epsilon=%s', epsilon)
            s = 0
            for i in range(5) :
                graph.edge_list.passes = 0
                graph.get_DD_ordering(epsilon)
                s += graph.edge_list.passes
            passes_list.append(s/5)
            data_logger.info('epsilon: %s',epsilon_list)
            data_logger.info('passes: %s',passes_list)
    
    elif args['testmode']=='sampling' :
        epsilon_list, tot_passes_list, sampling_passes_list = [i/10 for i in range(4, 31, 2)]+[i/10 for i in range(35,61,5)], [], []
        for epsilon in epsilon_list :
            logging.info('********* Testing epsilon=%s', epsilon)
            data_logger.info('epsilon: %s',epsilon_list)
            graph.edge_list.passes = 0
            graph.sampling_preprocess(args['k'], epsilon)
            
            samples = graph.sample(args['k'], args['batch_size'], args['target'])
            logging.info('Number of passes in total = %s', graph.edge_list.passes)
            logging.info('Number of passes for DD-ordering = %s', graph.passes_DD_ordering)
            logging.info('Number of passes for sampling = %s', graph.edge_list.passes - graph.passes_DD_ordering)
            tot_passes_list.append(graph.edge_list.passes)
            sampling_passes_list.append(graph.edge_list.passes - graph.passes_DD_ordering)
            
            data_logger.info('total passes: %s',tot_passes_list)
            data_logger.info('sampling passes: %s',sampling_passes_list)
    
    elif args['testmode']=='memory' :
        k_list, tot_passes_list, sampling_passes_list, mem = [3, 4, 5], [], [], []
        tracemalloc.start()
        ddorder = graph.get_DD_ordering(args['epsilon'])
        data_logger.info('memory: %s',tracemalloc.get_traced_memory()[1])
        tracemalloc.stop()
        for k in k_list :
            logging.info('********* Testing k=%s', k)
            data_logger.info('k: %s', k_list)
            
            tracemalloc.start()
            
            graph.edge_list.passes = 0
            graph.sampling_preprocess(k, args['epsilon'], ddorder)
            mem.append(tracemalloc.get_traced_memory()[1])
            logging.info('Memory Usage: %s', mem[-1])
            
            graph.sample_one_batch(k, args['batch_size'])
            mem.append(tracemalloc.get_traced_memory()[1])
            logging.info('Memory Usage: %s', mem[-1])
            
            logging.info('Number of passes in total = %s', graph.edge_list.passes)
            logging.info('Number of passes for DD-ordering = %s', graph.passes_DD_ordering)
            logging.info('Number of passes for sampling = %s', graph.edge_list.passes - graph.passes_DD_ordering)
            tot_passes_list.append(graph.edge_list.passes)
            sampling_passes_list.append(graph.edge_list.passes - graph.passes_DD_ordering)
            
            data_logger.info('total passes: %s',tot_passes_list)
            data_logger.info('sampling passes: %s',sampling_passes_list)
            data_logger.info('memory: %s',mem)
            tracemalloc.stop()
    
    graph.edge_list.end()
