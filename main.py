import yaml, logging, sys, pickle
from streaming import Graph

def load_yaml() :
    with open("config.yml", "r") as f :
        re = yaml.load(f, Loader=yaml.FullLoader)
    return re

if __name__ == '__main__' :
    args = load_yaml()
    logging.basicConfig(level=logging.DEBUG,
                        handlers=[logging.FileHandler('logs.log'), logging.StreamHandler(sys.stdout)],
                        format="[%(asctime)s %(levelname)s] %(message)s")
    
    # initialize
    graph = Graph(args['n'], args['edgelistmode'], args['edgelistfile'], args['MAX_EDGES'])
    
    # preprocess
    if 'ddorderfile' in args :
        # preprocess with a given dd-order
        with open(args['ddorderfile'],'rb') as f:
            ddorder = pickle.load(f)
        graph.sampling_preprocess(args['k'], args['epsilon'], ddorder)
    else :
        # preprocess by computing a dd-order
        c = args['fasterc'] if 'faster' in args['ddordermode'] else 0.1
        graph.sampling_preprocess(args['k'], args['epsilon'], None, args['ddordermode'], c)
        with open('DD_order.bin','wb') as f:
            pickle.dump(graph.DD_order, f)
    passes_for_preprocess = graph.edge_list.passes
    
    # sample
    samples = graph.sample(args['k'], args['target'])
    with open('samples.bin','wb') as f:
        pickle.dump(samples, f)
    
    logging.info('Number of passes in total = %s', graph.edge_list.passes)
    logging.info('Number of passes for preprocessing = %s', passes_for_preprocess)
    logging.info('Number of passes for sampling = %s', graph.edge_list.passes - passes_for_preprocess)
    
    graph.edge_list.end()