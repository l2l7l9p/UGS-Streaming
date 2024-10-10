# from pyspark.sql import SparkSession
# from pyspark.sql import functions as sf
# from pyspark.sql.types import *
import csv

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