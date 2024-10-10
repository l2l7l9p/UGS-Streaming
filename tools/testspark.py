from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
import argparse
import math

class Graph() :
    def __init__(self, spark) :
        self.edge_list = spark.read.csv('edge_list.csv', header=True)
    
    def get_degree_G_list(self) :
        degree = self.edge_list.union(self.edge_list.select('v', 'u')) \
                               .groupby('u').count() \
                               .withColumnRenamed('count', 'degree')
        return degree


if __name__ == '__main__' :
    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    graph = Graph(spark)
    degree = graph.get_degree_G_list()
    degree.show()
    
    spark.stop()