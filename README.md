# UGS-Streaming

This is the streaming algorithm for Uniform Graphlet Sampling.

# Main Code

## main.py

This is the code for sampling a graphlet of size $k$ uniformly in a large undirected graph of $n$ vertices in a streaming manner. 

To run the code, first install the following packages:

- pyspark (Only necessary for the `DataFrame` mode)

- pyyaml

- numpy

Then, prepare for the edge list file and the configuration `config.yml`, and put them in the same directory with the code. The edge list file should be in the following format (a header followed by all edges) and not contain loops or duplicated edges:

```
u,v
x1,y1
x2,y2
x3,y3
...
xm,ym
```

The parameters in `config.yml` are:

- `n`: the number of vertices,

- `edgelistfile`: the name of the edge list file,

- `edgelistmode`: the mode of storing the edge list which has four choices:
  
  - `local`: The code reads the edges from the local file regarding it as a streaming source. Everything except the edge list is stored in the main memory. We assume the main memory is $O(nk^2)$.
  
  - `DataFrame`: The code iterates the edges from Spark DataFrame regarding it as a streaming source. We assume that the driver node has $O(nk^2)$ memory. Large data (e.g., edge list of size $O(n^2)$) is stored in the form of Spark DataFrame so that it is distributed, while small data (e.g., the vertex list of size $O(n)$ or $O(nk)$) is stored in the driver's memory.
  
  - `StreamingDataFrame` (TODO): The code iterates the edges from Spark StreamingDataFrame.
  
  - `memory`: The code stores everything (including the edge list) in main memory.

- `ddordermode`: choose an of the following algorithms to compute the DD order,
  
  - `approx`
  
  - `approx-heuristic`

- `epsilon`: the parameter for DD-order,

- `k`: the parameter for graphlet sampling,

- `target`: we do sampling in batches until there are `target` successful trials.

- `MAX_EDGES`: the maximum number of edges we store during the computation of DD-order and sampling. We will perform sampling with a batch size `MAX_EDGES/(k*k)`.

- (optional) `ddorderfile`: a binary file containing a list representing the DD-order, so that we do not compute it again.

Finally, run:

```
python main.py
```

If `edgelistmode` is `DataFrame`, you may also run the code by:

```
spark-submit main.py 
```

It will print the logs on the terminal and produce the following three files:

- `logs.log`: the logs.

- `DD_order.bin`: the DD-order which is a list of size `n`. Use `pickle.load()` to recover it. (This file is produced only when `ddorderfile` is NOT specified in the config.)

- `samples.bin`: the sampling results which is a nested list of shape `target*k`. Use `pickle.load()` to recover it.

# Other Tools

## testspark.py

This is the code for testing the Spark environment. It reads the edge list from the file `edge_list.csv` and calculates the degree of vertices.

To run it on the personal computer, a cleaner way is to install pyspark (through pip or conda), JVM (usually by install JAVA) and Hadoop, and set the environment variables correctly. You may follow the documentation:

- [PySpark Overview — PySpark 3.5.0 documentation](https://spark.apache.org/docs/latest/api/python/index.html) The instruction of installation and quick start.

Then, run

```
python testspark.py
```

or

```
spark-submit testspark.py
```

The edge list should be in the same directory named as `edge_list.csv`.

To run it on a cluster, you may need to install Spark completely and other dependent environments (e.g., python, HDFS). You may follow the documentation:

- [Cluster Mode Overview - Spark 3.5.0 Documentation](https://spark.apache.org/docs/latest/cluster-overview.html) An introduction to the cluster mode of Spark.

- [Spark Standalone Mode - Spark 3.5.0 Documentation](https://spark.apache.org/docs/latest/spark-standalone.html) Standalone mode, one of the cluster mode that is claimed to be easy to use.

- [Submitting Applications - Spark 3.5.0 Documentation](https://spark.apache.org/docs/latest/submitting-applications.html) An instruction of submitting the code to the cluster.

## gen_edge.cpp

This program is to generate the edge list of a random undirected graph. It takes 2 arguments `n` and `p`, indicating the number of vertices and the probability of each edge.

G++ is the recommended compiler for this code.

```
g++ gen_edge.cpp -o gen_edge
```

It produces a file `edge_list.csv` in the same directory. Then, for example, run

```
./gen_edge 10 0.5
```

## reformat.py

This is to reformat the dataset from http://konect.cc/networks/ , removing duplicated edges and self-loops.

The usage is

```
python reformat.py <INPUT_FILE> <OUTPUT_FILE>
```

For example,

```
python reformat.py out.edit-biwikibooks edge_list.csv
```

## probability.py

This is to print the probability of each $k$-graphlet in a log file.

The code takes one argument as the binary file containing the sampling result (for example, `samples.bin`). Then, run

```
python probability.py samples.bin
```

It will produce a file `probability.txt` showing the probability of each $k$-graphlet in the log file.
