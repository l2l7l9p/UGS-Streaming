## streaming.py

This is the code for sampling a graphlet of size $k$ uniformly in a large graph of $n$ vertices in a  streaming manner. 

To run the code, first install the following packages for running the code:

- pyspark (necessary for the `DataFrame` mode)

- pyyaml  (which is used to read configuration from a yml file. If you cannot install pyyaml, you just need to use the third line of the main function instead of the first line.)

Then, prepare for the edge list file and `config.yml` where you set the parameters, and put them in the same directory with the code. The parameters are:

- `n`: the number of vertices,

- `edgelistfile`: the path of the edge list file,

- `edgelistmode`: the mode of storing the edge list which has three choices:
  
  - `local`: The code reads the edges from the local file regarding it as a streaming source. Everything except the edge list is stored in the main memory. We assume the main memory is $O(nk)$.
  
  - `DataFrame`: The code iterates the edges from Spark DataFrame regarding it as a streaming source. We assume that the driver node has $O(nk)$ memory. Large data (e.g., edge list of size $O(n^2)$) is stored in the form of Spark DataFrame so that it is distributed, while small data (e.g., the vertex list of size $O(n)$ or $O(nk)$) is stored in the driver's memory.
  
  - `StreamingDataFrame` (TODO): The code iterates the edges from Spark StreamingDataFrame.
  
  - `memory`: The code stores everything (including the edge list) in main memory.

- `epsilon`: the parameter for DD-ordering,

- `k`: the parameter for graphlet sampling,

- `batch_size`: the number of samplings we do in parallel (i.e., within two passes for Algorithm 3),

- `target`: we do sampling in batches (each batch of size `batch_size`) until there are `target` successful trials.

Finally, run:

```
python streaming.py
```

If `edgelistmode` is `DataFrame`, you may also run the code by:

```
spark-submit streaming.py 
```

It will print the logs on the terminal as well as to the file `logs.log` in the same directory with the code.

## experiment.py

This is the script for experiments. It produces data we need, storing it in `data.log` and drawing a figure. Detail logs are printed on the terminate so that you can see the progress of running.

It takes two arguments, the first of which is the test mode and the second is the experiment ID (let it be `id`). Default parameters are specified in `config<id>.yml`. It outputs the logging information to `logs<id>.log` and data to `data<id>.log`. (e.g., if `id` is `1`, the config file is `config1.yml` and it produces `logs1.log` and `data1.log`.)

For example,

```
python experiment.py ddordering 1
python experiment.py sampling 2
```

Note: use `logging.info()` to print logs and `data_logger.info()` to store data.

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
