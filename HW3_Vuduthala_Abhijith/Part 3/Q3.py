#from _future_ import print_function
from pyspark.ml.clustering import KMeans

import sys
from operator import add

from pyspark.sql import SparkSession
from pyspark import SparkContext

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col 
import pandas as pd

spark = SparkSession\
        .builder\
        .appName("PythonKMeans")\
        .getOrCreate()

sc = SparkContext.getOrCreate()
# Loads data.
data = spark.read.csv("wine.data")

td=data.rdd.map(lambda x:(Vectors.dense(x[1:-1]), x[0])).toDF(["features", "label"])
td.show()

# Trains a k-means model.
kmeans = KMeans().setK(3).setSeed(1)
model = kmeans.fit(td)

# Shows the result.
clusters = model.clusterCenters()
sc.parallelize(clusters).saveAsTextFile("kMeansOutput")
print("Cluster Centers: ")
for cluster in clusters:
    print(cluster)
