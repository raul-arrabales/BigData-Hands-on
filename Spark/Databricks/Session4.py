# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com 

# See files in DBFS
dbutils.fs.ls('/') 

# Simple clustering example:
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt 

data = sc.textFile("data/kmeans_data.txt")

# Load and parse input data
data = sc.textFile("data/kmeans_data.txt")
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))


