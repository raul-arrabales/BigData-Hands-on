# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com 

# See files in DBFS
dbutils.fs.ls('/') 
dbutils.fs.ls('/FileStore/tables')
dbutils.fs.ls('/FileStore/tables/1x1xr57q1502297004187/') 

# Also accesible as a table (if table declared during upload to CE)
%sql select * from kmeans_table 

# Applying KMeans (new ml lib - not mllib)
from pyspark.ml.clustering import KMeans

# Loading data: 
# This is the old way, load into RDD
# dataset = sc.textFile('/FileStore/tables/1x1xr57q1502297004187/kmeans_data.txt')

# Reading CSV 
kmeans_df = sqlContext.read.format("com.databricks.spark.csv") \
  .option("header", "false").option("delimiter"," ") \
  .load("/FileStore/tables/1x1xr57q1502297004187/kmeans_data.txt")
  

# Simple clustering example:
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt 

data = sc.textFile("data/kmeans_data.txt")

# Load and parse input data
data = sc.textFile("data/kmeans_data.txt")
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))


