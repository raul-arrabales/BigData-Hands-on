# PySpark Hands-On Training
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

# Reading CSV to a df - Change to spark.read.csv
kmeans_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter"," ").load("/FileStore/tables/1x1xr57q1502297004187/kmeans_data.txt")

# Check schema
kmeans_df.printSchema()

# Check data
display(kmeans_df) 

# Need to infer correctly the schema. Data are doubles, not string
kmeans_df = sqlContext.read.format("com.databricks.spark.csv") \
  .option("header", "false").option("delimiter"," ").option("inferschema", "true") \
  .load("/FileStore/tables/1x1xr57q1502297004187/kmeans_data.txt")
  
# Prepare data for training (see later the explanation about ML Pipelines)
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

assembler = VectorAssembler(inputCols=["_c0","_c1","_c2"], outputCol="features") 
assembler.transform(kmeans_df)

# Create the KMeans model
kmeans_estimator = KMeans().setFeaturesCol("features").setPredictionCol("prediction")
    
# Pipeline stages definition
pipeline = Pipeline(stages=[assembler, kmeans_estimator])

# Pipeline training
model = pipeline.fit(kmeans_df)

# Get the results: 
results = model.transform(kmeans_df)

# Check results:
display(results) 


# Without using Pipelines: 


# Clustering
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

# Load and parse input data
data = sc.textFile("data/kmeans_data.txt")
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))

# Build and train the model: K=2, 10 iterations. 
clusters = KMeans.train(parsedData, 2, 10)

# Evaluate the clustering
def error(point):
  center = clusters.centers[clusters.predict(point)]
  return sqrt(sum([x**2 for x in (point - center)]))
  
WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)

print("Within Set Sum of Squared Error = " + str(WSSSE))


# Saving and loading the model
clusters.save(sc, "MyModels")
sameModel = KMeansModel.load(sc, "MyModels")
sameModel
sameModel.k
sameModel.clusterCenters

  


