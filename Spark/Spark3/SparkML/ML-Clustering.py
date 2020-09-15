# PySpark Hands-On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com 

# Reading CSV to a df - Change to spark.read.csv
url = "https://raw.githubusercontent.com/raul-arrabales/BigData-Hands-on/master/Spark/Spark3/data/kmeans_data.txt"
from pyspark import SparkFiles
spark.sparkContext.addFile(url)

kmeans_df = spark.read.csv("file://"+SparkFiles.get("kmeans_data.txt"), header=False, inferSchema=True, sep=" ")

# Check schema
kmeans_df.printSchema()

# Check data
display(kmeans_df) 

# Applying KMeans (new ml lib - not mllib)
from pyspark.ml.clustering import KMeans
  
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

  


