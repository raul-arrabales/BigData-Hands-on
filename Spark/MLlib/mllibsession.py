# TF-IDF

from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

hashingTF = HashingTF()

step2.take(4)

tf = hashingTF.transform(step2)

tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

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

  


