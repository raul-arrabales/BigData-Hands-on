# Create the Spark Context
import pyspark
sc = pyspark.SparkContext('local[*]')

# Do something to prove it works
rdd = sc.parallelize(range(1000))
rdd.takeSample(False, 5)

