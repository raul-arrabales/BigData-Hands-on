# Create the Spark Context
import pyspark
sc = pyspark.SparkContext('local[*]')

# Check Spark Version
sc

sc.version

# Do something to prove it works
rdd = sc.parallelize(range(1000))
rdd.takeSample(False, 5)


