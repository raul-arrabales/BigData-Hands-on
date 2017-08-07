# Spark Hands On Training
# Databricks CE Cloud Practice
# Raul Arrabales / Conscious-Robots.com 

# Loading CSV files from DBFS into RDDs in cluster memory
moviesRDD = sc.textFile('/FileStore/tables/y81un4d91488113985408/movies.dat')
ratingsRDD = sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')

# See what we've got in the RDDs
print('--- Movies:')
print(moviesRDD.take(4))
print('--- Ratings:')
print(ratingsRDD.take(4))

# Workign with ratings, step by step

# Current data format in the RDD
ratingsRDD.take(6)  

# Split fields using a map transformation
SplittedRaingsRDD = ratingsRDD.map(lambda l : l.split('::'))

# See what we've got now: 
ratingsRDD.take(6)  

# Create pairs M/R style for the counting task:
RatingCountsRDD = SplittedRaingsRDD.map(lambda (uId, mId, r, ts) : (int(uId), 1))

# Taking a sample of our partial counts
Rsample = RatingCountsRDD.sample(False, 0.001)

# See how big the sample is and inspect
Rsample.count()
Rsample.take(6) 






