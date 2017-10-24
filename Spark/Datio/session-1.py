# RDD Data Set load example (karma) 

! ls -la 

! head -10 movies.dat

! head -10 ratings_verysmall.dat

# Loading CSV files from file into RDDs in cluster memory
moviesRDD = sc.textFile('movies.dat')
ratingsRDD = sc.textFile('ratings_verysmall.dat')

# See what we've got in the RDDs
print('--- Movies:')
print(moviesRDD.take(4))
print('--- Ratings:')
print(ratingsRDD.take(4))

# Current data format in the RDD
ratingsRDD.take(6)  

# Split fields using a map transformation
SplittedRatingsRDD = ratingsRDD.map(lambda l : l.split('::'))

# See what we've got now: 
SplittedRatingsRDD.take(6) 

# Create pairs M/R style for the counting task (Mapper):
RatingCountsRDD = SplittedRatingsRDD.map( lambda (uId, mId, r, ts) : (int(uId), 1))

RatingCountsRDD.count()

# Taking a sample of our partial counts
Rsample = RatingCountsRDD.sample(False, 0.001)

# See how big the sample is and inspect
Rsample.count()

# See how big the sample is and inspect
Rsample.take(6) 

# Aggregate counts by user (Reducer)
RatingsByUserRDD = RatingCountsRDD.reduceByKey(lambda r1, r2 : r1 + r2)

# Inspect:
RatingsByUserRDD.takeSample(False, 10)

# Get the top 10 users by the number of ratings:
RatingsByUserRDD.takeOrdered(10, key=lambda (uId, nr): -nr)

# Nested version of the same using a "karma" RDD:
karma = (
sc.textFile('ratings_verysmall.dat')
.map(lambda l : l.split('::'))
.map(lambda (uId, mId, r, ts) : (int(uId), 1))
.reduceByKey(lambda r1, r2 : r1 + r2)
)
karma.takeOrdered(10, key=lambda (uId, nr): -nr)

