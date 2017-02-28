# Using Movielens dataset: https://grouplens.org/datasets/movielens/ 
# SPARK SQL VERSION

# ratings.dat file coontaining:
# uIDd: User Id
# mId: Movie Id
# r: Ratings
# ts: Timestamp

# movies.dat file coontaining:
# mId: Movie Id
# t: Title
# gs: List of genders

# File stored in Databricks community cloud (small version):
# /FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat
# /FileStore/tables/y81un4d91488113985408/movies.dat

from pyspark.sql import SQLContext, Row

# DBCE SQL Context acquisition:
# sqlContext = SQLContext(sc)
sqlContext = SQLContext.getOrCreate(sc.getOrCreate()) 

# Load ratings file with rows uId, mId, r
ratings = (
  sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (uId, mId, r, ts) : Row(uId=int(uId), mId=int(mId), r=float(r))))

# Declare ratings as table
sqlContext.createDataFrame(ratings).registerTempTable("ratings") 

# Query number of ratings per user
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) FROM ratings GROUP BY uId") 
UsrRatingsCount.take(10)

# Query number of ratings per user, ordered
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) FROM ratings GROUP BY uId ORDER BY COUNT(*) DESC")
UsrRatingsCount.take(10)

