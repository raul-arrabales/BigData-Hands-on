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

# And beter this way:
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) AS UserCount " +
                                 "FROM ratings " +
                                 "GROUP BY uId " +
                                 "ORDER BY UserCount DESC")
UsrRatingsCount.take(10)
# [Row(uId=14463, _c1=5169),
#  Row(uId=27468, _c1=4449),
#  Row(uId=19635, _c1=4165),
#  Row(uId=3817, _c1=4165),
#  Row(uId=27584, _c1=3479),
#  Row(uId=6757, _c1=3414),
#  Row(uId=19379, _c1=3202),
#  Row(uId=7795, _c1=3187),
#  Row(uId=8811, _c1=3164),
#  Row(uId=30723, _c1=3027)]

