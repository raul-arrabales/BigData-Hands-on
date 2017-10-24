# Movie Ratings Example (karma)
## Using RDD and DataFrames APIs

# Loading movies dataset as a set of rows
movies = (
  sc.textFile('movies.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (mId, t, gs) : Row(mId=int(mId), t=t)))

# Loading ratings dataset as a set of rows
ratings = (
  sc.textFile('ratings_verysmall.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (uId, mId, r, ts) : Row(mId=int(mId), r=float(r))))
  

# Register both as SQL tables
sqlContext.createDataFrame(movies).registerTempTable("movies")
sqlContext.createDataFrame(ratings).registerTempTable("ratings")

# Launch the SQL query
avgRs = sqlContext.sql("SELECT t, AVG(r) AS avgR " +
"FROM movies m " +
"JOIN ratings r ON (m.mId = r.mId) " +
"GROUP BY t ORDER BY avgR DESC")

# Inspect resutls
avgRs.take(10) 



# Exercise
## Counting ratings per user


# Another example: Counting ratings per user.
# Load ratings as rows with user id, movie id and rating (float):
ratings = (
  sc.textFile('ratings_verysmall.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (uId, mId, r, ts) : Row(uId=int(uId), mId=int(mId), r=float(r))))
  
 
# Register the table for SQL
sqlContext.createDataFrame(ratings).registerTempTable("ratings")


# SQL query for counting ratings:
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) FROM ratings GROUP BY uId")


# Inspect results:
UsrRatingsCount.take(10)

# SQL query for counting ratings (ordered):
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) FROM ratings GROUP BY uId ORDER BY COUNT(*) DESC")


# Inspect results:
UsrRatingsCount.take(10)


# SQL query for counting ratings (ordered and good formatting):
UsrRatingsCount = sqlContext.sql("SELECT uId, COUNT(*) AS UserCount " +
                                 "FROM ratings " +
                                 "GROUP BY uId " +
                                 "ORDER BY UserCount DESC")


# Inspect results:
UsrRatingsCount.take(10)

