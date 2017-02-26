# Using Movielens dataset: https://grouplens.org/datasets/movielens/ 

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

movies = (
  sc.textFile('/FileStore/tables/y81un4d91488113985408/movies.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (mId, t, gs) : (int(mId), t))
)

ratings = (
  sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (uId, mId, r, ts) : (int(mId), float(r)))
  .groupByKey()
  .map(lambda (mId, rs) : (mId, sum(rs) / len(rs)))
)

results = (
  movies.join(ratings)
  .map(lambda (mId, (t, r)) : (t, r))
)

results.takeOrdered(10, key=lambda (t, r): ‐r)

# Step by step explanation

