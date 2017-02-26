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

results.takeOrdered(10, key=lambda (t, r): -r)

# Step by step explanation

MovieRatingsRDD = sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat').map(lambda l : l.split('::')).map(lambda (uId, mId, r, ts) : (int(mId), float(r)))
MovieRatingsRDD.take(5)
# [(122, 5.0), (185, 5.0), (231, 5.0), (292, 5.0), (316, 5.0)] 

RatingsPerMovieRDD = MovieRatingsRDD.groupByKey()
RatingsPerMovieRDD.take(5)
# [(40962, <pyspark.resultiterable.ResultIterable at 0x7f6ef87a0650>),
# (3, <pyspark.resultiterable.ResultIterable at 0x7f6ef87a0fd0>),
# (8196, <pyspark.resultiterable.ResultIterable at 0x7f6ef87a0f10>),
# (6, <pyspark.resultiterable.ResultIterable at 0x7f6ef87a0dd0>),
# (8199, <pyspark.resultiterable.ResultIterable at 0x7f6ef87a0a50>)]

AvgRatingsRDD = RatingsPerMovieRDD.map(lambda (mId, rs) : (mId, sum(rs) / len(rs)))
AvgRatingsRDD.take(5)
# [(40962, 2.5760869565217392),
#  (3, 3.1605526441162457),
#  (8196, 2.877551020408163),
#  (6, 3.81640625),
#  (8199, 3.9047619047619047)]

movies.join(AvgRatingsRDD).map(lambda (mId, (t, r)) : (t, r)).take(5)
# [(u'Brothers (Br\xf8dre) (2004)', 3.515151515151515),
#  (u"Avventura, L' (a.k.a. Adventure, The) (1960)", 3.934065934065934),
#  (u'Father of the Bride Part II (1995)', 3.0775154004106775),
#  (u'GoldenEye (1995)', 3.427753303964758),
#  (u'Cutthroat Island (1995)', 2.6911177644710578)]

TitlesRatings = movies.join(AvgRatingsRDD).map(lambda (mId, (t, r)) : (t, r)) 
TitlesRatings.takeOrdered(5, key=lambda (t, r): -r)
# [(u'Life of Oharu, The (Saikaku ichidai onna) (1952)', 5.0),
#  (u'Titicut Follies (1967)', 5.0),
#  (u"Money (Argent, L') (1983)", 5.0),
#  (u"Pervert's Guide to Cinema, The (2006)", 5.0),
#  (u'Shanghai Express (1932)', 5.0)]

