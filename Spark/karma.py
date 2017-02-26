# Using Movielens dataset: https://grouplens.org/datasets/movielens/ 

# ratings.dat file coontaining:
# uIDd: User Id
# mId: Movie Id
# r: Ratings
# ts: Timestamp

# File stored in Databricks community cloud (small version):
# /FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat


karmaRDD = (
  sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (uId, mId, r, ts) : (int(uId), 1))
  .reduceByKey(lambda r1, r2 : r1 + r2)
  )
  
karma.take(10)

karma.takeOrdered(10, key=lambda (uId, nr): -nr) 

# Step by step explanation:

RecordsRDD = sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')
RecordsRDD.take(5) 
# [u'1::122::5::838985046',
#  u'1::185::5::838983525',
# u'1::231::5::838983392',
#  u'1::292::5::838983421',
# u'1::316::5::838983392']

SplittedRDD = RecordsRDD.map(lambda l : l.split('::'))
SplittedRDD.take(5)
# [[u'1', u'122', u'5', u'838985046'],
#  [u'1', u'185', u'5', u'838983525'],
#  [u'1', u'231', u'5', u'838983392'],
#  [u'1', u'292', u'5', u'838983421'],
#  [u'1', u'316', u'5', u'838983392']]

CountsRDD = SplittedRDD.map(lambda (uId, mId, r, ts) : (int(uId), 1))
CountsRDD.take(5)
# [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1)

SumRDD = CountsRDD.reduceByKey(lambda r1, r2 : r1 + r2)
SumRDD.take(5)
# [(32769, 39), (3, 33), (32772, 34), (6, 42), (32775, 42)]

SumRDD.takeOrdered(5, key=lambda (uId, nr): -nr)
# [(14463, 5169), (27468, 4449), (19635, 4165), (3817, 4165), (27584, 3479)] 

