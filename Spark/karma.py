karma = (
  sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')
  .map(lambda l : l.split('::'))
  .map(lambda (uId, mId, r, ts) : (int(uId), 1))
  .reduceByKey(lambda r1, r2 : r1 + r2)
  )
  
karma.take(10)

karma.takeOrdered(10, key=lambda (uId, nr): -nr) 

