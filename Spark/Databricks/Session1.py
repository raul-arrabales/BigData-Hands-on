
# Loading CSV files from DBFS into RDDs in cluster memory
moviesRDD = sc.textFile('/FileStore/tables/y81un4d91488113985408/movies.dat')
ratingsRDD = sc.textFile('/FileStore/tables/p1ab1wsy1488114436633/ratings_small.dat')

