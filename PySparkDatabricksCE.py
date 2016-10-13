# On a Databricks CE notebook:

# From dropbox: 
rdd1 = sc.textFile("https://www.dropbox.com/s/6sajovchhfy3cun/quijote.txt?dl=0?raw=1")

# From Tables in Databricks cloud:
rdd1 = sc.textFile("/FileStore/tables/2ndixk251466511975346/quijote.txt")

wordcounts = rdd1.map( lambda x: x.replace(',',' ').replace('.',' ').replace('-',' ').lower()) \
        .flatMap(lambda x: x.split()) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x,y:x+y) \
        .map(lambda x:(x[1],x[0])) \
        .sortByKey(False)

wordcounts.take(10)
