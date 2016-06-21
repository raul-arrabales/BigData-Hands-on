# On a Databricks CE notebook:

# From dropbox: 
rdd1 = sc.textFile("https://www.dropbox.com/s/6sajovchhfy3cun/quijote.txt?dl=0?raw=1")

# From Tables in Databricks cloud:
rdd1 = sc.textFile("/FileStore/tables/2ndixk251466511975346/quijote.txt")


