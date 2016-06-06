from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF

hashingTF = HashingTF()

step2.take(4)

tf = hashingTF.transform(step2)

tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)
