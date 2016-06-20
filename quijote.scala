val textFile = sc.textFile("quijotedata")

val wordCounts = textFile.flatMap(line => line.split(“ “))

val filterWords = wordCounts.filter(word => word.length() > 0)

val mapWords = filterWords.map(word => (word, 1))

val countWords = mapWords.reduceByKey((a,b) => a + b )

val countWordsSort = countWords.sortByKey()

countWordsSort.saveAsTextFile(“wc-output”)

