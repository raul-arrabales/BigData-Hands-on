/**
 * Term Frequency example with El Quijote
 * 
 */

// Read the text file (I didn't use a .txt extension when I put the file to HDFS)
val textFile = sc.textFile("quijotedata")

// Split lines by spaces
val wordCounts = textFile.flatMap(line => line.split(“ “))

// Filter out zero length segments
val filterWords = wordCounts.filter(word => word.length() > 0)

// Getting a map for the count of each word 
val mapWords = filterWords.map(word => (word, 1))

// Reduce the counts by word
val countWords = mapWords.reduceByKey((a,b) => a + b )

// Sort alphabetically by words
val countWordsSort = countWords.sortByKey()

// Save the TF table
countWordsSort.saveAsTextFile(“wc-output”)


/**
 * Term Frequency example with El Quijote
 * With Databrick CE Cloud mini-cluster
 * 
 */
 val textFile = sc.textFile("/FileStore/tables/zv0aggtc1466847515469/quijote_complete.txt")
 val wordCounts = textFile.flatMap(line => line.split(" "))
 wordCounts.count()
 val filterWords = wordCounts.filter(word => word.length() > 0)
 filterWords.count()
 val mapWords = filterWords.map(word => (word, 1))
 mapWords.take(100)
 val countWords = mapWords.reduceByKey((a,b) => a + b )
 countWords.take(30)
 countWords.count()
 val countWordsSort = countWords.sortByKey(ascending=true)
 countWordsSort.take(10)
 
 # Sorting by frequency:
 val inverted = countWordsSort.map( item => item.swap ).sortByKey(ascending=false)
 inverted.take(10).foreach(a => println(a)) 
 
 # Or:
 countWordsSort.map( item => item.swap ).sortByKey(ascending=false).take(20).foreach(w => println(w)) 
 
 
