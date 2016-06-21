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

