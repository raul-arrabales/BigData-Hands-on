
/**
 * Reading a stream from Twitter using DBCE
 * From: https://docs.cloud.databricks.com/docs/latest/databricks_guide/index.html#07%20Spark%20Streaming/03%20Twitter%20Hashtag%20Count%20-%20Scala.html 
 */
 
// Spark imports
import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils

// Scala math util
import scala.math.Ordering

// Twitter4j
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder


// Enter your own twitter app tokens
System.setProperty("twitter4j.oauth.consumerKey", "XXXXXXXXXXXXXXXXXXXXXXXXXX")
System.setProperty("twitter4j.oauth.consumerSecret", "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
System.setProperty("twitter4j.oauth.accessToken", "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
System.setProperty("twitter4j.oauth.accessTokenSecret", "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

// DBFS Directory to output top hashtags
val outputDirectory = "/twitter"

// Recompute the top hashtags every 1 second
val slideInterval = new Duration(1 * 1000)

// Compute the top hashtags for the last 5 seconds
val windowLength = new Duration(5 * 1000)

// Wait this many seconds before stopping the streaming job
val timeoutJobLength = 10 * 1000

// delete old files from previous executions
dbutils.fs.rm(outputDirectory, true) 

var newContextCreated = false
var num = 0

// This is a helper class used for 
object SecondValueOrdering extends Ordering[(String, Int)] {
  def compare(a: (String, Int), b: (String, Int)) = {
    a._2 compare b._2
  }
}

// This is the function that creates the SteamingContext and sets up the Spark Streaming job.
def creatingFunc(): StreamingContext = {
  // Create a Spark Streaming Context.
  val ssc = new StreamingContext(sc, slideInterval)
  // Create a Twitter Stream for the input source. 
  val auth = Some(new OAuthAuthorization(new ConfigurationBuilder().build()))
  val twitterStream = TwitterUtils.createStream(ssc, auth)
  
  // Parse the tweets and gather the hashTags.
  val hashTagStream = twitterStream.map(_.getText).flatMap(_.split(" ")).filter(_.startsWith("#"))
  
  // Compute the counts of each hashtag by window.
  val windowedhashTagCountStream = hashTagStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, windowLength, slideInterval)

  // For each window, calculate the top hashtags for that time period.
  windowedhashTagCountStream.foreachRDD(hashTagCountRDD => {
    val topEndpoints = hashTagCountRDD.top(10)(SecondValueOrdering)
    dbutils.fs.put(s"${outputDirectory}/top_hashtags_${num}", topEndpoints.mkString("\n"), true)
    println(s"------ TOP HASHTAGS For window ${num}")
    println(topEndpoints.mkString("\n"))
    num = num + 1
  })
  
  newContextCreated = true
  ssc
}

// Create the StreamingContext using getActiveOrCreate, as required when starting a streaming job in Databricks.
@transient val ssc = StreamingContext.getActiveOrCreate(creatingFunc) 

// Start the Spark Streaming Context and return when the Streaming job exits or return with the specified timeout.
ssc.start()
ssc.awaitTerminationOrTimeout(timeoutJobLength) 

// Stop any active Streaming Contexts, but don't stop the spark contexts they are attached to.
StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) } 

// See the files created for each window
display(dbutils.fs.ls(outputDirectory))

// And see the results for each file:
dbutils.fs.head(s"${outputDirectory}/top_hashtags_132") 
