
/**
 * Reading a stream from Twitter
 * 
 */
 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter._

val config = new SparkConf().setAppName("twitter-stream")
val sc = new SparkContext(config)

# In DC Cloud sc is already created:
# sc.getConf.setAppName("twitter-stream")

sc.setLogLevel("WARN")
 
# Setting batched of 5 seconds
val ssc = new StreamingContext(sc, Seconds(5))
 
# Fill in your API keys
System.setProperty("twitter4j.oauth.consumerKey", "consumerKey")
System.setProperty("twitter4j.oauth.consumerSecret", "consumerSecret")
System.setProperty("twitter4j.oauth.accessToken", "accessToken")
System.setProperty("twitter4j.oauth.accessTokenSecret", "accessTokenSecret")
 
val stream = TwitterUtils.createStream(ssc, None)

val statuses = stream.map(status => status.getText())

val filtered = statuses.filter(status => status contains "big data")

statuses.print()

ssc.start()
ssc.awaitTermination()
 
