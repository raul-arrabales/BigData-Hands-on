
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

val config = new SparkConf().setAppName("twitter-stream-sentiment")

val sc = new SparkContext(config)
sc.setLogLevel("WARN")
 
val ssc = new StreamingContext(sc, Seconds(5))
 
System.setProperty("twitter4j.oauth.consumerKey", "consumerKey")
System.setProperty("twitter4j.oauth.consumerSecret", "consumerSecret")
System.setProperty("twitter4j.oauth.accessToken", accessToken)
System.setProperty("twitter4j.oauth.accessTokenSecret", "accessTokenSecret")
 
val stream = TwitterUtils.createStream(ssc, None)

val tags = stream.flatMap { status => status.getHashtagEntities.map(_.getText)
}

tags
    .countByValue()
    .foreachRDD { rdd =>
        
        val now = org.joda.time.DateTime.now()
        rdd
          .sortBy(_._2)
          .map(x => (x, now))
          .saveAsTextFile(s"~/twitter/$now")
      }
