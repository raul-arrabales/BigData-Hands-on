/**
 * This is a hello world example of Spark Streaming which counts words on 1 second batches of streaming data.
 * It uses an in-memory string generator as a dummy source for streaming data.
 */
 
 // === Configuration to control the flow of the application ===
val stopActiveContext = true	 
// "true"  = stop if any existing StreamingContext is running;              
// "false" = dont stop, and let it run undisturbed, but your latest code may not be used

// === Configurations for Spark Streaming ===
val batchIntervalSeconds = 1 
val eventsPerSecond = 1000    // For the dummy source

// Verify that the attached Spark cluster is 1.4.0+
require(sc.version >= "1.4.0", "Spark 1.4.0+ is required to run this notebook. Please attach it to a Spark 1.4.0+ cluster.")

import org.apache.spark._
import org.apache.spark.storage._
import org.apache.spark.streaming._


// This is the dummy source implemented as a custom receiver. No need to understand this.

import scala.util.Random
import org.apache.spark.streaming.receiver._

class DummySource(ratePerSec: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Dummy Source") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
   // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    while(!isStopped()) {      
      store("I am a dummy source " + Random.nextInt(10))
      Thread.sleep((1000.toDouble / ratePerSec).toInt)
    }
  }
}

var newContextCreated = false      // Flag to detect whether new context was created or not

// Function to create a new StreamingContext and set it up
def creatingFunc(): StreamingContext = {
    
  // Create a StreamingContext
  val ssc = new StreamingContext(sc, Seconds(batchIntervalSeconds))
  
  // Create a stream that generates 1000 lines per second
  val stream = ssc.receiverStream(new DummySource(eventsPerSecond))  
  
  // Split the lines into words, and then do word count
  val wordStream = stream.flatMap { _.split(" ")  }
  val wordCountStream = wordStream.map(word => (word, 1)).reduceByKey(_ + _)

  // Create temp table at every batch interval
  wordCountStream.foreachRDD { rdd => 
    rdd.toDF("word", "count").registerTempTable("batch_word_count")    
  }
  
  stream.foreachRDD { rdd =>
    System.out.println("# events = " + rdd.count())
    System.out.println("\t " + rdd.take(10).mkString(", ") + ", ...")
  }
  
  ssc.remember(Minutes(1))  // To make sure data is not deleted by the time we query it interactively
  
  println("Creating function called to create new StreamingContext")
  newContextCreated = true  
  ssc
}

// Stop any existing StreamingContext 
if (stopActiveContext) {	
  StreamingContext.getActive.foreach { _.stop(stopSparkContext = false) }
} 

// Get or create a streaming context
val ssc = StreamingContext.getActiveOrCreate(creatingFunc)
if (newContextCreated) {
  println("New context created from currently defined creating function") 
} else {
  println("Existing context running or recovered from checkpoint, may not be running currently defined creating function")
}

// Start the streaming context in the background.
ssc.start()

// This is to ensure that we wait for some time before the background streaming job starts. This will put this cell on hold for 5 times the batchIntervalSeconds.
ssc.awaitTerminationOrTimeout(batchIntervalSeconds * 5 * 1000)


%sql select * from batch_word_count

