import java.io._
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.scalatest.{FunSuite, Outcome}
import scala.collection.mutable.ListBuffer





class NonWorkingTest extends FunSuite with StreamingSuiteBase {
  var ssc: StreamingContext = null

  //tmpFile.deleteOnExit()

  override def useManualClock: Boolean = false      // doesn't seem to be respected, but leaving in to show intent

  override def conf = {
    val confToTweak = super.conf
    confToTweak.set("spark.streaming.clock", "org.apache.spark.streaming.util.SystemClock")
    confToTweak
  }


  def withFixture(test: Any): Outcome = ???

  test("lines in file are converted pairs with filename followed by sequence of all lines in file") {
    var dirPath = "/tmp/blah"
    val dir: File = new File(dirPath)
    FileUtils.deleteDirectory(dir);
    dir.mkdir();

    ssc = new StreamingContext(sc, Seconds(1))
    val invoker = new TestInvoker(ssc)
    invoker.invokeTest()
  }



}

object ResultStreamSource {
  val tmpFile: File = File.createTempFile("spark-streaming", "unit-test")
  println(s"file  is ${tmpFile.getAbsolutePath}")
  
  def getOutputStream: ObjectOutputStream = {
    val fos = new FileOutputStream(tmpFile.getAbsolutePath)
    val oos = new ObjectOutputStream(fos);
    oos
  }


  def getInputStream: ObjectInputStream = {
    val ois = new ObjectInputStream(new FileInputStream(tmpFile))
    ois
  }
}


case class TestInvoker(streamingContext : StreamingContext) {


  def withStreamingContext(batchDuration: Duration,
                           blockToRun: (StreamingContext) => DStream[(String, String)],
                           numExpectedOutputs : Int,
                           logLevel: String = "warn"): Unit = {
    val stream: DStream[(String, String)] = blockToRun(streamingContext)
     stream.foreachRDD {
      rdd => {
        rdd.zipWithIndex().foreach { case ((fileName, lineContent), count: Long) =>
          val oos = ResultStreamSource.getOutputStream
          println(s"filename: $fileName")
          println(s"lineContent: $lineContent")
          oos.writeObject((fileName,lineContent))
          if (count == (numExpectedOutputs-1)) {
            oos.close()
            println("CLOSED")
          }
        }

      }
    }
  }


  def invokeTest(): Unit = {
    var dirPath = "/tmp/blah"     // defined in 2 places !  TODO  - fix

    val block: (StreamingContext) => DStream[(String, String)] = {
      (ssc: StreamingContext) =>
        val fileNameLines: DStream[(String /*filename*/ , String /*line*/ )] =
          ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
            map {
              case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
                (fileName.toString, lineText.toString)
            }
        fileNameLines
    }

    withStreamingContext(Seconds(1), block, 2)

    streamingContext.start()
    println("BEGIN WAIT")

    Thread.sleep(9000)
    println("done...")
    //println(s"got thing 2: ${readResults()}")
    streamingContext.stop(stopSparkContext = false)
    Thread.sleep(200) // give some time to clean up (SPARK-1603)
  }

  def readResults(): List[(String, String)] = {
    val ois = ResultStreamSource.getInputStream
    val buf: ListBuffer[(String, String)] =  new ListBuffer[(String, String)]()

    (1 to 2).foreach { i =>
      val tuple = ois.readObject().asInstanceOf[Tuple2[String, String]] ;
      println(s"read item from file: $tuple")
      buf += tuple
    }

    ois.close()
    buf.toList
  }
}





