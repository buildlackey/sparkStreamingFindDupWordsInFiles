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
import util.{TestResultNotifier, TestResultNotifierFactory}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}



class TextFileContentsWithFileNameSpec extends FunSuite with StreamingSuiteBase {

  var ssc: StreamingContext = null

  override def useManualClock: Boolean = false      // doesn't seem to be respected, but leaving in to show intent

  override def conf = {
    val confToTweak = super.conf
    confToTweak.set("spark.streaming.clock", "org.apache.spark.streaming.util.SystemClock")
    confToTweak
  }


  def withFixture(test: Any): Outcome = ???

  test("lines in file are represented as pairs consisting of 'filename' followed by 'sequence of all lines in file'") {
    var dirPath = "/tmp/blah"
    val dir: File = new File(dirPath)
    FileUtils.deleteDirectory(dir);
    dir.mkdir();

    ssc = new StreamingContext(sc, Seconds(1))
    val invoker = new InputStreamVerifier()
    invoker.invokeTest(ssc)
  }
}


case class InputStreamVerifier() {
  val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.get()

  def withStreamingContext(streamingContext : StreamingContext,
                           blockToRun: (StreamingContext) => DStream[(String, String)],
                           numExpectedOutputs : Int,
                           logLevel: String = "warn"): Unit = {
    val stream: DStream[(String, String)] = blockToRun(streamingContext)
    val buf: ListBuffer[(String, String)] = new ListBuffer[(String, String)]()
     stream.foreachRDD {                                                              // TODO - make work across RDD's
      rdd => {
        rdd.zipWithIndex().foreach { case ((fileName, lineContent), count: Long) =>
          println(s"filename: $fileName")
          println(s"lineContent: $lineContent")
          buf += ((fileName, lineContent))
          if (count == (numExpectedOutputs-1)) {
            notifier.recordResults(buf.toList)
            println("CLOSED")
          }
        }
      }
    }
  }

  def invokeTest(streamingContext : StreamingContext): Unit = {
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

    withStreamingContext(streamingContext, block, 2)
    streamingContext.start()
    println("BEGIN WAIT")

    val result: Future[List[(String, String)]] = notifier.getResults
    val completedResult: Future[List[(String, String)]] =
      Await.ready(result, scala.concurrent.duration.Duration("100 second"))
    println(s"result: ${completedResult.value.get.get  }")

    println("done...")
    //println(s"got thing 2: ${readResults()}")
    streamingContext.stop(stopSparkContext = false)
    Thread.sleep(200) // give some time to clean up (SPARK-1603)
  }
}





