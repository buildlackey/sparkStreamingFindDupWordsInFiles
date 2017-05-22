import java.io._
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator
import org.scalatest.{FunSuite, Outcome}
import util.{TestResultNotifier, TestResultNotifierFactory}

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}


class TextFileContentsWithFileNameSpec extends FunSuite with StreamingSuiteBase {

  var ssc: StreamingContext = null

  override def useManualClock: Boolean = false // doesn't seem to be respected, but leaving in to show intent

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

    val accumCounter: LongAccumulator = sc.longAccumulator("counter")


    invoker.runWithStreamingContext(ssc, block, 2, accumCounter )

    Thread.sleep(1500) // give the code that creates the DStream time to start up
    new PrintWriter(dirPath + "/some.input.for.the.test1") {
      write("moo cow\brown cow"); close()
    }
    new PrintWriter(dirPath + "/some.input.for.the.test2") {
      write("moo cow\brown cow"); close()
    }
    invoker.awaitAndVerifyResults(ssc)
  }
}


case class InputStreamVerifier() {
  val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.getForNResults(2)

  def runWithStreamingContext(streamingContext: StreamingContext,
                              blockToRun: (StreamingContext) => DStream[(String, String)],
                              numExpectedOutputs: Int,
                             counter : LongAccumulator ,
                              logLevel: String = "warn"): Unit = {
    val stream: DStream[(String, String)] = blockToRun(streamingContext)
    stream.foreachRDD {
      rdd => {
        rdd.foreach { case ((fileName, lineContent)) =>
          notifier.recordResult((fileName, lineContent))
        }
      }
    }
    streamingContext.start()
  }


  def awaitAndVerifyResults(streamingContext: StreamingContext): Unit = {
    val result: Future[List[(String, String)]] = notifier.getResults
    val completedResult: Future[List[(String, String)]] =
      Await.ready(result, scala.concurrent.duration.Duration("100 second"))
    println(s"result: ${completedResult.value.get.get}")
    streamingContext.stop(stopSparkContext = false)
    Thread.sleep(200) // give some time to clean up (SPARK-1603)
  }
}





