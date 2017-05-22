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

import scala.collection.immutable
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
    FileUtils.deleteDirectory(dir)
    dir.mkdir()

    ssc = new StreamingContext(sc, Seconds(1))

    val codeBlock: (StreamingContext) => DStream[(String, String)] = {
      (ssc: StreamingContext) =>
        val fileNameLines: DStream[(String /*filename*/ , String /*line*/ )] =
          ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
            map {
              case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
                (fileName.toString, lineText.toString)
            }
        fileNameLines
    }

    val verifier = InputStreamVerifier[(String,String)](2)
    verifier.runWithStreamingContext(ssc, codeBlock)

    Thread.sleep(1500) // give the code that creates the DStream time to start up

    val testDataGenerationFunc: () => Unit = {
      () =>
        new PrintWriter(dirPath + "/test1") {
          write("moo cow\nbrown cow");
          close()
        }
        new PrintWriter(dirPath + "/test2") {
          write("moo cow\nbrown cow");
          close()
        }
        ()
    }

    testDataGenerationFunc()

    val expected = List(("test1", "moo cow"), ("test2", "moo cow"), ("test1", "brown cow"), ("test2", "brown cow"))
    verifier.awaitAndVerifyResults(ssc, expected)


    InputStreamTestingContext(
      codeBlock,
      testDataGenerationFunc,
      scala.concurrent.duration.Duration("1500 milliseconds"),
      expected,
      false)

  }
}








