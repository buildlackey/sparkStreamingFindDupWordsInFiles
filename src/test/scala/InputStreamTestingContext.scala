import org.apache.spark.SparkContext
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

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.Duration

case class InputStreamTestingContext(sparkContext: SparkContext,
                                     codeBlock: (StreamingContext) => DStream[(String, String)],
                                     testDataGenerationFunc: () => Unit,
                                     pauseDuration: scala.concurrent.duration.Duration,
                                     expected: List[(String, String)],
                                     verboseOutput : Boolean = false) {
  val ssc = new StreamingContext(sparkContext, Seconds(1))

  def run(): Unit = {
    val verifier = InputStreamVerifier[(String,String)](expected.length)
    verifier.runWithStreamingContext(ssc, codeBlock)
    Thread.sleep(pauseDuration.toMillis) // give the code that creates the DStream time to start up
    testDataGenerationFunc()
    verifier.awaitAndVerifyResults(ssc, expected)
  }
}
