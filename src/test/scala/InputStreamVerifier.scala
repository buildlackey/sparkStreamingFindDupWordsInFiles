import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import util.{TestResultNotifier, TestResultNotifierFactory}

import scala.concurrent.{Await, Future}

// TODO -  verification against expected results ignore ordering of elements and disregards duplicates
// since the list of expected elements is turned into a set and compared against the actual results (which are
// also converted to a set.
//  

case class InputStreamVerifier[T](numExpectedResults: Int) {
  val notifier: TestResultNotifier[T] = TestResultNotifierFactory.getForNResults(numExpectedResults)

  def runWithStreamingContext(streamingContext: StreamingContext,
                              blockToRun: (StreamingContext) => DStream[T],
                              logLevel: String = "warn"): Unit = {
    val stream: DStream[T] = blockToRun(streamingContext)
    stream.foreachRDD {
      rdd => {
        rdd.foreach { case (item) =>
          notifier.recordResult(item)
        }
      }
    }
    streamingContext.start()
  }

  def awaitAndVerifyResults(streamingContext: StreamingContext, expected: List[T]): Unit = {
    val result: Future[List[T]] = notifier.getResults
    val completedResult: Future[List[T]] =
      Await.ready(result, scala.concurrent.duration.Duration("100 second"))
    val tuples = completedResult.value.get.get.toSet
    println(s"result: $tuples")

    assert(tuples.equals(expected.toSet))
    streamingContext.stop(stopSparkContext = false)
    Thread.sleep(200) // give some time to clean up (SPARK-1603)
  }
}
