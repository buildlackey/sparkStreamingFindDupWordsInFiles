import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.scalatest.{FunSuite, Outcome}


class SampleStreamingTest extends FunSuite with StreamingSuiteBase {

  test("lines in file are converted pairs with filename followed by sequence of all lines in file") {
    val ssc = new StreamingContext(sc, Seconds(10))
    try {
      val finder  = new DupWordFinder(ssc)
      val pairs: DStream[(String, String)] = finder.getNameLinePairsFromFile("/tmp/blah")
      pairs.foreachRDD {
        rdd =>
          rdd foreach { item =>
            println(s"item: $item")
          }
      }
      ssc.start()
      Thread.sleep(12000) // give some time to clean up (SPARK-1603)
    } finally {
      try {
        ssc.stop(stopSparkContext = false)
        Thread.sleep(200) // give some time to clean up (SPARK-1603)
      } catch {
        case e: Exception =>
          logError("Error stopping StreamingContext", e)
      }
    }
  }


  //test("lines in file are converted to WordOccurrence's") { }

  // This is the sample operation we are testing
  def tokenize(f: DStream[String]): DStream[String] = {
    f.flatMap(_.split(" "))
  }


  protected def withFixture(test: Any): Outcome = ???
}
