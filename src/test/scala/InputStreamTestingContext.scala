import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

case class InputStreamTestingContext[T](sparkContext: SparkContext,
                                        codeBlock: (StreamingContext) => DStream[T],
                                        testDataGenerationFunc: () => Unit,
                                        pauseDuration: scala.concurrent.duration.Duration,
                                        expected: List[T],
                                        verboseOutput: Boolean = false) {
  val ssc = new StreamingContext(sparkContext, Seconds(1))

  def run(): Unit = {
    ssc.sparkContext.setLogLevel(if (verboseOutput) "info" else "warn")
    val verifier = InputStreamVerifier[T](expected.length)
    verifier.runWithStreamingContext(ssc, codeBlock)
    Thread.sleep(pauseDuration.toMillis) // give the code that creates the DStream time to start up
    testDataGenerationFunc()
    verifier.awaitAndVerifyResults(ssc, expected)
  }
}
