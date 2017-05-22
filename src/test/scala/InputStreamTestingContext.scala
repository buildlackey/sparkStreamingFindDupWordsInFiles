import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.Duration

case class InputStreamTestingContext(codeBlock: (StreamingContext) => DStream[(String, String)],
                                     testDataGenerationFunc: () => Unit,
                                     duration: Duration,
                                     expected: List[(String, String)],
                                     b: Boolean) { // mine

}
