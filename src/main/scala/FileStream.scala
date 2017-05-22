import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


object FileStream {

  /**
    * Read a file  of text (one sentence per line), return the line numbers
    * of the words  that occurs more than once (the duplicates).  I am assuming
    * that a word is considered to be a duplicate if it occurs elsewhere in the
    * file, not just on the same line.
    *
    * The problem statement did not specify where the file comes from, so I have
    * implemented the solution using textFileStream which allows users of this
    * job to drop whatever file (or files) they want in the directory passed in
    * as the first argument to main().
    *
    *
    * Also finally return the total occurrences of those duplicated words at the end of 10 minutes.
    */
  def main(args: Array[String]) {
    var dirPath =
      if (args.length >= 1) {
        args(0)
      } else {
        "/tmp/files"
      }


    def updateFunction(values: Seq[Int], runningCount: Option[Int]) = {
      val newCount = values.sum + runningCount.getOrElse(0)
      new Some(newCount)
    }

    val sparkConf = new SparkConf().setAppName("continuous").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lineStream: DStream[String] = ssc.textFileStream(dirPath)
    lineStream.foreachRDD { (lineRDD: RDD[String]) =>
      println(s"count: ${lineRDD.count()}")
      lineRDD.foreach {
        (arg: String) =>
          println("line" + arg)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }


}
