import java.lang
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object WordCount {

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
    * This information is returned as a map of
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

    val lines2: DStream[(lang.Long, String)] =
      ssc. fileStream[LongWritable, Text, TextInputFormat](dirPath).
        map { case( longWritable, text) => (new java.lang.Long( longWritable.get() ), text.toString) }
    lines2.print()


    //val lines: DStream[String] = ssc.textFileStream(args(0))
    //val words: DStream[String] = lines.flatMap(_.split(" "))
    /*

    var count = 0
    var lines = ListBuffer[(java.lang.Long, String)]()
    lines.foreachRDD{ (rddOfString: RDD[String]) =>
      rddOfString.foreach { (string: String) =>
        count = count+1
        lines = (count, string)
      }
    }
     */



    ssc.start()
    ssc.awaitTermination()
  }
}

