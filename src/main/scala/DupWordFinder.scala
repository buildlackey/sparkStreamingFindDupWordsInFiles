import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import java.io.{File, Serializable}
import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.typesafe.config.ConfigFactory


class DupWordFinder(ssc : StreamingContext) {
  def getNameLinePairsFromFile(dirPath: String) : DStream[(String /*filename*/ , String /*line*/ )] =
    ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
      map {
        case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
          (fileName.toString, lineText.toString)
      }
}
