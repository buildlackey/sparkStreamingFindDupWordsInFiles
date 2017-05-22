import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream


class DupWordFinder(ssc : StreamingContext) {
  def getNameLinePairsFromFile(dirPath: String) : DStream[(String /*filename*/ , String /*line*/ )] =
    ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
      map {
        case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
          (fileName.toString, lineText.toString)
      }
}
