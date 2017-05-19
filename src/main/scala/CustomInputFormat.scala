import java.io.IOException

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

class CustomInputFormat extends FileInputFormat[Tuple2[Text,LongWritable], Text] {
  @throws(classOf[IOException])
  def createRecordReader(genericSplit: InputSplit, context: TaskAttemptContext): RecordReader[Tuple2[Text,LongWritable], Text] = {
    context.setStatus(genericSplit.toString)
    new CustomRecordReader(context.getConfiguration)
  }
}