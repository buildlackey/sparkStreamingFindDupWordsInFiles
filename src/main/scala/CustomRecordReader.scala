import java.io.IOException

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, LineRecordReader}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}


/**
  * Given an input [[FileSplit]] will read each line of the file and return a key/value pair corresponding
  * to each read-in line where the key is a 2-tuple of File name and byte offset of the line, and
  * the value is the String content of the line.
  *
  * This class is the Scala version of the Java class described
  * [[https://halfvim.github.io/2016/06/28/FileInputDStream-in-Spark-Streaming/ here.]]
  *
  */
class CustomRecordReader(conf : Configuration )
  extends RecordReader[Tuple2[Text,LongWritable], Text] with LazyLogging {
  private val lineRecordReader: LineRecordReader = new LineRecordReader
  private var innerValue: Text = null
  private var innerKey: LongWritable = null
  private var key: Tuple2[Text,LongWritable] = null
  private var value: Text = null
  private var filename: String = ""

  def getKeyClass: Class[_] = {
    return classOf[Text]
  }

  @throws(classOf[IOException])
  def initialize(genericSplit: InputSplit, context: TaskAttemptContext) {
    logger.info(s"init of CustomRecordReader with $genericSplit")
    val split: FileSplit = genericSplit.asInstanceOf[FileSplit]
    val file: Path = split.getPath
    this.filename = file.getName
    lineRecordReader.initialize(genericSplit, context)
  }

  @throws(classOf[IOException])
  def nextKeyValue: Boolean = {
    var line: Array[Byte] = null
    if (lineRecordReader.nextKeyValue) {
      innerValue = lineRecordReader.getCurrentValue
      line = innerValue.getBytes

      innerKey = lineRecordReader.getCurrentKey
      if (innerKey == null) {   // Maybe too paranoid.. but can't hurt
        logger.warn(s"null line number key in file $filename at line $innerValue" )
        innerKey = new LongWritable(-1)
      }
    }
    else {
      return false
    }

    if (line == null) {
      return false
    }

    if (key == null) {
      key = (new Text, new LongWritable(0))     // TODO - can't we just init this once ?
    }
    if (value == null) {                        // TODO - can't we just init this once ?
      value = new Text
    }
    this.key._1.set(this.filename)
    this.key._2.set(innerKey.get())
    this.value = innerValue
    true
  }

  def getCurrentKey: Tuple2[Text,LongWritable] = {
    key
  }

  def getCurrentValue: Text = {
    value
  }

  @throws(classOf[IOException])
  def getProgress: Float = {
    lineRecordReader.getProgress
  }

  @throws(classOf[IOException])
  def close {
    lineRecordReader.close
  }
}