import java.io._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.receiver.{InputStreamSuiteBase, InputStreamTestingContext}


class TextFileContentsWithFileNameSpec extends FunSuite with InputStreamSuiteBase {

  test("lines in file are represented as pairs consisting of 'filename' followed by 'sequence of all lines in file'") {
    var dirPath = "/tmp/blah"
    val dir: File = new File(dirPath)
    FileUtils.deleteDirectory(dir)
    dir.mkdir()

    val dstreamCreationFunc: (StreamingContext) => DStream[(String, String)] = {
      (ssc: StreamingContext) =>
        val fileNameLines: DStream[(String /*filename*/ , String /*line*/ )] =
          ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
            map {
              case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
                (fileName.toString, lineText.toString)
            }
        fileNameLines
    }

    val testDataGenerationFunc: () => Unit = {
      () =>
        new PrintWriter(dirPath + "/test1") {
          write("moo cow\nbrown cow");
          close()
        }
        new PrintWriter(dirPath + "/test2") {
          write("moo cow\nbrown cow");
          close()
        }
        ()
    }


    val expectedResult =
      List(("test1", "moo cow"), ("test2", "moo cow"), ("test1", "brown cow"), ("test2", "brown cow"))

    InputStreamTestingContext(
        sparkContext = sc,
        dStreamCreationFunc = dstreamCreationFunc,
        testDataGenerationFunc =   testDataGenerationFunc,
        pauseDuration = scala.concurrent.duration.Duration("1500 milliseconds"),
        expectedResult = expectedResult)
        .run()
  }
}








