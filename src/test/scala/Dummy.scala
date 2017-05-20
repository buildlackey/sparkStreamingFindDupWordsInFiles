import java.io._
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}

import com.holdenkarau.spark.testing.StreamingSuiteBase
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.scalatest.{FunSuite, Outcome}
import scala.collection.mutable.ListBuffer


class UnderTest extends FunSuite with StreamingSuiteBase {
  val tmpFile: File = File.createTempFile("spark-streaming", "unit-test")
  val fos = new FileOutputStream(tmpFile.getAbsolutePath)
  val oos = new ObjectOutputStream(fos);
  val latch = new CountDownLatch(1)
  var ssc: StreamingContext = null

  tmpFile.deleteOnExit()



  def withFixture(test: Any): Outcome = ???

  test("lines in file are converted pairs with filename followed by sequence of all lines in file") {
    var dirPath = "/tmp/blah"
    val dir: File = new File(dirPath)
    FileUtils.deleteDirectory(dir);
    dir.mkdir();


    ssc = new StreamingContext(sc, Seconds(1))

    val block: (StreamingContext) => DStream[(String, String)] = {
      (ssc: StreamingContext) =>
        val fileNameLines: DStream[(String /*filename*/ , String /*line*/ )] =
          ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
            map {
              case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
                (fileName.toString, lineText.toString)
            }
        fileNameLines
    }

    withStreamingContext(Seconds(1), block, 2)

    ssc.start()
    println("BEGIN WAIT")
    latch.await()
    println(s"got thing 2: ${readResults()}")
    ssc.stop(stopSparkContext = false)
    Thread.sleep(200) // give some time to clean up (SPARK-1603)
  }

  def readResults(): List[(String, String)] = {
    val ois = new ObjectInputStream(new FileInputStream(tmpFile))
    val buf: ListBuffer[(String, String)] =  new ListBuffer[(String, String)]()

    (1 to 2).foreach { i =>
      val tuple = ois.readObject().asInstanceOf[Tuple2[String, String]] ;
      println(s"read item from file: $tuple")
      buf += tuple
    }

    ois.close()
    buf.toList
  }




  def withStreamingContext(batchDuration: Duration,
                           blockToRun: (StreamingContext) => DStream[(String, String)],
                           numExpectedOutputs : Int,
                           logLevel: String = "warn"): Unit = {



    val stream: DStream[(String, String)] = blockToRun(ssc)

    stream.foreachRDD {
      rdd =>
        rdd.zipWithIndex().foreach { case ((fileName, lineContent), count: Long) =>
          println(s"filename: $fileName")
          println(s"lineContent: $lineContent")
          oos.writeObject((fileName,lineContent))
          if (count == (numExpectedOutputs-1)) {
            oos.close()
            println("CLOSED")
            latch.countDown()
          }
        }
    }

  }

  /*

  def main(args: Array[String]): Unit = {
    var dirPath = "/tmp/blah"
    val dir: File = new File(dirPath)
    FileUtils.deleteDirectory(dir);
    dir.mkdir();

    val block: (StreamingContext) => DStream[(String, String)] = {
      (ssc: StreamingContext) =>
        val fileNameLines: DStream[(String /*filename*/ , String /*line*/ )] =
          ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
            map {
              case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
                (fileName.toString, lineText.toString)
            }
        fileNameLines
    }

    withStreamingContext(Seconds(1), block, 2)
  }


  def rain(ssc: StreamingContext) {
    var dirPath = "/tmp/blah"
    val dir: File = new File(dirPath)
    FileUtils.deleteDirectory(dir);
    dir.mkdir();


    val fileNameLines: DStream[(String /*filename*/ , String /*line*/ )] =
      ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
        map {
          case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
            (fileName.toString, lineText.toString)
        }

    fileNameLines.foreachRDD {
      rdd =>
        rdd foreach { item =>
          println(s"item: $item")
        }
    }
    ssc.start()
    ssc.awaitTermination()
  }
   */

}

