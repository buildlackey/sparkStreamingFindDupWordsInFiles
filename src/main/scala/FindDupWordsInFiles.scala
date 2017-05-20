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


case class WordOccurrence(word: String, foundInFile: String, atLine: Long)

object FindDupWordsInFiles {
  def main(args: Array[String]) {
    var dirPath =
      if (args.length >= 1) {
        args(0)
      } else {
        "/tmp/files"
      }
    val dir: File = new File(dirPath)
    FileUtils.deleteDirectory(dir);
    dir.mkdir();

    val conf = ConfigFactory.load
    val sparkConf = new SparkConf().setAppName("continuous").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.sparkContext.setLogLevel(conf.getString("sparkContextLogLevel"))

    val fileNameLines: DStream[(String /*filename*/ , String /*line*/ )] =
      ssc.fileStream[Tuple2[Text, LongWritable], Text, CustomInputFormat](dirPath).
        map {
          case ((fileName: Text, lineNum: LongWritable), lineText: Text) =>
            (fileName.toString, lineText.toString)
        }

    val linesGroupedByFileName: DStream[(String, Iterable[String])] = fileNameLines.groupByKey()

    val fileNameLineLineNumber: DStream[(String /*fileName*/ , String /*line*/ , Int /*lineNumber*/ )] =
      linesGroupedByFileName.flatMap {
        case (fileName: String, lineList: Iterable[String]) =>
          (lineList zip (Stream from 1)).map {
            case (line: String, number: Int) => (fileName, line, number)
          }
      }

    val wordOccurrences: DStream[WordOccurrence] = fileNameLineLineNumber.flatMap {
      case (fileName, line, lineNumber) =>
        line.split("[^\\w']+")
          .map {
            WordOccurrence(_, fileName, lineNumber)
          }
    }

    val countsAndOccurrencesByWord: DStream[(String, (Int, List[WordOccurrence]))] =
      wordOccurrences.transform { (rdd: RDD[WordOccurrence]) =>
        val occurrencesByWord: RDD[(String, WordOccurrence)] = rdd.map { (wo: WordOccurrence) => (wo.word, wo) }
        val zero: (Int, List[WordOccurrence]) = (0, List[WordOccurrence]())
        val itemCombiner: ((Int, List[WordOccurrence]), WordOccurrence) => (Int, List[WordOccurrence]) = {
          (countAndListArgs: (Int, List[WordOccurrence]), occurrence: WordOccurrence) => // arguments to combiner
            val count: Int = countAndListArgs._1
            val wordOccurences: List[WordOccurrence] = countAndListArgs._2
            (count + 1, List(occurrence) ::: wordOccurences) // add 1 to count for word, append to list of occurrences
        }
        val resultCombiner: ((Int, List[WordOccurrence]), (Int, List[WordOccurrence])) => (Int, List[WordOccurrence]) = {
          (result1: (Int, List[WordOccurrence]), result2: (Int, List[WordOccurrence])) =>
            (result1._1 + result2._1, result1._2 ::: result2._2)
        }
        occurrencesByWord.aggregateByKey(zero)(itemCombiner, resultCombiner)
      }

    val countsAndOccurrencesOfDupsOnlyByWord: DStream[(String, (Int, List[WordOccurrence]))] =
      countsAndOccurrencesByWord.filter { case (word, (count, occurrences)) => count > 1 }  // filter out non dups

    countsAndOccurrencesOfDupsOnlyByWord.foreachRDD {
      (rdd: RDD[(String, (Int, List[WordOccurrence]))]) =>
        val date: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
        rdd foreach { item =>
          println(s"$date: minor interval line: $item")
        }
    }

    val countsByWord: DStream[(String, Int)] =
      countsAndOccurrencesOfDupsOnlyByWord.map { case (w, (count, occurences)) => (w, count) }
    val majorIntervalSummary = countsByWord.reduceByKeyAndWindow({ (x: Int, y: Int) => x + y }, Seconds(15), Seconds(15))

    majorIntervalSummary.foreachRDD {
      (rdd: RDD[(String, Int)]) =>
        val date: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
        rdd foreach { (item: (String, Int)) =>
          println(s"$date: major interval line: $item")
        }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

