package util

import java.io._
import java.nio.file.Path
import java.util.UUID

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global


class TestResultNotifier[T](val resultsDirPath: String,
                            val numExpectedResults: Int) extends LazyLogging  with Serializable {
  private[util] val resultsDataFilePath: String = resultsDirPath + File.separator + "results.dat"
  private[util] val doneSentinelFilePath: String = resultsDirPath + File.separator + "results.done"

  def countOfResultsEqualsExpected(resultsDirPath: String): Boolean = {
    val foundList = new File(resultsDirPath).listFiles.filter{
      file =>
        println(s"${Thread.currentThread().getName} -- examinging: $file")
        file.isFile
    }.toList

    foundList.length >= numExpectedResults
  }

  def recordResult(result: T): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    logger.info(s"TestResultNotifier recording results: $result")

    try {
      val fos = new FileOutputStream(new File (resultsDataFilePath +  UUID.randomUUID().toString))
      val oos = new ObjectOutputStream(fos);
      oos.writeObject(result)
      oos.close()
      if (countOfResultsEqualsExpected(resultsDirPath)) {
        new File (doneSentinelFilePath).mkdir()   // sentinel file: marks as 'done'
      }
    } catch {
      case  e: Throwable=>
        throw new RuntimeException(s"util.TestResultNotifier could not record result: $result", e)
    }
  }

  def getResults : Future[List[T]] = {
    val promise = Promise[List[T]]()
    val sentinelFile = new File(doneSentinelFilePath)
    val logger = LoggerFactory.getLogger(getClass)

    Future {
      var results : List[T] =  null
      try {
        while (results == null) {
          if (sentinelFile.exists()) {
            val buffer = new ListBuffer[T]()
            val filesToCheck = new File(resultsDirPath).listFiles. filter { f => ! f.getName.endsWith("done") }.toList
            logger.info(s"checking files: $filesToCheck")
            for (file: File <- filesToCheck) {
              val ois = new ObjectInputStream(new FileInputStream(file.getAbsolutePath))
              val result = ois.readObject().asInstanceOf[T]
              buffer += result
              ois.close()
            }
            results = buffer.toList
            logger.info(s"returning results: $results")
            FileUtils.deleteDirectory(new File(resultsDirPath));
          } else {
            logger.debug(s"no sentinel file at $sentinelFile yet")
          }
          Thread.sleep(10)
        }
      } catch {
        case  e: Throwable=>
          logger.error(s"util.TestResultNotifier could get results", e)
          promise.failure(e)        // TODO - test this path by manually deleting directory
      }

      promise.success(results)
    }
    promise.future
  }
}

object TestResultNotifierFactory extends LazyLogging {
  def getForNResults[T](numExpectedResults: Int): TestResultNotifier[T] = {
    var dir: Path = java.nio.file.Files.createTempDirectory("test-results")
    val path: String = dir.toAbsolutePath.toString
    logger.info(s"initializing TestResultNotifier pointed to this directory: $path")
    new TestResultNotifier[T](path, numExpectedResults)
  }

}
