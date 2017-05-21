package util

import java.io._
import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global


case class TestResultNotifier[T](resultsDirPath: String) extends LazyLogging  with Serializable {
  private[util] val resultsDataFilePath: String = resultsDirPath + File.separator + "results.dat"
  private[util] val doneSentinelFilePath: String = resultsDirPath + File.separator + "results.done"

  def recordResults(results: List[T]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    logger.info(s"TestResultNotifier recording results: $results")

    try {
      val fos = new FileOutputStream(new File (resultsDataFilePath))
      val oos = new ObjectOutputStream(fos);
      oos.writeObject(results)
      oos.close()
      require ( new File (doneSentinelFilePath).mkdir() )  // sentinel file: marks as 'done'
    } catch {
      case  e: Throwable=>
        throw new RuntimeException(s"util.TestResultNotifier could not record results: $results", e)
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
            val ois = new ObjectInputStream(new FileInputStream(resultsDataFilePath))
            results = ois.readObject().asInstanceOf[List[T]]
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
  def get[T](): TestResultNotifier[T] = {
    var dir: Path = java.nio.file.Files.createTempDirectory("test-results")
    val path: String = dir.toAbsolutePath.toString
    logger.info(s"initializing TestResultNotifier pointed to this directory: $path")
    TestResultNotifier[T](path)
  }

}
