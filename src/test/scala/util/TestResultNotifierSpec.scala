package util

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{FunSuite}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class TestResultNotifierSpec extends FunSuite {

  test("getResults() call returns results only after recordResults() has been called") {
    val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.getForNResults(0)
    val result: Future[List[(String, String)]] = notifier.getResults

    assert(result.value.isEmpty)


    intercept[java.util.concurrent.TimeoutException] {
      Await.ready(result, Duration("1 millisecond"))
    }

    val item = ("dummy-value", "dummy")
    notifier.recordResult(item)
    assert(new File(notifier.doneSentinelFilePath).exists )

    val completedResult: Future[List[(String, String)]] = Await.ready(result, Duration("100 millisecond"))

    assert(completedResult.value.get.get.equals(List(item)))

  }

  test("getResults() returns same results as what was recorded") {
    runTestWithNResultsExpected(2)
  }

  test("error not raised if recordResults() called more than number of expected results") {
    runTestWithNResultsExpected(1)
  }

  test("error raised if directory becomes unavailable before recordResults() called ") {
    val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.getForNResults(2)
    val result: Future[List[(String, String)]] = notifier.getResults

    val tupleList = List(("blah", "foo"), ("hi", "ho"))
    tupleList.foreach {
      notifier.recordResult
    }

    FileUtils.deleteDirectory(new File(notifier.resultsDirPath))

    intercept[java.util.concurrent.TimeoutException] {
      Await.ready(result, Duration("500 millisecond"))
    }
  }

  private def runTestWithNResultsExpected(numResultsExpected: Int) = {
    val tupleList = List(("blah", "foo"), ("hi", "ho"))
    val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.getForNResults(numResultsExpected)
    val result: Future[List[(String, String)]] = notifier.getResults

    tupleList.foreach {
      notifier.recordResult
    }
    val completedResult: Future[List[(String, String)]] = Await.ready(result, Duration("100 millisecond"))
    assert(completedResult.value.get.get.toSet .equals( tupleList.toSet ))
  }
}

