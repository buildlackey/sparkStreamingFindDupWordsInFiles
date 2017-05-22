package util

import java.io.File

import org.apache.commons.io.FileUtils
import org.scalatest.{FunSuite, ShouldMatchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class TestResultNotifierSpec extends FunSuite with ShouldMatchers {

  test("getResults() call returns results only after recordResults() has been called") {
    val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.getForNResults(0)
    val result: Future[List[(String, String)]] = notifier.getResults

    result.value shouldBe None

    intercept[java.util.concurrent.TimeoutException] {
      Await.ready(result, Duration("1 millisecond"))
    }

    val item = ("dummy-value", "dummy")
    notifier.recordResult(item)
    new File(notifier.doneSentinelFilePath).exists shouldBe true

    val completedResult: Future[List[(String, String)]] = Await.ready(result, Duration("100 millisecond"))
    completedResult.value.get.get  shouldBe List(item)
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
    completedResult.value.get.get.toSet shouldBe tupleList.toSet
  }
}

