package util

import java.io.File

import org.scalatest.{FunSuite, ShouldMatchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

class TestResultNotifierSpec extends FunSuite with ShouldMatchers {

  test("getResults() call returns results only after recordResults() has been called (empty results case)") {
    val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.get()
    val result: Future[List[(String, String)]] = notifier.getResults

    result.value shouldBe None

    intercept[java.util.concurrent.TimeoutException] {
      Await.ready(result, Duration("1 millisecond"))
    }

    notifier.recordResults(List())
    new File(notifier.doneSentinelFilePath).exists shouldBe true

    val completedResult: Future[List[(String, String)]] = Await.ready(result, Duration("100 millisecond"))
    completedResult.value.get.get  shouldBe List()
  }

  test("getResults() returns same results as what was recorded") {
    val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.get()
    val result: Future[List[(String, String)]] = notifier.getResults

    val tupleList = List(("blah", "foo"), ("hi", "ho"))
    notifier.recordResults(tupleList)
    val completedResult: Future[List[(String, String)]] = Await.ready(result, Duration("100 millisecond"))
    completedResult.value.get.get  shouldBe tupleList
  }

  test("error raised if recordResults() called more than once") {
    val notifier: TestResultNotifier[(String, String)] = TestResultNotifierFactory.get()
    val result: Future[List[(String, String)]] = notifier.getResults

    notifier.recordResults(List())

    intercept[Exception] {
      notifier.recordResults(List())
    }
  }
}

