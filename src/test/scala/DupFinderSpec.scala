import org.scalatest.{Matchers, _}

class DupFinderSpec extends FlatSpec with Matchers {
  "DupFinder" should "return empty map if given empty sequence of lines" in {
    DupFinder.findDupsInList(List()) shouldEqual Map[String, DupInfo]()
  }

  "DupFinder" should "return empty map if given empty sequence of lines with no duplicated words" in {
    val lines = List("dog bone", "cat zone", "phone home")
    DupFinder.findDupsInList(lines) shouldEqual Map[String, DupInfo]()
  }

  "DupFinder" should "find dups that occur on the same line and report correct line numbers" in {
    val lines = List("cat zone", "dog break zebra mouse dog", "phone home")
    DupFinder.findDupsInList(lines) shouldEqual Map("dog" -> DupInfo(2, Set(2)))
  }

  "DupFinder" should "find dups that occur on different lines of a file and report correct line numbers" in {
    val lines = List("cat zone", "dog break zebra mouse dog", "cat phone home")
    DupFinder.findDupsInList(lines) shouldEqual Map("dog" -> DupInfo(2, Set(2)), "cat" -> DupInfo(2, Set(1, 3)))
  }
}


