import java.lang

import scala.collection.mutable

/**
  * Describes the result of analyzing dups in one or more files submitted to the target
  * directory during the minor batch interval.
  */
case class DupInfo(numOccurencesInFile: Int, lineNumsOfOccurence: Set[Long])

/**
  * Describes the dups found in current minor batch interval, plus sums up the the counts of all dups found
  * during the most recent major batch interval.
  */
case class DupReport(dupsForMinorInterval: DupInfo,
                     totalDupOccurencesForMajorInteveral: Option[Map[String/*dupd word*/,Int/*count*/]])

object DupFinder {

  // This method is mainly for testing the other method of this object using more convenient List[String]
  def findDupsInList(lines: List[String]) : scala.collection.immutable.Map[String,DupInfo] = {
    findDups(
      lines.zipWithIndex.map {
        case (line: String, lineNumZeroBased: Int) => (new java.lang.Long(lineNumZeroBased + 1), line)
      }
    )
  }

  def findDups(lines: Seq[(java.lang.Long, String)]) : scala.collection.immutable.Map[String,DupInfo] = {
    val dups = mutable.Map[String, DupInfo]().withDefaultValue(DupInfo(0, Set()))
    lines.foreach {
      case(lineNum: java.lang.Long,  line: String) =>
        line.split("[^\\w']+").foreach {
          word: String =>                         // "word" is one of the words encounterd on line number 'lineNum'
            val curInfo: DupInfo = dups(word)
            dups(word) =  DupInfo(curInfo.numOccurencesInFile+1, curInfo.lineNumsOfOccurence + lineNum)
        }

    }
    dups.filter {   // only return those words found in the batch that occured more than once
      case(word: String,dupInfo: DupInfo) => dupInfo.numOccurencesInFile > 1
    }.toMap

  }
}
