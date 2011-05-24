package opennlp.textgrounder.util

import java.io._

object TextUtil {

  def populateStoplist(filename: String): Set[String] = {
    var stoplist:Set[String] = Set()
    io.Source.fromFile(filename).getLines.foreach(line => stoplist += line)
    stoplist.toSet()
    stoplist
  }

}
