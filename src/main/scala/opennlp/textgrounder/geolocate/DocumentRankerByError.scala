package opennlp.textgrounder.geolocate

import org.clapper.argot._
import opennlp.textgrounder.topo._
import opennlp.textgrounder.util.LogUtil

object DocumentRankerByError {

  import ArgotConverters._

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.geolocate.ErrorKMLGenerator", preUsage = Some("TextGrounder"))
  val logFile = parser.option[String](List("l", "log"), "log", "log input file")
  //val outFile = parser.option[String](List("o", "out"), "out", "output file")
  
  def main(args: Array[String]) {
    try {
      parser.parse(args)
    }
    catch {
      case e: ArgotUsageException => println(e.message); sys.exit(0)
    }

    if(logFile.value == None) {
      println("You must specify a log input file via -l.")
      sys.exit(0)
    }

    val docsAndErrors:List[(String, Double)] =
      (for((docName, trueCoord, predCoord) <- LogUtil.parseLogFile(logFile.value.get)) yield {
        val dist = trueCoord.distanceInKm(predCoord)

        (docName, dist)
      }).sortWith((x, y) => x._2 < y._2)

    for((docName, dist) <- docsAndErrors) {
      println(docName+"\t"+dist)
    }
  }
}
