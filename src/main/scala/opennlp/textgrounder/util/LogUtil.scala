package opennlp.textgrounder.util

import opennlp.textgrounder.topo._

object LogUtil {

  val DOC_PREFIX = "Document "
  val TRUE_COORD_PREFIX = " at ("
  val PRED_COORD_PREFIX = " predicted cell center at ("
  val NEIGHBOR_PREFIX = " close neighbor: ("

  def parseLogFile(filename: String): List[(String, Coordinate, Coordinate, List[(Coordinate, Int)])] = {
    val lines = scala.io.Source.fromFile(filename).getLines

    var docName:String = null
    var neighbors:List[(Coordinate, Int)] = null
    var trueCoord:Coordinate = null
    var predCoord:Coordinate = null

    (for(line <- lines) yield {
      if(line.startsWith("#")) {

        if(line.contains(DOC_PREFIX)) {
          var startIndex = line.indexOf(DOC_PREFIX) + DOC_PREFIX.length
          var endIndex = line.indexOf(TRUE_COORD_PREFIX, startIndex)
          docName = line.slice(startIndex, endIndex)
          if(docName.contains("/")) docName = docName.drop(docName.indexOf("/")+1)
          
          startIndex = line.indexOf(TRUE_COORD_PREFIX) + TRUE_COORD_PREFIX.length
          endIndex = line.indexOf(")", startIndex)
          val rawCoords = line.slice(startIndex, endIndex).split(",")
          trueCoord = Coordinate.fromDegrees(rawCoords(0).toDouble, rawCoords(1).toDouble)

          neighbors = List()

          None
        }

        else if(line.contains(NEIGHBOR_PREFIX)) {
          val startIndex = line.indexOf(NEIGHBOR_PREFIX) + NEIGHBOR_PREFIX.length
          val endIndex = line.indexOf(")", startIndex)
          val rawCoords = line.slice(startIndex, endIndex).split(",")
          val curNeighbor = Coordinate.fromDegrees(rawCoords(0).toDouble, rawCoords(1).toDouble)
          val rankStartIndex = line.indexOf("#", 1)+1
          val rankEndIndex = line.indexOf(" ", rankStartIndex)
          val rank = line.slice(rankStartIndex, rankEndIndex).toInt
          
          neighbors = neighbors ::: ((curNeighbor, rank) :: Nil)

          None
        }

        else if(line.contains(PRED_COORD_PREFIX)) {
          val startIndex = line.indexOf(PRED_COORD_PREFIX) + PRED_COORD_PREFIX.length
          val endIndex = line.indexOf(")", startIndex)
          val rawCoords = line.slice(startIndex, endIndex).split(",")
          predCoord = Coordinate.fromDegrees(rawCoords(0).toDouble, rawCoords(1).toDouble)

          Some((docName, trueCoord, predCoord, neighbors))
        }

        else None
      }
      else None
    }).flatten.toList
  }

}
