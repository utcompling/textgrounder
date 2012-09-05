package opennlp.textgrounder.tr.util

import opennlp.textgrounder.tr.topo._

object LogUtil {

  val DPC = 1.0

  val DOC_PREFIX = "Document "
  val PRED_CELL_RANK_PREFIX = "  Predicted cell (at rank "
  val PRED_CELL_KL_PREFIX = ", kl-div "
  val PRED_CELL_BOTTOM_LEFT_COORD_PREFIX = "): GeoCell(("
  val TRUE_COORD_PREFIX = " at ("
  val PRED_COORD_PREFIX = " predicted cell center at ("
  val NEIGHBOR_PREFIX = " close neighbor: ("

  val CELL_BOTTOM_LEFT_COORD_PREFIX = "Cell ("
  val NGRAM_DIST_PREFIX = "unseen mass, "
  val ngramAndCountRE = """^(\S+)\=(\S+)$""".r

  def parseLogFile(filename: String): List[LogFileParseElement]/*List[(String, Coordinate, Coordinate, List[(Coordinate, Int)])]*/ = {
    val lines = scala.io.Source.fromFile(filename).getLines

    var docName:String = null
    var neighbors:List[(Coordinate, Int)] = null
    var predCells:List[(Int, Double, Coordinate)] = null
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

          predCells = List()
          neighbors = List()

          None
        }

        else if(line.contains(PRED_CELL_RANK_PREFIX)) {
          val rankStartIndex = line.indexOf(PRED_CELL_RANK_PREFIX) + PRED_CELL_RANK_PREFIX.length
          val rankEndIndex = line.indexOf(PRED_CELL_KL_PREFIX, rankStartIndex)
          val rank = line.slice(rankStartIndex, rankEndIndex).toInt
          val klStartIndex = rankEndIndex + PRED_CELL_KL_PREFIX.length
          val klEndIndex = line.indexOf(PRED_CELL_BOTTOM_LEFT_COORD_PREFIX, klStartIndex)
          val kl = line.slice(klStartIndex, klEndIndex).toDouble
          val blCoordStartIndex = klEndIndex + PRED_CELL_BOTTOM_LEFT_COORD_PREFIX.length
          val blCoordEndIndex = line.indexOf(")", blCoordStartIndex)
          val rawBlCoord = line.slice(blCoordStartIndex, blCoordEndIndex).split(",")
          val blCoord = Coordinate.fromDegrees(rawBlCoord(0).toDouble, rawBlCoord(1).toDouble)

          predCells = predCells ::: ((rank, kl, blCoord) :: Nil)

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

          Some(new LogFileParseElement(docName, trueCoord, predCoord, predCells, neighbors))
        }

        else None
      }
      else None
    }).flatten.toList
  }

  def getNgramDists(filename: String): Map[Int, Map[String, Double]] = {
    val lines = scala.io.Source.fromFile(filename).getLines

    (for(line <- lines) yield {
      if(line.startsWith(CELL_BOTTOM_LEFT_COORD_PREFIX)) {
        val blCoordStartIndex = CELL_BOTTOM_LEFT_COORD_PREFIX.length
        val blCoordEndIndex = line.indexOf(")", blCoordStartIndex)
        val rawBlCoord = line.slice(blCoordStartIndex, blCoordEndIndex).split(",")
        val cellNum = TopoUtil.getCellNumber(rawBlCoord(0).toDouble, rawBlCoord(1).toDouble, DPC)

        val ngramDistRawStartIndex = line.indexOf(NGRAM_DIST_PREFIX, blCoordEndIndex) + NGRAM_DIST_PREFIX.length
        val ngramDistRawEndIndex = line.indexOf(")", ngramDistRawStartIndex)
        val dist =
        (for(token <- line.slice(ngramDistRawStartIndex, ngramDistRawEndIndex).split(" ")) yield {
          if(ngramAndCountRE.findFirstIn(token) != None) {
            val ngramAndCountRE(ngram, count) = token
            Some((ngram, count.toDouble))
          }
          else
            None
        }).flatten.toMap

        Some((cellNum, dist))
      }
      else
        None
    }).flatten.toMap
  }

}

class LogFileParseElement(
  val docName: String,
  val trueCoord: Coordinate,
  val predCoord: Coordinate,
  val predCells: List[(Int, Double, Coordinate)],
  val neighbors: List[(Coordinate, Int)]) {

    def getProbDistOverPredCells(knn:Int, dpc:Double): List[(Int, Double)] = {
      var sum = 0.0
      val myKNN = if(knn < 0) predCells.size else knn
      (for((rank, kl, blCoord) <- predCells.take(myKNN)) yield {
        val unnormalized = math.exp(-kl)
        sum += unnormalized
        (TopoUtil.getCellNumber(blCoord, dpc), unnormalized)
      }).map(p => (p._1, p._2/sum)).toList
    }
}
