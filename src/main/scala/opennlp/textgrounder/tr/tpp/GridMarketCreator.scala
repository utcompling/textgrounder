package opennlp.textgrounder.tr.tpp

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.util._

import scala.collection.JavaConversions._

class GridMarketCreator(doc:Document[StoredToken], val dpc:Double) extends MarketCreator(doc) {
  override def apply:List[Market] = {
    val cellNumsToPotLocs = new scala.collection.mutable.HashMap[Int, scala.collection.mutable.HashMap[ToponymMention, PotentialLocation]]

    val docAsArray = TextUtil.getDocAsArrayNoFilter(doc)

    var tokIndex = 0
    for(token <- docAsArray) {
      if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0) {
        val toponym = token.asInstanceOf[Toponym]
        var gazIndex = 0
        for(loc <- toponym.getCandidates) {
          val topMen = new ToponymMention(doc.getId, tokIndex)
          val potLoc = new PotentialLocation(doc.getId, tokIndex, gazIndex, loc)
          
          val cellNums = TopoUtil.getCellNumbers(loc, dpc)
          for(cellNum <- cellNums) {
            val potLocs = cellNumsToPotLocs.getOrElse(cellNum, new scala.collection.mutable.HashMap[ToponymMention, PotentialLocation])
            potLocs.put(topMen, potLoc)
            cellNumsToPotLocs.put(cellNum, potLocs)
          }
          gazIndex += 1
        }
      }
      tokIndex += 1
    }
    
    (for(p <- cellNumsToPotLocs) yield {
      new Market(p._1, p._2.toMap)
    }).toList
  }
}
