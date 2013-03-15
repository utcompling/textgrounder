package opennlp.textgrounder.tr.tpp

import java.util.ArrayList

import opennlp.textgrounder.tr.topo._

abstract class TPPSolver {

  def getUnresolvedToponymMentions(tppInstance:TPPInstance): scala.collection.mutable.HashSet[ToponymMention] = {
    val utms = new scala.collection.mutable.HashSet[ToponymMention]

    for(market <- tppInstance.markets) {
      for(potLoc <- market.locations) {
        utms.add(new ToponymMention(potLoc.docId, potLoc.tokenIndex))
      }
    }

    utms
  }

  def resolveToponymMentions(market:Market, unresolvedToponymMentions:scala.collection.mutable.HashSet[ToponymMention]) {
    //print(unresolvedToponymMentions.size)
    for(potLoc <- market.locations) {
      unresolvedToponymMentions.remove(new ToponymMention(potLoc.docId, potLoc.tokenIndex))
    }
    //println(" --> " + unresolvedToponymMentions.size)
  }

  def getSolutionMap(tour:List[MarketVisit]): Map[(String, Int), Int] = {
    (for(marketVisit <- tour) yield {
      (for(potLoc <- marketVisit.purchasedLocations) yield {
        ((potLoc.docId, potLoc.tokenIndex), potLoc.gazIndex)
      })
    }).flatten.toMap
  }

  def apply(tppInstance:TPPInstance): List[MarketVisit]
}

class MarketVisit(val market:Market) {
  val purchasedLocations = new scala.collection.mutable.HashSet[PotentialLocation]
}

class ToponymMention(val docId:String,
                     val tokenIndex:Int) {

  override def toString: String = {
    docId+":"+tokenIndex
  }

  override def equals(other:Any):Boolean = {
    if(!other.isInstanceOf[ToponymMention])
      false
    else {
      val o = other.asInstanceOf[ToponymMention]
      this.docId.equals(o.docId) && this.tokenIndex == o.tokenIndex
    }
  }

  override def hashCode: Int = {
    41 * (41 + tokenIndex) + docId.hashCode
  }
}
