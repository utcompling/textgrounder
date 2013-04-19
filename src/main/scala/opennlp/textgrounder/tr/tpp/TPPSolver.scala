package opennlp.textgrounder.tr.tpp

import java.util.ArrayList
import java.io._
import javax.xml.datatype._
import javax.xml.stream._

import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util.KMLUtil

import scala.collection.JavaConversions._

abstract class TPPSolver {

  def getUnresolvedToponymMentions(tppInstance:TPPInstance): scala.collection.mutable.HashSet[ToponymMention] = {
    val utms = new scala.collection.mutable.HashSet[ToponymMention]

    for(market <- tppInstance.markets) {
      for(tm <- market.locations.map(_._1)) {
        utms.add(tm)
      }
    }

    utms
  }

  def resolveToponymMentions(market:Market, unresolvedToponymMentions:scala.collection.mutable.HashSet[ToponymMention]) {
    //print(unresolvedToponymMentions.size)
    for(tm <- market.locations.map(_._1)) {
      unresolvedToponymMentions.remove(tm)
    }
    //println(" --> " + unresolvedToponymMentions.size)
  }

  def getSolutionMap(tour:List[MarketVisit]): Map[(String, Int), Int] = {

    val s = new scala.collection.mutable.HashSet[(String, Int)]

    (for(marketVisit <- tour) yield {
      (for(potLoc <- marketVisit.purchasedLocations.map(_._2)) yield {
        //if(s.contains((potLoc.docId, potLoc.tokenIndex)))
          //println("Already had "+potLoc.docId+":"+potLoc.tokenIndex)
        s.add((potLoc.docId, potLoc.tokenIndex))
        ((potLoc.docId, potLoc.tokenIndex), potLoc.gazIndex)
      })
    }).flatten.toMap
  }

  def writeKML(tour:List[MarketVisit], filename:String) {
    val bw = new BufferedWriter(new FileWriter(filename))
    val factory = XMLOutputFactory.newInstance
    val out = factory.createXMLStreamWriter(bw)

    KMLUtil.writeHeader(out, "tour")

    var prevMV:MarketVisit = null
    var index = 0
    for(mv <- tour) {
      val style = (index % 4) match {
        case 0 => "yellow"
        case 1 => "green"
        case 2 => "blue"
        case 3 => "white"
      }

      for(purLoc <- mv.purchasedLocations.map(_._2)) {
        //for(coord <- purLoc.loc.getRegion.getRepresentatives) {
        val coord = purLoc.loc.getRegion.getCenter
        KMLUtil.writePinPlacemark(out, purLoc.loc.getName+"("+purLoc+")", coord, style)
        //}
      }
      if(index >= 1) {
        KMLUtil.writeArcLinePlacemark(out, prevMV.centroid, mv.centroid)
      }
      
      prevMV = mv
      index += 1
    }

    KMLUtil.writeFooter(out)

    out.close
  }

  def apply(tppInstance:TPPInstance): List[MarketVisit]
}

class MarketVisit(val market:Market) {
  val purchasedLocations = new scala.collection.mutable.HashMap[ToponymMention, PotentialLocation]

  // This should only be accessed after purchasedLocations has stopped changing:
  lazy val centroid: Coordinate = {
    val lat:Double = purchasedLocations.map(_._2.loc.getRegion.getCenter.getLat).sum/purchasedLocations.size
    val lng:Double = purchasedLocations.map(_._2.loc.getRegion.getCenter.getLng).sum/purchasedLocations.size
    Coordinate.fromRadians(lat, lng)
  }
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
