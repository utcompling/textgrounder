package opennlp.textgrounder.tr.tpp

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import java.util.ArrayList

import scala.collection.JavaConversions._

class ClusterMarketCreator(doc:Document[StoredToken], val thresholdInKm:Double) extends MarketCreator(doc) {

  val threshold = thresholdInKm / 6372.8

  override def apply:List[Market] = {

    // Initialize singleton clusters:
    val clusters = new scala.collection.mutable.HashSet[Cluster]
    val docAsArray = TextUtil.getDocAsArrayNoFilter(doc)
    var tokIndex = 0
    var clusterIndex = 0
    for(token <- docAsArray) {
      if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0) {
        val toponym = token.asInstanceOf[Toponym]
        var gazIndex = 0
        for(loc <- toponym.getCandidates) {
          //val topMen = new ToponymMention(doc.getId, tokIndex)
          val potLoc = new PotentialLocation(doc.getId, tokIndex, gazIndex, loc)

          val cluster = new Cluster(clusterIndex)
          cluster.add(potLoc)
          clusters.add(cluster)

          clusterIndex += 1

          gazIndex += 1
        }
      }
      tokIndex += 1
    }

    // Repeatedly merge until no more merging happens:
    var atLeastOneMerge = true
    while(atLeastOneMerge) {
      atLeastOneMerge = false
      for(cluster <- clusters.toArray) {
        if(clusters.size >= 2 && clusters.contains(cluster)) {
          val closest = clusters.minBy(c => if(c.equals(cluster)) Double.PositiveInfinity else cluster.distance(c))
        
          if(cluster.distance(closest) <= threshold) {
            cluster.merge(closest)
            clusters.remove(closest)
            atLeastOneMerge = true
          }
        }
      }
    }

    // Turn clusters into Markets:
    (for(cluster <- clusters) yield {
      val tmsToPls = new scala.collection.mutable.HashMap[ToponymMention, PotentialLocation]
      for(potLoc <- cluster.potLocs)
        tmsToPls.put(new ToponymMention(potLoc.docId, potLoc.tokenIndex), potLoc)
      new Market(cluster.id, tmsToPls.toMap)
    }).toList
  }
}

class Cluster(val id:Int) {

  val potLocs = new ArrayList[PotentialLocation]
  var centroid:Coordinate = null

  def add(potLoc:PotentialLocation) {
    potLocs.add(potLoc)
    val newCoord = potLoc.loc.getRegion.getCenter
    if(size == 1)
      centroid = newCoord
    else {
      val newLat = (centroid.getLat * (size-1) + newCoord.getLat) / size
      val newLng = (centroid.getLng * (size-1) + newCoord.getLng) / size
      centroid = Coordinate.fromRadians(newLat, newLng)
    }
  }

  def size = potLocs.size

  def distance(other:Cluster) = this.centroid.distance(other.centroid)

  // This cluster absorbs the cluster given as a parameter, keeping this.id:
  def merge(other:Cluster) {
    val newLat = (this.centroid.getLat * this.size + other.centroid.getLat * other.size) / (this.size + other.size)
    val newLng = (this.centroid.getLng * this.size + other.centroid.getLng * other.size) / (this.size + other.size)
    this.centroid = Coordinate.fromRadians(newLat, newLng)

    for(otherPotLoc <- other.potLocs) {
      this.potLocs.add(otherPotLoc)
    }
  }

  override def equals(other:Any):Boolean = {
    if(!other.isInstanceOf[Cluster])
      false
    else {
      val o = other.asInstanceOf[Cluster]
      this.id.equals(o.id)
    }
  }

  override def hashCode:Int = id
}
