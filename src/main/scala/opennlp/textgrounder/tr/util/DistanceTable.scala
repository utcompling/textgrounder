package opennlp.textgrounder.tr.util

import opennlp.textgrounder.tr.topo._

class DistanceTable {

  val storedDistances = new scala.collection.mutable.HashMap[(Int, Int), Double]
  
  def distance(l1:Location, l2:Location): Double = {
    var leftLoc = l1
    var rightLoc = l2
    if(l1.getId > l2.getId) {
      leftLoc = l2
      rightLoc = l1
    }

    if(leftLoc.getRegion.getRepresentatives.size == 1 && rightLoc.getRegion.getRepresentatives.size == 1) {
      leftLoc.distance(rightLoc)
    }
    else {
      val key = (leftLoc.getId, rightLoc.getId)
      if(storedDistances.contains(key)) {
        storedDistances(key)
      }
      else {
        val dist = leftLoc.distance(rightLoc)
        storedDistances.put(key, dist)
        dist
      }
    }
  }
}
