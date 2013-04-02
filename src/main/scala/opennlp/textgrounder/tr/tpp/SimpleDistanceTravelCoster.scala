package opennlp.textgrounder.tr.tpp

import opennlp.textgrounder.tr.util._

class SimpleDistanceTravelCoster extends TravelCoster {

  val storedDistances = new scala.collection.mutable.HashMap[(Int, Int), Double]
  val distanceTable = new DistanceTable

  def apply(m1:Market, m2:Market): Double = {

    if(storedDistances.contains((m1.id, m2.id))) {
      //println(storedDistances((m1.id, m2.id)))
      storedDistances((m1.id, m2.id))
    }

    else {
      var minDist = Double.PositiveInfinity
      for(loc1 <- m1.locations.map(_._2).map(_.loc)) {
        for(loc2 <- m2.locations.map(_._2).map(_.loc)) {
          val dist = distanceTable.distance(loc1, loc2)
          if(dist < minDist)
            minDist = dist
        }
      }
      
      storedDistances.put((m1.id, m2.id), minDist)
      //println(minDist)
      minDist
    }
  }
}
