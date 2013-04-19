package opennlp.textgrounder.tr.resolver

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import java.util._

import scala.collection.JavaConversions._

class HeuristicTPPResolver extends Resolver {

  val DPC = 1.0

  def disambiguate(corpus:StoredCorpus): StoredCorpus = {

    for(doc <- corpus) {

      println("\n---\n")

      val cellsToLocs = new HashMap[Int, HashSet[Location]]
      val candsToNames = new HashMap[Location, HashSet[String]]
      val unresolvedToponyms = new HashSet[String]
      val localGaz = new HashMap[String, HashSet[Location]]

      for(sent <- doc) {
        for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) {
          unresolvedToponyms.add(toponym.getForm)
          if(!localGaz.contains(toponym.getForm)) {
            localGaz.put(toponym.getForm, new HashSet[Location])
            for(cand <- toponym) localGaz.get(toponym.getForm).add(cand)
          }
          for(cand <- toponym) {
            if(!candsToNames.containsKey(cand))
              candsToNames.put(cand, new HashSet[String])
            candsToNames.get(cand).add(toponym.getForm)

            val cellNum = TopoUtil.getCellNumber(cand.getRegion.getCenter, DPC)
            if(!cellsToLocs.containsKey(cellNum))
              cellsToLocs.put(cellNum, new HashSet[Location])
            cellsToLocs.get(cellNum).add(cand)
          }
        }
      }

      val resolvedToponyms = new HashMap[String, Int]

      // Initialize tour to be empty
      val tour = new ArrayList[Int]

      // While not all toponyms have been resolved,
      while(!unresolvedToponyms.isEmpty) {
        println(unresolvedToponyms.size + " unresolved toponyms. (First is " + unresolvedToponyms.toList(0) + ")")
        // Iterate over ALL cellNums, choosing one to add to the tour
        val bestCell = chooseBestCell(cellsToLocs, tour)
        val locs = cellsToLocs.get(bestCell)
        cellsToLocs.remove(bestCell)

        addCellToTour(tour, bestCell)
        //println("Added " + bestCell + " (" + locs.size + " locations) to the tour, making it length " + tour.length)

        for(loc <- locs) {
          println(candsToNames.get(loc).size)
          for(name <- candsToNames.get(loc)) {
            if(unresolvedToponyms.contains(name)) {
              resolvedToponyms.put(name, bestCell)
              println("  Resolved " + name + " to " + bestCell + ".  " + cellsToLocs.size + " left.")
              unresolvedToponyms.remove(name)

              for(otherLoc <- localGaz.get(name)) {
                val otherCellNum = TopoUtil.getCellNumber(otherLoc.getRegion.getCenter, DPC)
                if(cellsToLocs.contains(otherCellNum) && noneCanResolveTo(unresolvedToponyms, otherLoc, localGaz)) {
                  cellsToLocs.get(otherCellNum).remove(otherLoc)
                  println("    Removing " + name + " from " + otherCellNum)
                  if(cellsToLocs.get(otherCellNum).isEmpty) {
                    cellsToLocs.remove(otherCellNum)
                    println("      " + otherCellNum + " is now empty; removing.  " + cellsToLocs.size + " left.")
                  }
                }
              }

            }
          }
        }
      }

      // Iterate over document, setting each toponym token according to resolvedToponyms
      for(sent <- doc) {
        for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) {
          if(resolvedToponyms.contains(toponym.getForm)) {
            val cellToChoose = resolvedToponyms.get(toponym.getForm)
            var index = 0
            for(cand <- toponym) {
              val cellNum = TopoUtil.getCellNumber(cand.getRegion.getCenter, DPC)
              if(cellNum == cellToChoose) {
                toponym.setSelectedIdx(index)
              }
              index += 1
            }
          }
        }
      }
    }

    corpus
  }

  def noneCanResolveTo(unresolvedToponyms:HashSet[String], otherLoc:Location,
                       localGaz:HashMap[String, HashSet[Location]]): Boolean = {
    for(top <- unresolvedToponyms) {
      for(cand <- localGaz.get(top)) {
        if(cand == otherLoc)
          return false
      }
    }

    true
  }

  def chooseBestCell(cellsToLocs:HashMap[Int, HashSet[Location]], tour:ArrayList[Int]): Int = {
    mostLocsRanker(cellsToLocs)
    //leastDistAddedRanker(cellsToLocs, tour)
    //comboRanker(cellsToLocs, tour)
  }

  def mostLocsRanker(cellsToLocs:HashMap[Int, HashSet[Location]]): Int = {
    val sortedCellsToLocs:List[(Int, HashSet[Location])] = cellsToLocs.toList.sortWith(
      (x, y) => x._2.size > y._2.size)

    sortedCellsToLocs(0)._1
  }

  def leastDistAddedRanker(cellsToLocs:HashMap[Int, HashSet[Location]], tour:ArrayList[Int]): Int = {
    val sortedCellsToLocs:List[(Int, HashSet[Location])] = cellsToLocs.toList.sortWith(
      (x, y) => computeBestIndexAndDist(tour, x._1)._2 < computeBestIndexAndDist(tour, y._1)._2)

    sortedCellsToLocs(0)._1
  }

  def comboRanker(cellsToLocs:HashMap[Int, HashSet[Location]], tour:ArrayList[Int]): Int = {
    val sortedCellsToLocs:List[(Int, HashSet[Location])] = cellsToLocs.toList.sortWith(
      (x, y) => if(x._2.size != y._2.size) x._2.size > y._2.size
                else computeBestIndexAndDist(tour, x._1)._2 < computeBestIndexAndDist(tour, y._1)._2)

    sortedCellsToLocs(0)._1
  }

  def addCellToTour(tour:ArrayList[Int], cellNum:Int) {
    tour.add(computeBestIndexAndDist(tour, cellNum)._1, cellNum)
  }

  def computeBestIndexAndDist(tour:ArrayList[Int], cellNum:Int): (Int, Double) = {
    val newCellCenter = TopoUtil.getCellCenter(cellNum, DPC)

    if(tour.length == 0)
      return (0, 0.0)

    if(tour.length == 1)
      return (0, newCellCenter.distance(TopoUtil.getCellCenter(tour.get(0), DPC)))

    var optimalIndex = -1
    var minDistChange = Double.PositiveInfinity
    for(index <- 0 to tour.length) {
      var distChange = 0.0
      if(index == 0)
        distChange = newCellCenter.distance(TopoUtil.getCellCenter(tour.get(0), DPC))
      else if(index == tour.length)
        distChange = newCellCenter.distance(TopoUtil.getCellCenter(tour.get(tour.length-1), DPC))
      else {
        val prevCellCenter = TopoUtil.getCellCenter(tour.get(index-1), DPC)
        val nextCellCenter = TopoUtil.getCellCenter(tour.get(index), DPC)
        distChange = prevCellCenter.distance(newCellCenter)
                   + newCellCenter.distance(nextCellCenter)
                   - prevCellCenter.distance(nextCellCenter)
      }

      if(distChange < minDistChange) {
        minDistChange = distChange
        optimalIndex = index
      }
    }

    (optimalIndex, minDistChange)
  }

}
