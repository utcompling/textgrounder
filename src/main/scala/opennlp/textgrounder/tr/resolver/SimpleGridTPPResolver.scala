package opennlp.textgrounder.tr.resolver

import opennlp.textgrounder.tr.tpp._
import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import java.util._

import scala.collection.JavaConversions._

class SimpleGridTPPResolver(val dpc:Double) extends TPPResolver(new TPPInstance(new SimpleContainmentPurchaseCoster, new SimpleDistanceTravelCoster)) {

  def disambiguate(corpus:StoredCorpus): StoredCorpus = {

    for(doc <- corpus) {

      val cellNumsToPotLocs = new scala.collection.mutable.HashMap[Int, scala.collection.mutable.HashSet[PotentialLocation]]

      val docAsArray = TextUtil.getDocAsArray(doc)

      // Set tppInstance's markets
      var tokIndex = 0
      for(token <- docAsArray) {
        if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0) {
          val toponym = token.asInstanceOf[Toponym]
          var gazIndex = 0
          for(loc <- toponym.getCandidates) {
            val potLoc = new PotentialLocation(doc.getId, tokIndex, gazIndex, loc)
            
            val cellNums = TopoUtil.getCellNumbers(loc, dpc)
            for(cellNum <- cellNums) {
              val potLocs = cellNumsToPotLocs.getOrElse(cellNum, new scala.collection.mutable.HashSet[PotentialLocation])
              potLocs.add(potLoc)
              cellNumsToPotLocs.put(cellNum, potLocs)
            }
            gazIndex += 1
          }
        }
        tokIndex += 1
      }

      tppInstance.markets = 
      (for(p <- cellNumsToPotLocs) yield {
        new Market(p._1, p._2.toSet)
      }).toList

      /*for(market <- tppInstance.markets.sortWith((x,y) => x.locations.size > y.locations.size)) {
        println("Market ID: "+market.id+"("+TopoUtil.getCellCenter(market.id, dpc)+")")
        print("  ")
        for(potLoc <- market.locations) {
          print(potLoc.loc.getName+"("+potLoc+":"+potLoc.loc.getRegion.getCenter+")"+", ")
        }
        println
      }*/

      // Apply a TPPSolver
      val solver = new ConstructionTPPSolver
      val tour = solver(tppInstance)

      // Decode the tour into the corpus
      val solutionMap = solver.getSolutionMap(tour)

      tokIndex = 0
      for(token <- docAsArray) {
        if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0) {
          val toponym = token.asInstanceOf[Toponym]
          if(solutionMap.contains((doc.getId, tokIndex))) {
            toponym.setSelectedIdx(solutionMap((doc.getId, tokIndex)))
            if(toponym.getSelectedIdx >= toponym.getAmbiguity) {
              println(tokIndex)
              println(toponym.getForm+": "+toponym.getSelectedIdx+" >= "+toponym.getAmbiguity)
            }
          }
        }

        tokIndex += 1
      }

    }

    //println("\n---\n")

    corpus
  }
}
