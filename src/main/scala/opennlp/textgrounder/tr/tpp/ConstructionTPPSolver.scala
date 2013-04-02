package opennlp.textgrounder.tr.tpp

import java.util.ArrayList

import scala.collection.JavaConversions._

class ConstructionTPPSolver extends TPPSolver {
  def apply(tppInstance:TPPInstance): List[MarketVisit] = {

    val tour = new ArrayList[MarketVisit]

    val unvisitedMarkets = new ArrayList[Market](tppInstance.markets)

    val unresolvedToponymMentions = getUnresolvedToponymMentions(tppInstance)

    while(!unresolvedToponymMentions.isEmpty) {
      //println(unresolvedToponymMentions.size)
      val bestMarketAndIndexes = chooseBestMarketAndIndexes(unvisitedMarkets, unresolvedToponymMentions, tour, tppInstance)
      
      insertIntoTour(tour, bestMarketAndIndexes._1, bestMarketAndIndexes._3, tppInstance)

      unvisitedMarkets.remove(bestMarketAndIndexes._2)

      resolveToponymMentions(bestMarketAndIndexes._1, unresolvedToponymMentions)
    }

    //println("Tour had " + tour.size + " market visits.")

    tour.toList
  }

  // First index is index in parameter named markets containing the chosen market; second index is optimal position in the tour to insert it
  def chooseBestMarketAndIndexes(markets:ArrayList[Market], unresolvedToponymMentions:scala.collection.mutable.HashSet[ToponymMention], tour:ArrayList[MarketVisit], tppInstance:TPPInstance): (Market, Int, Int) = {

    //val mostUnresolvedToponymMentions = markets.map(m => countOverlap(m.locations, unresolvedToponymMentions)).max
    val pc = tppInstance.purchaseCoster
    val leastAveragePurchaseCost = markets.map(m => m.locations.map(l => pc(m, l._2)).sum/m.locations.size).min //////////
    //println(leastAveragePurchaseCost)

    //val potentialBestMarkets = markets.zipWithIndex.filter(p => countOverlap(p._1.locations, unresolvedToponymMentions) == mostUnresolvedToponymMentions)
    val potentialBestMarkets = markets.zipWithIndex.filter(p => p._1.locations.map(l => pc(p._1, l._2)).sum/p._1.locations.size <= (leastAveragePurchaseCost+.000000001)) // Prevent rounding errors

    val r = potentialBestMarkets.map(p => (p, getBestCostIncreaseAndIndex(tour, p._1, tppInstance))).minBy(q => q._2._1)

    (r._1._1, r._1._2, r._2._2)


    //markets.zipWithIndex.maxBy(p => p._1.locations.map(_._1).map(tm => if(unresolvedToponymMentions.contains(tm)) 1 else 0).sum) // market with the greatest number of goods (types) I haven't puchased yet; but this is bugged, so why does it work well? -- it doesn't seem to anymore after fixing other bugs
    //markets.zipWithIndex.maxBy(_._1.locations.map(_._2).map(_.loc).toSet.size) // biggest market by types
    //markets.zipWithIndex.maxBy(_._1.size) // biggest market by tokens
  }

  def countOverlap(pls:Map[ToponymMention, PotentialLocation], urtms:scala.collection.mutable.HashSet[ToponymMention]): Int = {
    var sum = 0
    for(tm <- pls.map(_._1))
      if(urtms.contains(tm))
        sum += 1

    sum
  }

  def insertIntoTour(tour:ArrayList[MarketVisit], market:Market, index:Int, tppInstance:TPPInstance) {
    val marketVisit = new MarketVisit(market)

    // Buy everything at the new market
    for((topMen, potLoc) <- market.locations) {
      marketVisit.purchasedLocations.put(topMen, potLoc)
    }

    val pc = tppInstance.purchaseCoster

    // Unbuy goods that have already been purchased elsewhere for the same or cheaper prices
    for(existingMarketVisit <- tour) {
      var index = 0
      val purLocs = marketVisit.purchasedLocations.toList
      while(index < purLocs.size) {
      //for((topMen, newPotLoc) <- marketVisit.purchasedLocations) {
        val topMen = purLocs(index)._1
        val newPotLoc = purLocs(index)._2
        val prevPotLoc = existingMarketVisit.purchasedLocations.getOrElse(topMen, null)
        if(prevPotLoc != null) {
          if(pc(existingMarketVisit.market, prevPotLoc) <= pc(marketVisit.market, newPotLoc)) {
            //print(purLocs.size+" => ")
            marketVisit.purchasedLocations.remove(topMen)
            //println(purLocs.size)
          }
          else {
            existingMarketVisit.purchasedLocations.remove(topMen)
          }
        }
        index += 1
      }
    }

    if(marketVisit.purchasedLocations.size > 0)
      tour.insert(index, marketVisit) // This puts the market in the place that minimizes the added travel cost
  }

  def getBestCostIncreaseAndIndex(tour:ArrayList[MarketVisit], market:Market, tppInstance:TPPInstance): (Double, Int) = {

    val tc = tppInstance.travelCoster

    if(tour.size == 0)
      (0.0, 0)
    else if(tour.size == 1)
      (tc(tour(0).market, market), 1)
    else {
      var minAddedCost = Double.PositiveInfinity
      var bestIndex = -1
      for(index <- 0 to tour.size) {
        var addedCost = 0.0
        if(index == 0) {
          addedCost = tc(market, tour(0).market)
        }
        else if(index == tour.size) {
          addedCost = tc(tour(tour.size-1).market, market)
        }
        else {
          addedCost = tc(tour(index-1).market, market) + tc(market, tour(index).market) - tc(tour(index-1).market, tour(index).market)
        }
        
        if(addedCost < minAddedCost) {
          minAddedCost = addedCost
          bestIndex = index
        }
      }
      //println(minAddedCost+" at "+bestIndex)
      (minAddedCost, bestIndex)
    }
  }

}
