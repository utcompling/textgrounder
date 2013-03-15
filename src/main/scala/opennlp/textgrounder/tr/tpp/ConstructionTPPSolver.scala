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
      val bestMarketAndIndex = chooseBestMarketAndIndex(unvisitedMarkets)
      
      insertIntoTour(tour, bestMarketAndIndex._1, tppInstance)

      unvisitedMarkets.remove(bestMarketAndIndex._2)

      resolveToponymMentions(bestMarketAndIndex._1, unresolvedToponymMentions)
    }

    tour.toList
  }

  def chooseBestMarketAndIndex(markets:ArrayList[Market]): (Market, Int) = {
    markets.zipWithIndex.maxBy(_._1.size) // This should be modified to choose a market that depends on the tour so far
  }

  def insertIntoTour(tour:ArrayList[MarketVisit], market:Market, tppInstance:TPPInstance) {
    val marketVisit = new MarketVisit(market)

    for(potLoc <- market.locations) {
      marketVisit.purchasedLocations.add(potLoc)
    }

    val pc = tppInstance.purchaseCoster

    for(existingMarketVisit <- tour) {
      for(potLoc <- existingMarketVisit.purchasedLocations) {
        if(pc(existingMarketVisit.market, potLoc) <= pc(marketVisit.market, potLoc)) {
          marketVisit.purchasedLocations.remove(potLoc)
        }
      }
    }

    if(marketVisit.purchasedLocations.size > 0)
      tour.append(marketVisit) // This should be modified to put the market in the place that minimizes the added travel cost
  }

}
