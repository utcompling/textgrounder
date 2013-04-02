package opennlp.textgrounder.tr.tpp

class SimpleContainmentPurchaseCoster extends PurchaseCoster {

  def apply(m:Market, potLoc:PotentialLocation): Double = {
    if(m.locations.map(_._2).toSet.contains(potLoc))
      1.0
    else
      Double.PositiveInfinity
  }
}
