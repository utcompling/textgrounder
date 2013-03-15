package opennlp.textgrounder.tr.tpp

class SimpleContainmentPurchaseCoster extends PurchaseCoster {

  def apply(m:Market, tm:PotentialLocation): Double = {
    if(m.locations.contains(tm))
      1.0
    else
      Double.PositiveInfinity
  }
}
