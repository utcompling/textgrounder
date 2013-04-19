package opennlp.textgrounder.tr.tpp

class MultiPurchaseCoster(val purchaseCosters:List[PurchaseCoster]) extends PurchaseCoster {

  def apply(m:Market, potLoc:PotentialLocation): Double = {
    purchaseCosters.map(pc => pc(m, potLoc)).reduce(_*_)
  }
}
