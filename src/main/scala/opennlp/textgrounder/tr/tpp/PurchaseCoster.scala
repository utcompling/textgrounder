package opennlp.textgrounder.tr.tpp

abstract class PurchaseCoster {

  def apply(m:Market, potLoc:PotentialLocation): Double
}
