package opennlp.textgrounder.tr.tpp

abstract class PurchaseCoster {

  def apply(m:Market, tm:PotentialLocation): Double
}
