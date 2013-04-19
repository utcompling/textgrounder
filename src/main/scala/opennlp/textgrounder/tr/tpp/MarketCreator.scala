package opennlp.textgrounder.tr.tpp

import opennlp.textgrounder.tr.text._

abstract class MarketCreator(val doc:Document[StoredToken]) {
  def apply:List[Market]
}
