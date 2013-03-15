package opennlp.textgrounder.tr.tpp

import opennlp.textgrounder.tr.topo._

class TPPInstance(val purchaseCoster:PurchaseCoster,
                  val travelCoster:TravelCoster) {

  var markets:List[Market] = null  
}

class Market(val id:Int,
             val locations:Set[PotentialLocation]) {

  def size = locations.size
}

class PotentialLocation(val docId:String,
                        val tokenIndex:Int,
                        val gazIndex:Int,
                        val loc:Location) {

  override def toString: String = {
    docId+":"+tokenIndex+":"+gazIndex
  }

  override def equals(other:Any):Boolean = {
    if(!other.isInstanceOf[PotentialLocation])
      false
    else {
      val o = other.asInstanceOf[PotentialLocation]
      this.docId.equals(o.docId) && this.tokenIndex == o.tokenIndex && this.gazIndex == o.gazIndex && this.loc.equals(o.loc)
    }
  }

  val S = 41*41
  val C = S*41

  override def hashCode: Int = {
    C * (C + tokenIndex) + S * (S + docId.hashCode) + 41 * (41 * gazIndex) + loc.getId
  }

}
