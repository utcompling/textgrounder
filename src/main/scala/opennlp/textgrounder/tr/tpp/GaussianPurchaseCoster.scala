package opennlp.textgrounder.tr.tpp

import opennlp.textgrounder.tr.topo._

class GaussianPurchaseCoster extends PurchaseCoster {

  val VARIANCE_KM = 161.0
  val variance = VARIANCE_KM / 6372.8

  def left(sig_x:Double, sig_y:Double, rho:Double) = 1.0/(2*math.Pi*sig_x*sig_y*math.pow(1-rho*rho,.5))

  def right(x:Double, y:Double, mu_x:Double, mu_y:Double, sig_x:Double, sig_y:Double, rho:Double) = math.exp(-1.0/(2*(1-rho*rho))*( math.pow(x-mu_x,2)/math.pow(sig_x,2) + math.pow(y-mu_y,2)/math.pow(sig_y,2) - (2*rho*(x-mu_x)*(y-mu_y))/(sig_x*sig_y)))

  def f(x:Double, y:Double, mu_x:Double, mu_y:Double, sig_x:Double, sig_y:Double, rho:Double) = left(sig_x,sig_y,rho) * right(x,y,mu_x,mu_y,sig_x,sig_y,rho)
  
  def g(x:Double,y:Double) = f(x,y,0,0,1,1,0)

  //val max = g(0,0)


  val storedCosts = new scala.collection.mutable.HashMap[(Int, Int), Double] // (location.id, market.id) => distance
  def cost(l:Location, m:Market): Double = {
      val key = (l.getId, m.id)
      if(storedCosts.contains(key))
        storedCosts(key)
      else {
        val cost = 1.0-g(l.getRegion.distance(m.centroid)/variance, 0)///max
        storedCosts.put(key, cost)
        cost
      }
  }

  def apply(m:Market, potLoc:PotentialLocation): Double = {
    val c = cost(potLoc.loc, m)
    //println(m.centroid+", "+potLoc.loc.getRegion.getCenter+" ("+potLoc.loc.getName+"): "+c)
    c
  }
}
