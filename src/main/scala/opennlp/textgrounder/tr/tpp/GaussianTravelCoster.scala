package opennlp.textgrounder.tr.tpp

import opennlp.textgrounder.tr.topo._

class GaussianTravelCoster extends TravelCoster {

  val VARIANCE_KM = 1610
  val variance = VARIANCE_KM / 6372.8

  def g(x:Double, y:Double) = GaussianUtil.g(x,y)

  def apply(m1:Market, m2:Market): Double = {
    1.0-g(m1.centroid.distance(m2.centroid)/variance, 0)
  }
}
