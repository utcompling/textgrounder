package opennlp.textgrounder.tr.tpp

object GaussianUtil {
  def left(sig_x:Double, sig_y:Double, rho:Double) = 1.0/(2*math.Pi*sig_x*sig_y*math.pow(1-rho*rho,.5))

  def right(x:Double, y:Double, mu_x:Double, mu_y:Double, sig_x:Double, sig_y:Double, rho:Double) = math.exp(-1.0/(2*(1-rho*rho))*( math.pow(x-mu_x,2)/math.pow(sig_x,2) + math.pow(y-mu_y,2)/math.pow(sig_y,2) - (2*rho*(x-mu_x)*(y-mu_y))/(sig_x*sig_y)))

  def f(x:Double, y:Double, mu_x:Double, mu_y:Double, sig_x:Double, sig_y:Double, rho:Double) = left(sig_x,sig_y,rho) * right(x,y,mu_x,mu_y,sig_x,sig_y,rho)
  
  def g(x:Double,y:Double) = f(x,y,0,0,1,1,0)
}
