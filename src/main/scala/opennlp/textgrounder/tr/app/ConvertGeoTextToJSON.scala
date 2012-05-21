package opennlp.textgrounder.tr.app

import com.codahale.jerkson.Json._

object ConvertGeoTextToJSON extends App {
  for(line <- scala.io.Source.fromFile(args(0), "ISO-8859-1").getLines) {
    val tokens = line.split("\t")
    println(generate(new tweet(tokens(3).toDouble, tokens(4).toDouble, tokens(5))))
  }
}

case class tweet(val lat:Double, val lon:Double, val text:String)
