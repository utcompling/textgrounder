package opennlp.textgrounder.util

object WikiRelFreqs extends App {

  val geoFreqs = getFreqs(args(0))
  val allFreqs = getFreqs(args(1))

  val relFreqs = allFreqs.map(p => (p._1, geoFreqs.getOrElse(p._1, 0.0f) / p._2)).toList.sortWith((x, y) => if(x._2 != y._2) x._2 > y._2 else x._1 < y._1)

  relFreqs.foreach(println)

  def getFreqs(filename:String):Map[String, Float] = {
    val wordCountRE = """^(\w+)\s=\s(\d+)$""".r
    val lines = scala.io.Source.fromFile(filename).getLines
    val freqs = new scala.collection.mutable.HashMap[String, Float]
    var total = 0
    var lineCount = 0

    for(line <- lines) {
      if(wordCountRE.findFirstIn(line) != None) {
        val wordCountRE(word, count) = line
        val lowerWord = word.toLowerCase
        val oldCount = freqs.getOrElse(lowerWord, 0.0f)
        freqs.put(lowerWord, oldCount + count.toFloat)
        total += count.toInt
      }
      if(lineCount % 10000000 == 0)
        println(filename+" "+lineCount)
      lineCount += 1
    }

    freqs.map(p => (p._1, p._2 / total)).toMap
  }
}
