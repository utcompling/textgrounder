package opennlp.textgrounder.geolocate

import opennlp.textgrounder.util.Twokenize
import org.clapper.argot._
import java.io._

object WordRankerByAvgErrorUT {

  import ArgotConverters._

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.geolocate.WordRankerByAvgError", preUsage = Some("TextGrounder"))
  val corpusFile = parser.option[String](List("i", "input"), "list", "corpus input file")
  val listFile = parser.option[String](List("l", "list"), "list", "list input file")
  val docThresholdOption = parser.option[Int](List("t", "threshold"), "threshold", "document frequency threshold")
  
  def main(args: Array[String]) {
    try {
      parser.parse(args)
    }
    catch {
      case e: ArgotUsageException => println(e.message); sys.exit(0)
    }

    if(corpusFile.value == None) {
      println("You must specify a corpus input file via -i.")
      sys.exit(0)
    }
    if(listFile.value == None) {
      println("You must specify a list input file via -l.")
      sys.exit(0)
    }

    val docThreshold = if(docThresholdOption.value == None) 10 else docThresholdOption.value.get

    val docNamesAndErrors:Map[String, Double] = scala.io.Source.fromFile(listFile.value.get).getLines.
      map(_.split("\t")).map(p => (p(0), p(1).toDouble)).toMap

    val in = new BufferedReader(
      new InputStreamReader(new FileInputStream(corpusFile.value.get), "UTF8"))

    val wordsToErrors = new scala.collection.mutable.HashMap[String, Double]
    val wordsToDocNames = new scala.collection.mutable.HashMap[String, scala.collection.immutable.HashSet[String]]

    var line:String = in.readLine

    while(line != null) {
      val tokens = line.split("\t")
      if(tokens.length >= 3) {
        val docName = tokens(0)
        if(docNamesAndErrors.contains(docName)) {
          val error = docNamesAndErrors(docName)
          val text = tokens(2)
          
          val words:Array[String] = text.split(" ").map(_.split(":")(0))
          
          for(word <- words) {
            //if(!word.startsWith("@user_")) {
              val prevError = wordsToErrors.getOrElse(word, 0.0)
              wordsToErrors.put(word, prevError + error)
              val prevSet = wordsToDocNames.getOrElse(word, new scala.collection.immutable.HashSet())
              wordsToDocNames.put(word, prevSet + docName)
            //}
          }
        }
      }
      line = in.readLine
    }  
    in.close

    wordsToErrors.foreach(p => if(wordsToDocNames(p._1).size < docThreshold) wordsToErrors.remove(p._1))

    wordsToErrors.foreach(p => wordsToErrors.put(p._1, p._2 / wordsToDocNames(p._1).size))

    wordsToErrors.toList.sortWith((x, y) => x._2 < y._2).foreach(p => println(p._1+"\t"+p._2))
  }
}
