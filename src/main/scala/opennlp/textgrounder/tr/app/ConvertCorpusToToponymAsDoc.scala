package opennlp.textgrounder.tr.app

import java.io._

import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.text.io._
import opennlp.textgrounder.tr.util._

import scala.collection.JavaConversions._

object ConvertCorpusToToponymAsDoc extends App {

  val windowSize = if(args.length >= 2) args(1).toInt else 0

  val alphanumRE = """^[a-zA-Z0-9]+$""".r

  val tokenizer = new OpenNLPTokenizer

  val corpus = Corpus.createStoredCorpus
  corpus.addSource(new TrXMLDirSource(new File(args(0)), tokenizer))
  corpus.setFormat(BaseApp.CORPUS_FORMAT.TRCONLL)
  corpus.load

  for(doc <- corpus) {
    val docAsArray = TextUtil.getDocAsArray(doc)
    var tokIndex = 0
    for(token <- docAsArray) {
      if(token.isToponym && token.asInstanceOf[Toponym].hasGold) {
        val goldCoord = token.asInstanceOf[Toponym].getGold.getRegion.getCenter

        val unigramCounts = getUnigramCounts(docAsArray, tokIndex, windowSize)
        
        print(doc.getId.drop(1)+"_"+tokIndex+"\t")
        print(doc.getId+"_"+tokIndex+"\t")
        print(goldCoord.getLatDegrees+","+goldCoord.getLngDegrees+"\t")
        print("1\t\tMain\tno\tno\tno\t")
        //print(token.getForm+":"+1+" ")\
        for((word, count) <- unigramCounts) {
          print(word+":"+count+" ")
        }
        println
      }
      tokIndex += 1
    }
  }

  def getUnigramCounts(docAsArray:Array[StoredToken], tokIndex:Int, windowSize:Int): Map[String, Int] = {

    val startIndex = math.max(0, tokIndex - windowSize)
    val endIndex = math.min(docAsArray.length, tokIndex + windowSize + 1)

    val unigramCounts = new collection.mutable.HashMap[String, Int]

    for(rawToken <- docAsArray.slice(startIndex, endIndex)) {
      for(token <- rawToken.getForm.split(" ")) {
        val prevCount = unigramCounts.getOrElse(token, 0)
        unigramCounts.put(token, prevCount + 1)
      }
    }

    unigramCounts.toMap
  }
}
