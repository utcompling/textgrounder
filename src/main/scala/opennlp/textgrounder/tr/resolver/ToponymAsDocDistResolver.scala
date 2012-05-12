package opennlp.textgrounder.tr.resolver

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import scala.collection.JavaConversions._

class ToponymAsDocDistResolver(val logFilePath:String) extends Resolver {

  val docTokRE = """(.+)_([0-9]+)""".r
  val alphanumRE = """^[a-zA-Z0-9]+$""".r

  def disambiguate(corpus:StoredCorpus): StoredCorpus = {

    val predLocations = (for(pe <- LogUtil.parseLogFile(logFilePath)) yield {
      val docTokRE(docName, tokenIndex) = pe.docName
      ((docName, tokenIndex.toInt), pe.predCoord)
    }).toMap

    for(doc <- corpus) {
      var tokenIndex = 0
      for(sent <- doc) {
        for(token <- sent.filter(t => alphanumRE.findFirstIn(t.getForm) != None)) {
          if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0) {
            val toponym = token.asInstanceOf[Toponym]
            val predLocation = predLocations.getOrElse((doc.getId, tokenIndex), null)
            if(predLocation != null) {
              val indexToSelect = toponym.getCandidates.zipWithIndex.minBy(p => p._1.getRegion.distance(predLocation))._2
              if(indexToSelect != -1) {
                toponym.setSelectedIdx(indexToSelect)
              }
            }
          }
          tokenIndex += 1
        }
      }
    }

    corpus
  }
}
