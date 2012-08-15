package opennlp.textgrounder.tr.resolver

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import scala.collection.JavaConversions._

class DocDistResolver(val logFilePath:String) extends Resolver {

  def disambiguate(corpus:StoredCorpus): StoredCorpus = {

    val predDocLocations = (for(pe <- LogUtil.parseLogFile(logFilePath)) yield {
      (pe.docName, pe.predCoord)
    }).toMap

    for(doc <- corpus) {
      for(sent <- doc) {
        for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) {
          if(overwriteSelecteds || !toponym.hasSelected) {
            val predDocLocation = predDocLocations.getOrElse(doc.getId, null)
            if(predDocLocation != null) {
              val indexToSelect = toponym.getCandidates.zipWithIndex.minBy(
                p => p._1.getRegion.distance(predDocLocation))._2
              if(indexToSelect != -1) {
                toponym.setSelectedIdx(indexToSelect)
              }
            }
          }
        }
      }
    }

    corpus
  }
}
