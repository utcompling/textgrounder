package opennlp.textgrounder.tr.app

import java.io._
import java.util.zip._

import opennlp.textgrounder.tr.util._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.topo.gaz._
import opennlp.textgrounder.tr.text.io._

import scala.collection.JavaConversions._

object CorpusErrorAnalyzer extends BaseApp {
  
  def main(args:Array[String]) {
    initializeOptionsFromCommandLine(args)

    val corpus = TopoUtil.readStoredCorpusFromSerialized(getSerializedCorpusInputPath)

    for(doc <- corpus) {
      for(sent <- doc) {
        for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) {
          
        }
      }
    }
  }
}
