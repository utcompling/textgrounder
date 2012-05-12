package opennlp.textgrounder.tr.app

import java.io._

import opennlp.textgrounder.tr.util._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.text.io._

import scala.collection.JavaConversions._

object CorpusInfo extends App {

  val DPC = 1.0

  val tokenizer = new OpenNLPTokenizer

  val corpus = Corpus.createStoredCorpus
  corpus.addSource(new TrXMLDirSource(new File(args(0)), tokenizer))
  corpus.setFormat(BaseApp.CORPUS_FORMAT.TRCONLL)
  corpus.load

  val topsToCellCounts = new collection.mutable.HashMap[String, collection.mutable.HashMap[Int, Int]]

  for(doc <- corpus) {
    for(sent <- doc) {
      for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0).filter(_.hasGold)) {
        val cellCounts = topsToCellCounts.getOrElse(toponym.getForm, new collection.mutable.HashMap[Int, Int])
        val cellNum = TopoUtil.getCellNumber(toponym.getGold.getRegion.getCenter, DPC)
        if(cellNum != -1) {
          val prevCount = cellCounts.getOrElse(cellNum, 0)
          cellCounts.put(cellNum, prevCount + 1)
          topsToCellCounts.put(toponym.getForm, cellCounts)
        }
      }
    }
  }

  for((topForm, cellCounts) <- topsToCellCounts.toList.sortWith((x, y) => if(x._2.size != y._2.size)
                                                                            x._2.size > y._2.size
                                                                          else x._1 < y._1) ) {
    print(topForm+": ")
    cellCounts.toList.sortWith((x, y) => if(x._2 != y._2) x._2 > y._2 else x._1 < y._1)
      .foreach(p => print("["+TopoUtil.getCellCenter(p._1, DPC)+":"+p._2+"] "))
    println
  }
}
