package opennlp.textgrounder.app

import java.io._
import java.util._

import opennlp.textgrounder.text._
import opennlp.textgrounder.text.io._
import opennlp.textgrounder.text.prep._
import opennlp.textgrounder.app._
import opennlp.textgrounder.util._

import scala.collection.JavaConversions._

object GeoTextLabelPropPreproc extends BaseApp {

  import BaseApp._

  def MIN_COUNT_THRESHOLD = 5

  def DPC = 1.0

  def CELL_ = "cell_"
  def CELL_LABEL_ = "cell_label_"
  def DOC_ = "doc_"
  def UNI_ = "uni_"
  def BI_ = "bi_"

  def main(args: Array[String]) {

    this.initializeOptionsFromCommandLine(args)
    this.doPreproc

  }

  def doPreproc() {
    checkExists(getSerializedCorpusInputPath)

    val stoplist:Set[String] =
      if(getStoplistInputPath != null) TextUtil.populateStoplist(getStoplistInputPath)
      else collection.immutable.Set[String]()

    val corpus = TopoUtil.readStoredCorpusFromSerialized(getSerializedCorpusInputPath)

    writeCellSeeds(corpus, getSeedOutputPath)
    writeDocCellEdges(corpus, getGraphOutputPath)
    writeNGramDocEdges(corpus, getGraphOutputPath, stoplist)

    //writeCellCellEdges(getGraphOutputPath)
  }

  def writeCellSeeds(corpus: StoredCorpus, seedOutputPath: String) = {
    val out = new FileWriter(seedOutputPath)

    val edges = (for (doc <- corpus.filter(_.isTrain)) yield {
      val cellNumber = TopoUtil.getCellNumber(doc.getGoldCoord, DPC)
      Tuple3(CELL_ + cellNumber, CELL_LABEL_ + cellNumber, 1.0)
    })

    edges.map(writeEdge(out, _))

    out.close
  }

  def writeDocCellEdges(corpus: StoredCorpus, graphOutputPath: String) = {
    val out = new FileWriter(graphOutputPath)

    val edges = corpus.filter(_.isTrain).map(doc => Tuple3(doc.getId, CELL_ + TopoUtil.getCellNumber(doc.getGoldCoord, DPC), 1.0))

    edges.map(writeEdge(out, _))

    out.close
  }

  def writeNGramDocEdges(corpus: StoredCorpus, graphOutputPath: String, stoplist: Set[String]) = {

    val docIdsToNGrams = new collection.mutable.HashMap[String, collection.mutable.HashSet[String]]
    val unigramsToCounts = new collection.mutable.HashMap[String, Int]
    val bigramsToCounts = new collection.mutable.HashMap[String, Int]

    for(document <- corpus) {
      docIdsToNGrams.put(document.getId, new collection.mutable.HashSet[String]())
      for(sentence <- document) {
        var prevUni:String = null
        for(token <- sentence) {
          val unigram = token.getForm
          if(!stoplist.contains(unigram)) {
            docIdsToNGrams.get(document.getId).get += UNI_ + unigram
            if(!unigramsToCounts.contains(unigram)) {
              unigramsToCounts.put(unigram, 1)
              //println("saw " + unigram + " for the first time.")
            }
            else {
              unigramsToCounts.put(unigram, unigramsToCounts.get(unigram).get + 1)
              //println("saw " + unigram + " for the " + (unigramsToCounts.get(unigram).get + 1) + " time.")
            }
          }
          if(prevUni != null) {
            val bigram = prevUni + " " + unigram
            docIdsToNGrams.get(document.getId).get += BI_ + bigram
            if(!bigramsToCounts.contains(bigram))
              bigramsToCounts.put(bigram, 1)
            else
              bigramsToCounts.put(bigram, bigramsToCounts.get(bigram).get + 1)
          }
          prevUni = unigram
        }
      }
    }

    val out = new FileWriter(graphOutputPath, true)

    for(docId <- docIdsToNGrams.keys) {
      for(ngram <- docIdsToNGrams.get(docId).get) {
        val shortNGram =
          if(ngram.startsWith(BI_)) ngram.substring(BI_.length)
          else if(ngram.startsWith(UNI_)) ngram.substring(UNI_.length)
          else ngram
        if((unigramsToCounts.contains(shortNGram) && unigramsToCounts.get(shortNGram).get >= MIN_COUNT_THRESHOLD)
           || (bigramsToCounts.contains(shortNGram) && bigramsToCounts.get(shortNGram).get >= MIN_COUNT_THRESHOLD)) {
          writeEdge(out, ngram, docId, 1.0)
          //println("writing " + ngram + " " + docId)
           }
      }
    }

    /*for(document <- corpus) {
      for(sentence <- document) {
        var prevUni:String = null
        for(token <- sentence) {
          if(!stoplist.contains(token.getForm))
            writeEdge(out, UNI_ + token.getForm, document.getId, 1.0)
          if(prevUni != null)
            writeEdge(out, BI_ + prevUni + " " + token.getForm, document.getId, 1.0)
          prevUni = token.getForm
        }
      }
    }*/

    out.close
  }

  def writeCellCellEdges(graphOutputPath: String) = {
    val out = new FileWriter(graphOutputPath, true)

    var lon = 0.0
    while(lon < 360.0 / DPC) {
      var lat = 0.0
      while(lat < 180.0 / DPC) {
        val curCellNumber = TopoUtil.getCellNumber(lat, lon, DPC)
        val leftCellNumber = TopoUtil.getCellNumber(lat, lon - DPC, DPC)
        val rightCellNumber = TopoUtil.getCellNumber(lat, lon + DPC, DPC)
        val topCellNumber = TopoUtil.getCellNumber(lat + DPC, lon, DPC)
        val bottomCellNumber = TopoUtil.getCellNumber(lat - DPC, lon, DPC)
        
        writeEdge(out, CELL_ + curCellNumber, CELL_ + leftCellNumber, 1.0)
        writeEdge(out, CELL_ + curCellNumber, CELL_ + rightCellNumber, 1.0)
        if(topCellNumber >= 0)
          writeEdge(out, CELL_ + curCellNumber, CELL_ + topCellNumber, 1.0)
        if(bottomCellNumber >= 0)
          writeEdge(out, CELL_ + curCellNumber, CELL_ + bottomCellNumber, 1.0)

        lat += DPC
      }

      lon += DPC
    }

    out.close
  }

  def writeEdge(out: FileWriter, node1: String, node2: String, weight: Double) = {
    out.write(node1 + "\t" + node2 + "\t" + weight + "\n")
  }

  def writeEdge(out: FileWriter, e: Tuple3[String, String, Double]) = {
    out.write(e._1 + "\t" + e._2 + "\t" + e._3 + "\n")
  }
}
