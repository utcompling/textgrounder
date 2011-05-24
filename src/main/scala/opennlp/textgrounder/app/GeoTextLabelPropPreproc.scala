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

    for(document <- corpus) {
      if(document.isTrain) {
        val cellNumber = TopoUtil.getCellNumber(document.getGoldCoord(), DPC)
        writeEdge(out, CELL_ + cellNumber, CELL_LABEL_ + cellNumber, 1.0)
      }
    }

    out.close
  }

  def writeDocCellEdges(corpus: StoredCorpus, graphOutputPath: String) = {
    val out = new FileWriter(graphOutputPath)

    for(document <- corpus) {
      if(document.isTrain) {
        val cellNumber = TopoUtil.getCellNumber(document.getGoldCoord(), DPC)
        writeEdge(out, /*DOC_ + */document.getId, CELL_ + cellNumber, 1.0)
      }
    }

    out.close
  }

  def writeNGramDocEdges(corpus: StoredCorpus, graphOutputPath: String, stoplist: Set[String]) = {
    val out = new FileWriter(graphOutputPath, true)

    for(document <- corpus) {
      for(sentence <- document) {
        for(token <- sentence) {
          if(!stoplist.contains(token.getForm))
            writeEdge(out, UNI_ + token.getForm, document.getId, 1.0)
        }
      }
    }

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
    out.write(node1 + "\t" + node2 + "\t" + weight + "\n");
  }
}
