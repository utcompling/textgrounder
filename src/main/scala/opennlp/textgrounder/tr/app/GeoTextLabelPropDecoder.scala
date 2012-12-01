package opennlp.textgrounder.tr
package app

import java.io._
import java.util._

import text._
import text.io._
import text.prep._
import topo._
import app._
import util.TopoUtil

import scala.collection.JavaConversions._

object GeoTextLabelPropDecoder extends BaseApp {

  import BaseApp._

  def DPC = 1.0

  def CELL_ = "cell_"
  def CELL_LABEL_ = "cell_label_"
  //def DOC_ = "doc_"
  def USER_ = "USER_"
  def UNI_ = "uni_"
  def BI_ = "bi_"

  def main(args: Array[String]) = {

    this.initializeOptionsFromCommandLine(args)
    this.doDecode

  }

  def doDecode() = {
    checkExists(getSerializedCorpusInputPath)
    checkExists(getGraphInputPath)

    val corpus = TopoUtil.readStoredCorpusFromSerialized(getSerializedCorpusInputPath)

    val docIdsToCells = new collection.mutable.HashMap[String, Int]

    val lines = scala.io.Source.fromFile(getGraphInputPath).getLines

    for(line <- lines) {
      val tokens = line.split("\t")

      if(tokens.length >= 4 && tokens(0).startsWith(USER_)) {
        val docId = tokens(0)

        val innertokens = tokens(3).split(" ")

        docIdsToCells.put(docId, findGreatestCell(innertokens))
      }
    }

    for(document <- corpus) {
      if(document.isDev || document.isTest) {
        if(docIdsToCells.containsKey(document.getId)) {
          val cellNumber = docIdsToCells(document.getId)
          if(cellNumber != -1) {
            val lat = ((cellNumber / 1000) * DPC) + DPC/2.0
            val lon = ((cellNumber % 1000) * DPC) + DPC/2.0
            document.setSystemCoord(Coordinate.fromDegrees(lat, lon))
          }
        }
      }
    }

    val eval = new EvaluateCorpus
    eval.doEval(corpus, corpus, CORPUS_FORMAT.GEOTEXT, true)
  }

  def findGreatestCell(innertokens: Array[String]): Int = {
    
    for(innertoken <- innertokens) {
      if(innertoken.startsWith(CELL_LABEL_)) {
        return innertoken.substring(CELL_LABEL_.length).toInt
      }
    }
    
    return -1
  }

}
