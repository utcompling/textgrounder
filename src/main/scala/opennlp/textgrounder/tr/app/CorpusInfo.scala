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

object CorpusInfo {

  val DPC = 1.0

  def getCorpusInfo(filename: String/* useNER:Boolean = false*/): collection.mutable.HashMap[String, collection.mutable.HashMap[Int, Int]] = {

    val corpus = if(filename.endsWith(".ser.gz")) TopoUtil.readStoredCorpusFromSerialized(filename)
                 else Corpus.createStoredCorpus

    /*if(useNER) {

      //System.out.println("Reading serialized GeoNames gazetteer from " + gazPath + " ...")

      val gis = new GZIPInputStream(new FileInputStream(gazPath))
      val ois = new ObjectInputStream(gis)

      val gnGaz = ois.readObject.asInstanceOf[GeoNamesGazetteer]

      corpus.addSource(new ToponymAnnotator(new ToponymRemover(new TrXMLDirSource(new File(filename), new OpenNLPTokenizer)), new OpenNLPRecognizer, gnGaz, null))
    }*/
    if(!filename.endsWith(".ser.gz")) {
      corpus.addSource(new TrXMLDirSource(new File(filename), new OpenNLPTokenizer))
      corpus.setFormat(BaseApp.CORPUS_FORMAT.TRCONLL)
      corpus.load
    }

    
    val topsToCellCounts = new collection.mutable.HashMap[String, collection.mutable.HashMap[Int, Int]]
    
    if(filename.endsWith(".ser.gz")) {
      for(doc <- corpus) {
        for(sent <- doc) {
          for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)/*.filter(_.hasGold)*/) {
            val cellCounts = topsToCellCounts.getOrElse(toponym.getForm, new collection.mutable.HashMap[Int, Int])
            val cellNum = if(toponym.hasGold) TopoUtil.getCellNumber(toponym.getGold.getRegion.getCenter, DPC) else 0
            if(cellNum != -1) {
              val prevCount = cellCounts.getOrElse(cellNum, 0)
              cellCounts.put(cellNum, prevCount + 1)
              topsToCellCounts.put(toponym.getForm, cellCounts)
            }
          }
        }
      }
    }

    else {
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
    }
    

    topsToCellCounts
  }

  def printCorpusInfo(topsToCellCounts: collection.mutable.HashMap[String, collection.mutable.HashMap[Int, Int]]) {

    for((topForm, cellCounts) <- topsToCellCounts.toList.sortWith((x, y) => if(x._2.size != y._2.size)
                                                                              x._2.size > y._2.size
                                                                            else x._1 < y._1) ) {
      print(topForm+": ")
      cellCounts.toList.sortWith((x, y) => if(x._2 != y._2) x._2 > y._2 else x._1 < y._1)
        .foreach(p => print("["+TopoUtil.getCellCenter(p._1, DPC)+":"+p._2+"] "))
      println
    }
  }

  def printCollapsedCorpusInfo(topsToCellCounts: collection.mutable.HashMap[String, collection.mutable.HashMap[Int, Int]]) {

    topsToCellCounts.toList.map(p => (p._1, p._2.toList.map(q => q._2).sum)).sortWith((x, y) => if(x._2 != y._2)
                                                                                                  x._2 > y._2
                                                                                                else x._1 < y._1).
      foreach(p => println(p._1+" "+p._2))


    /*for((topForm, cellCounts) <- topsToCellCounts.toList.sortWith((x, y) => if(x._2.size != y._2.size)
                                                                              x._2.size > y._2.size
                                                                            else x._1 < y._1) ) {
      print(topForm+": ")
      cellCounts.toList.sortWith((x, y) => if(x._2 != y._2) x._2 > y._2 else x._1 < y._1)
        .foreach(p => print("["+TopoUtil.getCellCenter(p._1, DPC)+":"+p._2+"] "))
      println
    }*/
  }

  def main(args:Array[String]) = {
    //if(args.length >= 2)
      printCollapsedCorpusInfo(getCorpusInfo(args(0)))
    //else
    //  printCollapsedCorpusInfo(getCorpusInfo(args(0)))
  }

}
