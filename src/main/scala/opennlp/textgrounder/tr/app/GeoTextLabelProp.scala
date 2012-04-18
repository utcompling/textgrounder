package opennlp.textgrounder.tr.app

import java.io._

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.io._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.app._
import opennlp.textgrounder.tr.util.TopoUtil

import upenn.junto.app._
import upenn.junto.config._

import gnu.trove._

import scala.collection.JavaConversions._

object GeoTextLabelProp extends BaseApp {

  import BaseApp._

  val MIN_COUNT_THRESHOLD = 5

  val DPC = 1.0

  val CELL_ = "cell_"
  val CELL_LABEL_ = "cell_label_"
  val DOC_ = "doc_"
  val UNI_ = "uni_"
  val BI_ = "bi_"
  val NGRAM_ = "ngram_"
  val USER = "USER"
  val USER_ = "USER_"

  //val nodeRE = """(.+)_([^_]+)""".r   SAVE AND COMPILE

  def main(args: Array[String]) {

    this.initializeOptionsFromCommandLine(args)
    checkExists(getSerializedCorpusInputPath)

    val stoplist:Set[String] =
      if(getStoplistInputPath != null) scala.io.Source.fromFile(getStoplistInputPath).getLines.toSet
      else Set()

    val corpus = TopoUtil.readStoredCorpusFromSerialized(getSerializedCorpusInputPath)

    val graph = createGraph(corpus, stoplist)

    JuntoRunner(graph, 1.0, .01, .01, getNumIterations, false)

    val docIdsToCells = new collection.mutable.HashMap[String, Int]

    for ((id, vertex) <- graph._vertices) {
      //val nodeRE(nodeType,nodeId) = id

      //if(nodeType.equals(USER))
      if(id.startsWith(USER_))
        docIdsToCells.put(id, getGreatestCell(vertex.GetEstimatedLabelScores))
    }

    for(doc <- corpus.filter(d => (d.isDev || d.isTest) && docIdsToCells.containsKey(d.getId))) {
      val cellNumber = docIdsToCells(doc.getId)
      if(cellNumber != -1) {
        val lat = ((cellNumber / 1000) * DPC) + DPC/2.0
        val lon = ((cellNumber % 1000) * DPC) + DPC/2.0
        doc.setSystemCoord(Coordinate.fromDegrees(lat, lon))
      }
    }

    val eval = new EvaluateCorpus
    eval.doEval(corpus, corpus, CORPUS_FORMAT.GEOTEXT)
  }

  def getGreatestCell(estimatedLabelScores: TObjectDoubleHashMap[String]): Int = {

    estimatedLabelScores.keys(Array[String]()).filter(_.startsWith(CELL_LABEL_)).maxBy(estimatedLabelScores.get(_)).substring(CELL_LABEL_.length).toInt

  }

  def createGraph(corpus: StoredCorpus, stoplist: Set[String]) = {
    val edges = getDocCellEdges(corpus) ::: getNgramDocEdges(corpus, stoplist)
    val seeds = getCellSeeds(corpus)
    GraphBuilder(edges, seeds)
  }

  def getCellSeeds(corpus: StoredCorpus): List[Label] = {
    (for (doc <- corpus.filter(_.isTrain)) yield {
      val cellNumber = TopoUtil.getCellNumber(doc.getGoldCoord, DPC)
      new Label(CELL_ + cellNumber, CELL_LABEL_ + cellNumber, 1.0)
    }).toList
  }

  def getDocCellEdges(corpus: StoredCorpus): List[Edge] = {
    (corpus.filter(_.isTrain).map(doc => new Edge(doc.getId, CELL_ + TopoUtil.getCellNumber(doc.getGoldCoord, DPC), 1.0))).toList
  }

  def getNgramDocEdges(corpus: StoredCorpus, stoplist: Set[String]): List[Edge] = {
    
    val ngramsToCounts = new collection.mutable.HashMap[String, Int] { override def default(s: String) = 0 }
    val docIdsToNgrams = new collection.mutable.HashMap[String, collection.mutable.HashSet[String]] {
      override def default(s: String) = new collection.mutable.HashSet
    }

    for(doc <- corpus) {
      for(sent <- doc) {
        
        val unigrams = (for(token <- sent) yield token.getForm).toList
        val bigrams = if(unigrams.length >= 2) (for(bi <- unigrams.sliding(2)) yield bi(0)+" "+bi(1)).toList else Nil
        val filteredUnigrams = unigrams.filterNot(stoplist.contains(_))
        
        for(ngram <- filteredUnigrams ::: bigrams) {
          ngramsToCounts.put(ngram, ngramsToCounts(ngram) + 1)
          docIdsToNgrams.put(doc.getId, docIdsToNgrams(doc.getId) + ngram)
        }
      }
    }

    (for(docId <- docIdsToNgrams.keys) yield
      (for(ngram <- docIdsToNgrams(docId).filter(ngramsToCounts(_) >= MIN_COUNT_THRESHOLD)) yield
        new Edge(NGRAM_ + ngram, docId, 1.0)).toList).toList.flatten
  }

}
