package opennlp.textgrounder.tr.resolver

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import upenn.junto.app._
import upenn.junto.config._

import gnu.trove._

import scala.collection.JavaConversions._

class LabelPropResolver extends Resolver {

  val DPC = 1.0

  val DOC = "doc_"
  val TYPE = "type_"
  val TOK = "tok_"
  val LOC = "loc_"
  val TPNM_TYPE = "tpnm_type_"
  val CELL = "cell_"
  val CELL_LABEL = "cell_label_"

  val nonemptyCellNums = new scala.collection.mutable.HashSet[Int]()

  val docTokNodeRE = """^doc_(.+)_tok_(.+)$""".r

  def disambiguate(corpus:StoredCorpus): StoredCorpus = {
    
    val graph = createGraph(corpus)

    JuntoRunner(graph, .01, .01, .01, 100, false)

    // Interpret output graph and setSelectedIdx of toponyms accordingly:

    val tokensToCells =
    (for ((id, vertex) <- graph._vertices) yield {
      if(docTokNodeRE.findFirstIn(id) != None) {
        val docTokNodeRE(docid, tokidx) = id
        Some(((docid, tokidx.toInt), getGreatestCell(vertex.GetEstimatedLabelScores)))
      }
      else
        None
    }).flatten.toMap

    for(doc <- corpus) {
      for(sent <- doc) {
        var tokenIndex = -1
        for(toponym <- sent.getToponyms) {
          tokenIndex += 1
          val predCell = tokensToCells.getOrElse((doc.getId, tokenIndex), -1)
          if(predCell != -1) {
            val indexToSelect = TopoUtil.getCorrectCandidateIndex(toponym, predCell, DPC)
            if(indexToSelect != -1)
              toponym.setSelectedIdx(indexToSelect)
          }
        }
      }
    }

    corpus
  }

  def getGreatestCell(estimatedLabelScores: TObjectDoubleHashMap[String]): Int = {
    estimatedLabelScores.keys(Array[String]()).filter(_.startsWith(CELL_LABEL))
      .maxBy(estimatedLabelScores.get(_)).substring(CELL_LABEL.length).toInt
  }

  def createGraph(corpus:StoredCorpus) = {
    val edges:List[Edge] = getEdges(corpus)
    val seeds:List[Label] = getSeeds(corpus)

    GraphBuilder(edges, seeds)
  }

  def getEdges(corpus:StoredCorpus): List[Edge] = {
    getTokDocEdges(corpus:StoredCorpus) ::: getTokDocTypeEdges(corpus) :::
    getDocTypeGlobalTypeEdges(corpus) ::: getGlobalTypeLocationEdges(corpus) :::
    getLocationCellEdges(corpus)
  }

  def getTokDocEdges(corpus:StoredCorpus): List[Edge] = {
    (for(doc <- corpus) yield {
      (for(sent <- doc) yield {
        var tokenIndex = -1
        (for(toponym <- sent.getToponyms) yield {
          tokenIndex += 1
          new Edge(DOC+doc.getId, DOC+doc.getId+"_"+TOK+tokenIndex, 1.0)
        })
      }).flatten
    }).flatten.toList
  }

  def getTokDocTypeEdges(corpus:StoredCorpus): List[Edge] = {
    (for(doc <- corpus) yield {
      (for(sent <- doc) yield {
        var tokenIndex = -1
        (for(toponym <- sent.getToponyms) yield {
          tokenIndex += 1
          new Edge(DOC+doc.getId+"_"+TOK+tokenIndex, TPNM_TYPE+toponym.getForm, 1.0)
        })
      }).flatten
    }).flatten.toList
  }

  def getDocTypeGlobalTypeEdges(corpus:StoredCorpus): List[Edge] = {
    (for(doc <- corpus) yield {
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms) yield {
          new Edge(DOC+doc.getId+"_"+TYPE+toponym.getForm, TPNM_TYPE+toponym.getForm, 1.0)
        })
      }).flatten
    }).flatten.toList
  }

  def getGlobalTypeLocationEdges(corpus:StoredCorpus): List[Edge] = {
    (for(doc <- corpus) yield {
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms) yield {
          (for(loc <- toponym.getCandidates) yield {
            new Edge(TPNM_TYPE+toponym.getForm, LOC+loc.getId, 1.0)
          })
        }).flatten
      }).flatten
    }).flatten.toList
  }

  def getLocationCellEdges(corpus:StoredCorpus): List[Edge] = {
    (for(doc <- corpus) yield {
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms) yield {
          (for(loc <- toponym.getCandidates) yield {
            (for(cellNum <- TopoUtil.getCellNumbers(loc, DPC)) yield {
              nonemptyCellNums.add(cellNum)
              new Edge(LOC+loc.getId, CELL+cellNum, 1.0)
            })
          }).flatten
        }).flatten
      }).flatten
    }).flatten.toList
  }

  def getSeeds(corpus:StoredCorpus): List[Label] = {
    (for(cellNum <- nonemptyCellNums) yield {
      new Label(CELL+cellNum, CELL_LABEL+cellNum, 1.0)
    }).toList
  }

}
