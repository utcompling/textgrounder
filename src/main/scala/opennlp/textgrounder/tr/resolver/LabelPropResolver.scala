package opennlp.textgrounder.tr.resolver

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import upenn.junto.app._
import upenn.junto.config._

import gnu.trove._

import scala.collection.JavaConversions._

class LabelPropResolver(
  val logFilePath:String,
  val knn:Int) extends Resolver {

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

    JuntoRunner(graph, 1.0, .01, .01, 10, false)

    // Interpret output graph and setSelectedIdx of toponyms accordingly:

    val tokensToCells =
    (for ((id, vertex) <- graph._vertices) yield {
      if(docTokNodeRE.findFirstIn(id) != None) {
        val docTokNodeRE(docid, tokidx) = id
        //println(DOC+docid+"_"+TOK+tokidx+"  "+getGreatestCell(vertex.GetEstimatedLabelScores))
        //println(id)
        /*val scores = vertex.GetEstimatedLabelScores
        val cellScorePairs = (for(key <- scores.keys) yield {
          (key,scores.get(key.toString))
        })
        cellScorePairs.sortWith((x, y) => x._2 > y._2).foreach(x => print(x._1+":"+x._2+"   "))
        println
        println(getGreatestCell(vertex.GetEstimatedLabelScores))*/
        Some(((docid, tokidx.toInt), getGreatestCell(vertex.GetEstimatedLabelScores, nonemptyCellNums.toSet)))
      }
      else {
        //println(id)
        None
      }
    }).flatten.toMap

    for(doc <- corpus) {
      var tokenIndex = -1
      for(sent <- doc) {
        for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) {
          tokenIndex += 1
          val predCell = tokensToCells.getOrElse((doc.getId, tokenIndex), -1)
          if(predCell != -1) {
            val indexToSelect = TopoUtil.getCorrectCandidateIndex(toponym, predCell, DPC)
            if(indexToSelect != -1) {
              toponym.setSelectedIdx(indexToSelect)
              //println(toponym.getSelected)
            }
          }
        }
      }
    }

    corpus
  }

  def getGreatestCell(estimatedLabelScores: TObjectDoubleHashMap[String], nonemptyCellNums: Set[Int]): Int = {
    estimatedLabelScores.keys(Array[String]()).filter(_.startsWith(CELL_LABEL))
      .map(_.drop(CELL_LABEL.length).toInt).filter(nonemptyCellNums(_)).maxBy(x => estimatedLabelScores.get(CELL_LABEL+x))
  }

  def createGraph(corpus:StoredCorpus) = {
    val edges:List[Edge] = getEdges(corpus)
    val seeds:List[Label] = getSeeds(corpus)

    GraphBuilder(edges, seeds)
  }

  def getEdges(corpus:StoredCorpus): List[Edge] = {
    getTokDocEdges(corpus) :::
    //getTokTokEdges(corpus) ::: 
    //getTokDocTypeEdges(corpus) :::
    //getDocTypeGlobalTypeEdges(corpus) :::
    getTokGlobalTypeEdges(corpus) ::: // alternative to the above two edge sets
    getGlobalTypeLocationEdges(corpus) :::
    getLocationCellEdges(corpus) :::
    getCellCellEdges
  }

  def getTokDocEdges(corpus:StoredCorpus): List[Edge] = {
    var result =
    (for(doc <- corpus) yield {
      var tokenIndex = -1
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) yield {
          tokenIndex += 1
          new Edge(DOC+doc.getId, DOC+doc.getId+"_"+TOK+tokenIndex, 1.0)
        })
      }).flatten
    }).flatten.toList
    //result.foreach(println)
    result
  }

  def getTokTokEdges(corpus:StoredCorpus): List[Edge] = {
    var result =
    (for(doc <- corpus) yield {
      var prevTok:String = null
      var tokenIndex = -1
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) yield {
          tokenIndex += 1
          val curTok = DOC+doc.getId+"_"+TOK+tokenIndex
          val edge =
          if(prevTok != null)
            Some(new Edge(prevTok, curTok, 1.0))
          else
            None
          prevTok = curTok
          edge
        }).flatten
      }).flatten
    }).flatten.toList
    //result.foreach(println)
    result
  }

  def getTokDocTypeEdges(corpus:StoredCorpus): List[Edge] = {
    val result = 
    (for(doc <- corpus) yield {
      var tokenIndex = -1
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) yield {
          tokenIndex += 1
          new Edge(DOC+doc.getId+"_"+TOK+tokenIndex, DOC+doc.getId+"_"+TYPE+toponym.getForm, 1.0)
        })
      }).flatten
    }).flatten.toList
    //result.foreach(println)
    result
  }

  def getDocTypeGlobalTypeEdges(corpus:StoredCorpus): List[Edge] = {
    val result = (for(doc <- corpus) yield {
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) yield {
          new Edge(DOC+doc.getId+"_"+TYPE+toponym.getForm, TPNM_TYPE+toponym.getForm, 1.0)
        })
      }).flatten
    }).flatten.toList
    //result.foreach(println)
    result
  }

  // This is an alternative to using BOTH of the above two edge sets:
  //   Don't instantiate DocType edges and go straight from tokens to global types
  def getTokGlobalTypeEdges(corpus:StoredCorpus): List[Edge] = {
    val result = 
    (for(doc <- corpus) yield {
      var tokenIndex = -1
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) yield {
          tokenIndex += 1
          new Edge(DOC+doc.getId+"_"+TOK+tokenIndex, TPNM_TYPE+toponym.getForm, 1.0)
        })
      }).flatten
    }).flatten.toList
    //result.foreach(println)
    result
  }

  def getGlobalTypeLocationEdges(corpus:StoredCorpus): List[Edge] = {
    val result = (for(doc <- corpus) yield {
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) yield {
          (for(loc <- toponym.getCandidates) yield {
            new Edge(TPNM_TYPE+toponym.getForm, LOC+loc.getId, 1.0)
          })
        }).flatten
      }).flatten
    }).flatten.toList
    //result.foreach(println)
    result
  }

  def getLocationCellEdges(corpus:StoredCorpus): List[Edge] = {
    val result = (for(doc <- corpus) yield {
      (for(sent <- doc) yield {
        (for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) yield {
          (for(loc <- toponym.getCandidates) yield {
            (for(cellNum <- TopoUtil.getCellNumbers(loc, DPC)) yield {
              nonemptyCellNums.add(cellNum)
              new Edge(LOC+loc.getId, CELL+cellNum, 1.0)
            })
          }).flatten
        }).flatten
      }).flatten
    }).flatten.toList

    //result.foreach(println)

    result
  }

  def getCellCellEdges: List[Edge] = {
    var lon = 0.0
    var lat = 0.0
    val edges = new collection.mutable.ListBuffer[Edge]
    while(lon < 360.0/DPC) {
      lat = 0.0
      while(lat < 180.0/DPC) {
        val curCellNumber = TopoUtil.getCellNumber(lat, lon, DPC)
        //if(nonemptyCellNums contains curCellNumber) {
          val leftCellNumber = TopoUtil.getCellNumber(lat, lon - DPC, DPC)
          //if(nonemptyCellNums contains leftCellNumber)
            edges.append(new Edge(CELL+curCellNumber, CELL+leftCellNumber, 1.0))
          val rightCellNumber = TopoUtil.getCellNumber(lat, lon + DPC, DPC)
          //if(nonemptyCellNums contains rightCellNumber)
            edges.append(new Edge(CELL+curCellNumber, CELL+rightCellNumber, 1.0))
          val topCellNumber = TopoUtil.getCellNumber(lat + DPC, lon, DPC)
          //if(nonemptyCellNums contains topCellNumber)
            edges.append(new Edge(CELL+curCellNumber, CELL+topCellNumber, 1.0))
          val bottomCellNumber = TopoUtil.getCellNumber(lat - DPC, lon, DPC)
          //if(nonemptyCellNums contains bottomCellNumber)
            edges.append(new Edge(CELL+curCellNumber, CELL+bottomCellNumber, 1.0))
        //}
        lat += DPC
      }
      lon += DPC
    }
    //edges.foreach(println)
    edges.toList
  }

  def getSeeds(corpus:StoredCorpus): List[Label] = {
    getCellCellLabelSeeds(corpus) ::: getDocCellLabelSeeds
  }

  def getCellCellLabelSeeds(corpus:StoredCorpus): List[Label] = {
    val result = (for(cellNum <- nonemptyCellNums) yield {
      new Label(CELL+cellNum, CELL_LABEL+cellNum, 1.0)
    }).toList
    //result.foreach(println)
    result
  }

  def getDocCellLabelSeeds: List[Label] = {
    val result =
    if(logFilePath != null) {
      (for(pe <- LogUtil.parseLogFile(logFilePath)) yield {
        (for((cellNum, probMass) <- pe.getProbDistOverPredCells(knn, DPC)) yield {
          new Label(DOC+pe.docName, CELL_LABEL+cellNum, probMass)
        })
      }).flatten.toList
    }
    else
      Nil
    //result.foreach(println)
    result
  }

}
