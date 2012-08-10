package opennlp.textgrounder.tr.resolver

import java.io._

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import opennlp.maxent._
import opennlp.maxent.io._
import opennlp.model._

import scala.collection.JavaConversions._

class ProbabilisticResolver(val logFilePath:String,
                            val modelDirPath:String) extends Resolver {

  val KNN = -1
  val DPC = 1.0
  val WINDOW_SIZE = 20

  def disambiguate(corpus:StoredCorpus): StoredCorpus = {

  val docIdToCellDist:Map[String, Map[Int, Double]] =
  (for(pe <- LogUtil.parseLogFile(logFilePath)) yield {
    (pe.docName, pe.getProbDistOverPredCells(KNN, DPC).toMap)
  }).toMap

  val predDocLocations = (for(pe <- LogUtil.parseLogFile(logFilePath)) yield {
    (pe.docName, pe.predCoord)
  }).toMap

  val modelDir = new File(modelDirPath)

  val toponymsToModels:Map[String, AbstractModel] =
  (for(file <- modelDir.listFiles.filter(_.getName.endsWith(".mxm"))) yield {
    val dataInputStream = new DataInputStream(new FileInputStream(file));
    val reader = new BinaryGISModelReader(dataInputStream)
    val model = reader.getModel

    //println(file.getName.dropRight(4))
    (file.getName.dropRight(4), model)
  }).toMap

  var toponymsToCounts = new scala.collection.mutable.HashMap[String, Int]
  var total = 0
  for(doc <- corpus) {
    for(sent <- doc) {
      for(token <- sent) {
        if(token.isToponym) {
          val prevCount = toponymsToCounts.getOrElse(token.getForm, 0)
          toponymsToCounts.put(token.getForm, prevCount + 1)
        }
        total += 1
      }
    }
  }

  val toponymsToFrequencies = toponymsToCounts.map(p => (p._1, p._2.toDouble / total)).toMap
  toponymsToCounts.clear // trying to free up some memory
  toponymsToCounts = null
  //toponymsToFrequencies.foreach(p => println(p._1+": "+p._2))

  for(doc <- corpus) {
    val docAsArray = TextUtil.getDocAsArray(doc)
    var tokIndex = 0
    for(token <- docAsArray) {
      if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0) {
        val toponym = token.asInstanceOf[Toponym]

        // P(l|t,d_c(t))
        val cellDistGivenLocalContext =
        if(toponymsToModels.containsKey(toponym.getForm)) {
          val contextFeatures = TextUtil.getContextFeatures(docAsArray, tokIndex, WINDOW_SIZE, Set[String]())

          //println("getting a cell dist for "+toponym.getForm)

          /*val d = */MaxentResolver.getCellDist(toponymsToModels(toponym.getForm), contextFeatures,
                                     toponym.getCandidates.toList, DPC)
          //println(d.size)
          //d.foreach(println)
          //d
        }
        else
          null

        // P(l|d)
        val cellDistGivenDocument = docIdToCellDist.getOrElse(doc.getId, null)

        val topFreq = toponymsToFrequencies(toponym.getForm)
        val lambda = topFreq / (topFreq + 1.0E-3)//0.7

        var indexToSelect = -1
        var maxProb = 0.0
        var candIndex = 0
        for(cand <- toponym.getCandidates) {
          val curCellNum = TopoUtil.getCellNumber(cand.getRegion.getCenter, DPC)

          val localContextComponent =
          if(cellDistGivenLocalContext != null)
            cellDistGivenLocalContext.getOrElse(curCellNum, 0.0)
          else
            0.0

          val documentComponent =
          if(cellDistGivenDocument != null)
            cellDistGivenDocument.getOrElse(curCellNum, 0.0)
          else
            0.0

          /*if(localContextComponent == 0.0) {
            if(documentComponent == 0.0) {
              println("BOTH ZERO")
            }
            else {
              println("LOCAL ZERO")
            }
          }
          else if(documentComponent == 0.0)
            println("DOC ZERO")*/

          // Incorporate administrative level here
          val adminLevelComponent = getAdminLevelComponent(cand.getType, cand.getAdmin1Code)

          // P(l|t,d)
          val probOfLocation = adminLevelComponent * (lambda * localContextComponent + (1-lambda) * documentComponent)

          if(probOfLocation > maxProb) {
            indexToSelect = candIndex
            maxProb = probOfLocation
          }

          candIndex += 1
        }

        /*if(indexToSelect == -1) {
          val predDocLocation = predDocLocations.getOrElse(doc.getId, null)
          if(predDocLocation != null) {
            val indexToSelectBackoff = toponym.getCandidates.zipWithIndex.minBy(p => p._1.getRegion.distance(predDocLocation))._2
            if(indexToSelectBackoff != -1) {
              indexToSelect = indexToSelectBackoff
            }
          }
        }*/

        if(indexToSelect >= 0)
          toponym.setSelectedIdx(indexToSelect)

      }
      tokIndex += 1
    }
  }
    
  corpus
  }

  //val countryRE = """^\w\w\.\d\d$""".r
  val usStateRE = """^US\.[A-Za-z][A-Za-z]$""".r

  def getAdminLevelComponent(locType:Location.Type, admin1Code:String): Double = {
    if(locType == Location.Type.STATE) {
      if(usStateRE.findFirstIn(admin1Code) != None) {
        //println(admin1Code+" .2")
        .2
      }
      else {
        //println(admin1Code+" .7")
        .7
      }
    }
    else if(locType == Location.Type.CITY) {
      //println("CITY")
      0.095
    }
    else {
      //println("ELSE")
      0.005
    }
  }

}
