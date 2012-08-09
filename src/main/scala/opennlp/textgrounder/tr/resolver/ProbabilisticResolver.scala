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

    (file.getName.dropRight(4), model)
  }).toMap

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

          MaxentResolver.getCellDist(toponymsToModels(toponym.getForm), contextFeatures,
                                     toponym.getCandidates.toList, DPC)
        }
        else
          null

        // P(l|d)
        val cellDistGivenDocument = docIdToCellDist.getOrElse(doc.getId, null)

        val lambda = 0.7 // this will vary later, perhaps based on the frequency of the toponym in the training and/or eval data

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

          // P(l|t,d)
          val probOfLocation = lambda * localContextComponent + (1-lambda) * documentComponent

          // Need to incorporate administrative level here ( P(a|t,d) ), summing over all possible values for a and multiplying with P(l|t,d)

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

}
