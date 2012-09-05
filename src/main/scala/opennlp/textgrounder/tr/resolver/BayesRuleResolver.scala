package opennlp.textgrounder.tr.resolver

import java.io._

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import opennlp.maxent._
import opennlp.maxent.io._
import opennlp.model._

import scala.collection.JavaConversions._

class BayesRuleResolver(val logFilePath:String,
                        val modelDirPath:String) extends Resolver {

  val DPC = 1.0
  val WINDOW_SIZE = 20

  def disambiguate(corpus:StoredCorpus): StoredCorpus = {

    val modelDir = new File(modelDirPath)

    val toponymsToModels:Map[String, AbstractModel] =
      (for(file <- modelDir.listFiles.filter(_.getName.endsWith(".mxm"))) yield {
        val dataInputStream = new DataInputStream(new FileInputStream(file));
        val reader = new BinaryGISModelReader(dataInputStream)
        val model = reader.getModel
        
        //println(file.getName.dropRight(4).replaceAll("_", " "))
        (file.getName.dropRight(4).replaceAll("_", " "), model)
      }).toMap

    val ngramDists = LogUtil.getNgramDists(logFilePath)

    for(doc <- corpus) {
      val docAsArray = TextUtil.getDocAsArray(doc)
      var tokIndex = 0
      for(token <- docAsArray) {
        if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0) {
        val toponym = token.asInstanceOf[Toponym]

        // P(l|d_c(t))
        val cellDistGivenLocalContext =
        if(toponymsToModels.containsKey(toponym.getForm)) {
          val contextFeatures = TextUtil.getContextFeatures(docAsArray, tokIndex, WINDOW_SIZE, Set[String]())

          /*val d = */MaxentResolver.getCellDist(toponymsToModels(toponym.getForm), contextFeatures,
                                     toponym.getCandidates.toList, DPC)
        }
        else
          null

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

          val dist = ngramDists.getOrElse(curCellNum, null)
          val probOfDocGivenLocation = 
          if(dist != null) {
            val denom = dist.map(_._2 + 1.0).sum + 1.0 // add 1 smoothing
            (for(word <- docAsArray.map(_.getForm.split(" ")).flatten) yield {
              dist.getOrElse(word, 1.0) / denom
            }).product
          }
          else
            0.0

          val probOfLocation = localContextComponent * probOfDocGivenLocation

          if(probOfLocation > maxProb) {
            indexToSelect = candIndex
            maxProb = probOfLocation
          }

          candIndex += 1
        }

        if(indexToSelect >= 0)
          toponym.setSelectedIdx(indexToSelect)
        }
      }
      tokIndex += 1
    }

    // Backoff to DocDist:
    val docDistResolver = new DocDistResolver(logFilePath)
    docDistResolver.overwriteSelecteds = false
    docDistResolver.disambiguate(corpus)

    corpus
  }
}
