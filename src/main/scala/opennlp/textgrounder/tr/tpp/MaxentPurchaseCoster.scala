package opennlp.textgrounder.tr.tpp

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.util._
import opennlp.textgrounder.tr.resolver._
import opennlp.maxent._
import opennlp.maxent.io._
import opennlp.model._

import java.io._

import scala.collection.JavaConversions._

class MaxentPurchaseCoster(corpus:StoredCorpus, modelDirPath:String) extends PurchaseCoster {

  val windowSize = 20

  val modelDir = new File(modelDirPath)

  val toponymsToModels:Map[String, AbstractModel] =
  (for(file <- modelDir.listFiles.filter(_.getName.endsWith(".mxm"))) yield {
    val dataInputStream = new DataInputStream(new FileInputStream(file));
    val reader = new BinaryGISModelReader(dataInputStream)
    val model = reader.getModel
    
    (file.getName.dropRight(4).replaceAll("_", " "), model)
  }).toMap

  val potLocsToCosts = new scala.collection.mutable.HashMap[PotentialLocation, Double]

  for(doc <- corpus) {
    val docAsArray = TextUtil.getDocAsArrayNoFilter(doc)
    var tokIndex = 0
      for(token <- docAsArray) {
        if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0
           && toponymsToModels.containsKey(token.getForm)) {
          val toponym = token.asInstanceOf[Toponym]
          val contextFeatures = TextUtil.getContextFeatures(docAsArray, tokIndex, windowSize, Set[String]())

          val indexToWeightMap = MaxentResolver.getIndexToWeightMap(toponymsToModels(token.getForm), contextFeatures)
          //contextFeatures.foreach(f => print(f+",")); println
          for((gazIndex, weight) <- indexToWeightMap.toList.sortBy(_._1)) {
            val loc = toponym.getCandidates.get(gazIndex)
            val potLoc = new PotentialLocation(doc.getId, tokIndex, gazIndex, loc)
            //println(" "+gazIndex+": "+(1.0-weight))
            potLocsToCosts.put(potLoc, 1.0-weight) // Here's where the cost is defined in terms of the probability mass
          }
          
        }
        tokIndex += 1
      }
  }
  
  def apply(m:Market, potLoc:PotentialLocation): Double = {
    //if(m.locations.map(_._2).toSet.contains(potLoc)) {
      potLocsToCosts.getOrElse(potLoc, 1.0) // Not sure what the default cost should be
    //}
    //else
    //  Double.PositiveInfinity
  }
}
