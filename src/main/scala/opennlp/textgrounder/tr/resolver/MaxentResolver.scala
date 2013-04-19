package opennlp.textgrounder.tr.resolver

import java.io._

import opennlp.maxent._
import opennlp.maxent.io._
import opennlp.model._

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import scala.collection.JavaConversions._

class MaxentResolver(val logFilePath:String,
                     val modelDirPath:String) extends Resolver {

  val windowSize = 20
  val dpc = 1.0

  def disambiguate(corpus:StoredCorpus): StoredCorpus = {

    val modelDir = new File(modelDirPath)

    val toponymsToModels:Map[String, AbstractModel] =
    (for(file <- modelDir.listFiles.filter(_.getName.endsWith(".mxm"))) yield {
      val dataInputStream = new DataInputStream(new FileInputStream(file));
      val reader = new BinaryGISModelReader(dataInputStream)
      val model = reader.getModel

      //println("Found model for "+file.getName.dropRight(4))

      (file.getName.dropRight(4).replaceAll("_", " "), model)
    }).toMap

    for(doc <- corpus) {
      val docAsArray = TextUtil.getDocAsArray(doc)
      var tokIndex = 0
      for(token <- docAsArray) {
        if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0
           && toponymsToModels.containsKey(token.getForm)) {
          val toponym = token.asInstanceOf[Toponym]
          val contextFeatures = TextUtil.getContextFeatures(docAsArray, tokIndex, windowSize, Set[String]())
          //print("Features for "+token.getForm+": ")
          //contextFeatures.foreach(f => print(f+","))
          //println
          //print("\n" + token.getForm+" ")
          val bestIndex = MaxentResolver.getBestIndex(toponymsToModels(token.getForm), contextFeatures)
          
          //val bestIndex = MaxentResolver.getCellDist(toponymsToModels(toponym.getForm), contextFeatures,
          //                           toponym.getCandidates.toList, dpc)
          //println("best index for "+token.getForm+": "+bestIndex)
          if(bestIndex != -1)
            token.asInstanceOf[Toponym].setSelectedIdx(bestIndex)
        }
        tokIndex += 1
      }
    }

    // Backoff to DocDist:
    val docDistResolver = new DocDistResolver(logFilePath)
    docDistResolver.overwriteSelecteds = false
    docDistResolver.disambiguate(corpus)

    corpus
  }
}

object MaxentResolver {
  def getBestIndex(model:AbstractModel, features:Array[String]): Int = {
    //print(candidates.map(c => TopoUtil.getCellNumber(c.getRegion.getCenter, dpc)) + " ")
    //val candCellNums = candidates.map(c => TopoUtil.getCellNumber(c.getRegion.getCenter, dpc)).toSet
    //candCellNums.foreach(println)
    //val cellNumToCandIndex = candidates.zipWithIndex
    //  .map(p => (TopoUtil.getCellNumber(p._1.getRegion.getCenter, dpc), p._2)).toMap
    //cellNumToCandIndex.foreach(p => println(p._1+": "+p._2))
    //val labels = model.getDataStructures()(2).asInstanceOf[Array[String]]
    //labels.foreach(l => print(l+","))
    //println
    //val result = model.eval(features)
    //result.foreach(r => print(r+","))
    //println
    //features.foreach(f => print(f+","))
    //result.foreach(r => print(r+" "))
    //println
    //val sortedResult = result.zipWithIndex.sortWith((x, y) => x._1 > y._1)

    //labels(sortedResult(0)._2).toInt

    //sortedResult.foreach(r => print(r+" "))
    /*for(i <- 0 until sortedResult.size) { // i should never make it past 0
      val label = labels(sortedResult(i)._2).toInt
      if(candCellNums contains label)
        return cellNumToCandIndex(label)
    }
    -1*/

    getIndexToWeightMap(model, features).maxBy(_._2)._1//.toList.sortBy(_._2).get(0)._1
  }

  def getIndexToWeightMap(model:AbstractModel, features:Array[String]): Map[Int, Double] = {
    val labels = model.getDataStructures()(2).asInstanceOf[Array[String]]
    val result = model.eval(features).zipWithIndex

    (for(p <- result) yield {
      (labels(p._2).toInt, p._1)
    }).toMap
  }

  /*def getCellDist(model:AbstractModel, features:Array[String], candidates:List[Location], dpc:Double): Map[Int, Double] = {
    val candCellNums = candidates.map(c => TopoUtil.getCellNumber(c.getRegion.getCenter, dpc)).toSet
    //candCellNums.foreach(n => print(n+","))
    //println

    val labels = model.getDataStructures()(2).asInstanceOf[Array[String]].map(_.toInt)
    //labels.foreach(l => print(l+","))
    //println
    val result = model.eval(features)
    //result.foreach(r => print(r+","))
    //println
    val relevantResult = result.zipWithIndex.filter(r => candCellNums contains labels(r._2))
    //relevantResult.foreach(r => print(r+","))
    //println
    val normFactor = relevantResult.map(_._1).sum
    /*val toReturn = */relevantResult.map(r => (labels(r._2), r._1 / normFactor)).toMap
    //toReturn.foreach(r => print(r+","))
    //println
    //toReturn
    
  }*/
}
