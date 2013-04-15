///////////////////////////////////////////////////////////////////////////////
//  PreprocWikiDump.scala
//
//  Copyright (C) 2012 Mike Speriosu, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////

package opennlp.textgrounder.preprocess

import opennlp.textgrounder.tr.util._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.app._
import opennlp.textgrounder.tr.topo.gaz._

import java.io._
//import java.util._
import java.util.zip._
import org.apache.commons.compress.compressors.bzip2._

import scala.collection.JavaConversions._

import org.clapper.argot._
import ArgotConverters._

object ExtractLinksFromWikiDump {

  //var MAX_COUNT = 0//100000
  val windowSize = 20
  val THRESHOLD = 10.0 / 6372.8 // 10km in radians

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.preprocess.ExtractLinksFromWikiDump", preUsage = Some("Textgrounder"))
  
  val articleNamesIDsCoordsFile = parser.option[String](List("a", "art"), "art", "wiki article IDs, titles, and coordinates (as output by ExtractGeotaggedListFromWikiDump)")
  val rawWikiInputFile = parser.option[String](List("w", "wiki"), "wiki", "raw wiki input file (bz2)")
  val trInputFile = parser.option[String](List("i", "tr-input"), "tr-input", "toponym resolution corpus input path")
  val gazInputFile = parser.option[String](List("g", "gaz"), "gaz", "serialized gazetteer input file")
  val stoplistInputFile = parser.option[String](List("s", "stoplist"), "stoplist", "stopwords input file")
  val linksOutputFile = parser.option[String](List("l", "links"), "links", "geotagged->geotagged link count output file")
  val trainingInstanceOutputDir = parser.option[String](List("d", "training-dir"), "training-dir", "training instance output directory")
  val maxCountOption = parser.option[Int](List("n", "max-count"), "max-count", "maximum number of lines to read (if unspecified, all will be read)")

  //val coordRE = """\|\s*latd|\|\s*lat_deg|\|\s*latG|\|\s*latitude|\{\{\s*Coord?|\|\s*Breitengrad|\{\{\s*Coordinate\s""".r

  val titleRE = """^\s{4}<title>(.*)</title>\s*$""".r
  val idRE = """^\s{4}<id>(.*)</id>\s*$""".r

  val listRE = """^(\d+)\t([^\t]+)\t(-?\d+\.?\d*),(-?\d+\.?\d*)$""".r

  //val linkAndContextRE = """((?:\S+\s*){0,20})?(\[\[[^\|\]]+)(\|?[^\|\]]+)?\]\]((?:\s*\S+){0,20})?""".r
  val tokenRE = """(?:\[\[(?:[^\|\]]+)?\|?(?:[^\|\]]+)\]\])|(?:\w[^ :&=;{}\|<>]*\w)""".r
  //val tokenRE = """(?:\[\[(?:[^\|\]]+)?\|?(?:[^\|\]]+)\]\])|\w+""".r
  //val linkRE = """^\[\[([^\|\]]+)?\|?([^\|\]]+)\]\]$""".r
  val linkRE = """^\[\[([^\|]+)(?:\|(.+))?\]\]$""".r

  val markupTokens = "nbsp,lt,gt,ref,br,thinsp,amp,url,deadurl,http,quot,cite".split(",").toSet

  def main(args: Array[String]) {

    try {
      parser.parse(args)
    }
    catch {
      case e: ArgotUsageException => println(e.message); sys.exit(0)
    }

    //println(rawWikiInputFile.value.get)

    println("Reading output from ExtractGeotaggedListFromWikiDump from " + articleNamesIDsCoordsFile.value.get + " ...");
    val articleNamesToIDsAndCoords =
    (for(line <- scala.io.Source.fromFile(articleNamesIDsCoordsFile.value.get).getLines) yield {
      line match {
        case listRE(id, name, lat, lon) => { Some((name, (id.toInt, Coordinate.fromDegrees(lat.toDouble, lon.toDouble)))) }
        case _ => None
      }
    }).flatten.toMap

    println("Reading toponyms from TR-CoNLL at " + trInputFile.value.get + " ...")
    val toponyms:Set[String] = CorpusInfo.getCorpusInfo(trInputFile.value.get).map(_._1).toSet

    val links = new scala.collection.mutable.HashMap[(Int, Int), Int] // (location1.id, location2.id) => count
    val toponymsToTrainingSets = new scala.collection.mutable.HashMap[String, List[String]]

    println("Reading serialized gazetteer from " + gazInputFile.value.get + " ...")
    val gis = new GZIPInputStream(new FileInputStream(gazInputFile.value.get))
    val ois = new ObjectInputStream(gis)
    val gnGaz = ois.readObject.asInstanceOf[GeoNamesGazetteer]
    gis.close

    val stoplist:Set[String] =
    if(stoplistInputFile.value != None) {
      println("Reading stopwords file from " + stoplistInputFile.value.get + " ...")
      scala.io.Source.fromFile(stoplistInputFile.value.get).getLines.toSet
    }
    else {
      println("No stopwords file specified. Using an empty stopword list.")
      Set()
    }

    val fileInputStream = new FileInputStream(new File(rawWikiInputFile.value.get))
    val cbzip2InputStream = new BZip2CompressorInputStream(fileInputStream)
    val in = new BufferedReader(new InputStreamReader(cbzip2InputStream))

    //var totalPageCount = 0
    //var geotaggedPageCount = 0
    //var lookingForCoord = false
    var lineCount = 0
    var pageTitle = ""
    var id = ""
    var line = in.readLine
    val maxCount = if(maxCountOption.value != None) maxCountOption.value.get else 0
    while(line != null && (maxCount <= 0 || lineCount < maxCount)) {

      if(titleRE.findFirstIn(line) != None) {
        val titleRE(t) = line
        pageTitle = t
        //totalPageCount += 1
        //if(totalPageCount % 10000 == 0)
        //  println(line+" "+geotaggedPageCount+"/"+totalPageCount)
        //lookingForCoord = true
      }

      if(idRE.findFirstIn(line) != None) {
        val idRE(i) = line
        id = i
      }

      val tokArray:Array[String] = (for(token <- tokenRE.findAllIn(line)) yield {
        if(markupTokens.contains(token))
          None
        else
          Some(token)
      }).flatten.toArray

      for(tokIndex <- 0 until tokArray.size) {
        val token = tokArray(tokIndex)
        token match {
          case linkRE(title,a) => {
            val titleLower = title.toLowerCase
            val anchor = if(a == null || a.trim.size == 0) title else a.trim

            val idAndCoord = articleNamesToIDsAndCoords.getOrElse(title, null)

            if(idAndCoord != null) {

              // Count the link if the current page is also geotagged:
              val thisIDAndCoord = articleNamesToIDsAndCoords.getOrElse(pageTitle, null)
              if(thisIDAndCoord != null) {
                val pair = (thisIDAndCoord._1, idAndCoord._1)
                val prevCount = links.getOrElse(pair, 0)
                links.put(pair, prevCount+1)
              }

              // Extract and write context:
              val closestGazIndexResult = getClosestGazIndex(gnGaz, titleLower, idAndCoord._2, idAndCoord._1)
              val matchingToponym = closestGazIndexResult._1
              if(toponyms(matchingToponym)) {
                val closestGazIndex = closestGazIndexResult._2
                if(closestGazIndex != -1) {
                  val context = getContextFeatures(tokArray, tokIndex, windowSize, stoplist)
                  if(context.size > 0) {
                    val strippedContext = closestGazIndexResult._3
                    //print(matchingToponym+": ")
                    //context.foreach(f => print(f+","))
                    //tokenRE.findAllIn(strippedContext).foreach(f => print(f+","))
                    //println(closestGazIndex)

                    val contextAndLabel:List[String] = (context.toList ::: tokenRE.findAllIn(strippedContext).toList) ::: (closestGazIndex.toString :: Nil)
                    val contextAndLabelString = contextAndLabel.mkString(",")
                    print(matchingToponym+": ")
                    println(contextAndLabelString)

                    val prevSet = toponymsToTrainingSets.getOrElse(matchingToponym, Nil)
                    toponymsToTrainingSets.put(matchingToponym, contextAndLabelString :: prevSet)
                  }
                }
              }
            }
          }
          case _ => //println(token)
        }
      }

      if(lineCount % 10001 == 10000) {
        println("------------------")
        println("Line number: "+lineCount)
        println("Current article: "+pageTitle+" ("+id+")")
        println("line.size: "+line.size)
        println("tokArray.size: "+tokArray.size)
        println("toponymsToTrainingSets.size: "+toponymsToTrainingSets.size)
        println("toponymsToTrainingSets biggest training set: "+toponymsToTrainingSets.map(p => p._2.size).max)
        println("links.size: "+links.size)
        //println("storedDistances.size: "+storedDistances.size)
        println("------------------")
      }

      
      line = in.readLine
      lineCount += 1
    }

    val dir =
      if(trainingInstanceOutputDir.value.get != None) {
        println("Outputting training instances to directory " + trainingInstanceOutputDir.value.get + " ...")
        val dirFile:File = new File(trainingInstanceOutputDir.value.get)
        if(!dirFile.exists)
          dirFile.mkdir
        if(trainingInstanceOutputDir.value.get.endsWith("/"))
          trainingInstanceOutputDir.value.get
        else
          trainingInstanceOutputDir.value.get+"/"
      }
      else {
        println("Outputting training instances to current working directory ...")
        ""
      }
    for((toponym, trainingSet) <- toponymsToTrainingSets) {
      val outFile = new File(dir + toponym.replaceAll(" ", "_")+".txt")
      val out = new BufferedWriter(new FileWriter(outFile))
      for(line <- trainingSet) {
        //for(feature <- context) out.write(feature+",")
        out.write(line+"\n")
      }
      out.close
    }
    
    println("Writing links and counts to "+(if(linksOutputFile.value != None) linksOutputFile.value.get else "links.dat")+" ...")
    val out = new DataOutputStream(new FileOutputStream(if(linksOutputFile.value != None) linksOutputFile.value.get else "links.dat"))
    for(((id1, id2), count) <- links) {
      out.writeInt(id1); out.writeInt(id2); out.writeInt(count)
    }
    out.close

    in.close

    println("All done.")
  }

  // (loc.id, article.id) -> distance
  //val storedDistances = new scala.collection.mutable.HashMap[(Int, Int), Double]

  // Returns string that matched in gazetteer and index of closest entry in gazetteer, and any context that was stripped off
  def getClosestGazIndex(gnGaz:GeoNamesGazetteer, name:String, coord:Coordinate, articleID:Int): (String, Int, String) = {
    val looseLookupResult = looseLookup(gnGaz, name)
    val candidates = looseLookupResult._2
    if(candidates != null) {
      var minDist = Double.PositiveInfinity
      var bestIndex = -1

      for(index <- 0 until candidates.size) {
        val loc = candidates(index)
        //val key = (loc.getId, articleID)
        val dist = 
          //if(storedDistances.contains(key)) storedDistances(key)
          //else {
            /*val distComputed = */loc.getRegion.distance(coord)
            //storedDistances.put(key, distComputed)
            //distComputed
          //}
        if(dist < THRESHOLD && dist < minDist) {
          minDist = dist
          bestIndex = index
        }
      }

      (looseLookupResult._1, bestIndex, looseLookupResult._3)
    }
    else
      (null, -1, "")
  }

  // Returns string that matched in gazetteer and list of candidates, and any context that was stripped off
  def looseLookup(gnGaz:GeoNamesGazetteer, name:String): (String, java.util.List[Location], String) = {

    var nameToReturn:String = null
    var listToReturn:java.util.List[Location] = null
    var strippedContext = ""

    val firstAttempt = gnGaz.lookup(name)
    if(firstAttempt != null) {
      nameToReturn = name
      listToReturn = firstAttempt
    }

    val commaIndex = name.indexOf(",")
    
    if(commaIndex != -1) {
      val nameBeforeComma = name.slice(0, commaIndex).trim
      val secondAttempt = gnGaz.lookup(nameBeforeComma)
      if(secondAttempt != null) {
        nameToReturn = nameBeforeComma
        listToReturn = secondAttempt
        strippedContext = name.drop(commaIndex+1).trim
      }
      else {
        val parenIndex = name.indexOf("(")
        val nameBeforeParen = name.slice(0, parenIndex).trim
        val thirdAttempt = gnGaz.lookup(nameBeforeParen)
        nameToReturn = nameBeforeParen
        listToReturn = thirdAttempt
        strippedContext = name.drop(parenIndex+1).trim
      }
    }

    (nameToReturn, listToReturn, strippedContext)
  }

  def getContextFeatures(tokArray:Array[String], tokIndex:Int, windowSize:Int, stoplist:Set[String]): Array[String] = {
    val startIndex = math.max(0, tokIndex - windowSize)
    val endIndex = math.min(tokArray.length, tokIndex + windowSize + 1)

    val linksIncluded = Array.concat(tokArray.slice(startIndex, tokIndex), tokArray.slice(tokIndex + 1, endIndex))//.filterNot(stoplist(_))

    // Remove link notation:
    linksIncluded.map(t => t match {
      case linkRE(title, a) => {
        val anchor = if(a == null || a.trim.size == 0) title else a.trim
        anchor.split(" ")
      }
      case _ => t.split(" ")
    }).flatten.map(_.toLowerCase).filterNot(stoplist(_))
  }

}
