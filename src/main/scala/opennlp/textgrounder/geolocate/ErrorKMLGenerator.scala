///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 The University of Texas at Austin
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

////////
//////// ErrorKMLGenerator.scala
////////
//////// Copyright (c) 2011.
////////

package opennlp.textgrounder.geolocate

import java.io._
import javax.xml.datatype._
import javax.xml.stream._
import opennlp.textgrounder.topo._
import opennlp.textgrounder.util.KMLUtil
import scala.collection.JavaConversions._
import org.clapper.argot._

object ErrorKMLGenerator {

  val DOC_PREFIX = "Document "
  //val DOC_PREFIX = "Article "
  val TRUE_COORD_PREFIX = ") at ("
  val PRED_COORD_PREFIX = " predicted cell center at ("
  

  val factory = XMLOutputFactory.newInstance

  def parseLogFile(filename: String): List[(String, Coordinate, Coordinate)] = {
    val lines = scala.io.Source.fromFile(filename).getLines

    var docName:String = null
    var trueCoord:Coordinate = null
    var predCoord:Coordinate = null

    (for(line <- lines) yield {
      if(line.startsWith("#")) {

        if(line.contains(DOC_PREFIX)) {
          var startIndex = line.indexOf(DOC_PREFIX) + DOC_PREFIX.length
          var endIndex = line.indexOf("(", startIndex)
          docName = line.slice(startIndex, endIndex)
          
          startIndex = line.indexOf(TRUE_COORD_PREFIX) + TRUE_COORD_PREFIX.length
          endIndex = line.indexOf(")", startIndex)
          val rawCoords = line.slice(startIndex, endIndex).split(",")
          trueCoord = Coordinate.fromDegrees(rawCoords(0).toDouble, rawCoords(1).toDouble)
          None
        }

        else if(line.contains(PRED_COORD_PREFIX)) {
          val startIndex = line.indexOf(PRED_COORD_PREFIX) + PRED_COORD_PREFIX.length
          val endIndex = line.indexOf(")", startIndex)
          val rawCoords = line.slice(startIndex, endIndex).split(",")
          predCoord = Coordinate.fromDegrees(rawCoords(0).toDouble, rawCoords(1).toDouble)

          Some((docName, trueCoord, predCoord))
        }

        else None
      }
      else None
    }).flatten.toList

  }

  import ArgotConverters._

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.geolocate.ErrorKMLGenerator", preUsage = Some("TextGrounder"))
  val logFile = parser.option[String](List("l", "log"), "log", "log input file")
  val kmlOutFile = parser.option[String](List("k", "kml"), "kml", "kml output file")
  val usePred = parser.option[String](List("p", "pred"), "pred", "show predicted rather than gold locations")

  def main(args: Array[String]) {
    try {
      parser.parse(args)
    }
    catch {
      case e: ArgotUsageException => println(e.message); sys.exit(0)
    }

    if(logFile.value == None) {
      println("You must specify a log input file via -l.")
      sys.exit(0)
    }
    if(kmlOutFile.value == None) {
      println("You must specify a KML output file via -k.")
      sys.exit(0)
    }

    val rand = new scala.util.Random

    val outFile = new File(kmlOutFile.value.get)
    val stream = new BufferedOutputStream(new FileOutputStream(outFile))
    val out = factory.createXMLStreamWriter(stream, "UTF-8")

    KMLUtil.writeHeader(out, "errors-at-"+(if(usePred.value == None) "true" else "pred"))

    for((docName, trueCoord, predCoordOrig) <- parseLogFile(logFile.value.get)) {
      val predCoord = Coordinate.fromDegrees(predCoordOrig.getLatDegrees() + (rand.nextDouble() - 0.5) * .1,
                                             predCoordOrig.getLngDegrees() + (rand.nextDouble() - 0.5) * .1);

      //val dist = trueCoord.distanceInKm(predCoord)

      val coord1 = if(usePred.value == None) trueCoord else predCoord
      val coord2 = if(usePred.value == None) predCoord else trueCoord

      KMLUtil.writeLinePlacemark(out, coord1, coord2);
      KMLUtil.writePinPlacemark(out, docName, coord1, "yellow");
      //KMLUtil.writePlacemark(out, docName, coord1, KMLUtil.RADIUS);
      KMLUtil.writePinPlacemark(out, docName, coord2, "blue");
      //KMLUtil.writePolygon(out, docName, coord, KMLUtil.SIDES, KMLUtil.RADIUS, math.log(dist) * KMLUtil.BARSCALE/2)
    }

    KMLUtil.writeFooter(out)

    out.close
  }
}
