///////////////////////////////////////////////////////////////////////////////
//  ErrorKMLGenerator.scala
//
//  Copyright (C) 2011, 2012 Mike Speriosu, The University of Texas at Austin
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

package opennlp.textgrounder.postprocess

import java.io._
import javax.xml.datatype._
import javax.xml.stream._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util.KMLUtil
import opennlp.textgrounder.tr.util.LogUtil
import scala.collection.JavaConversions._
import org.clapper.argot._

object ErrorKMLGenerator {

  val factory = XMLOutputFactory.newInstance

  import ArgotConverters._

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.postprocess.ErrorKMLGenerator", preUsage = Some("TextGrounder"))
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

    for(pe <- LogUtil.parseLogFile(logFile.value.get)) {
      val predCoord = Coordinate.fromDegrees(pe.predCoord.getLatDegrees() + (rand.nextDouble() - 0.5) * .1,
                                             pe.predCoord.getLngDegrees() + (rand.nextDouble() - 0.5) * .1);

      //val dist = trueCoord.distanceInKm(predCoord)

      val coord1 = if(usePred.value == None) pe.trueCoord else predCoord
      val coord2 = if(usePred.value == None) predCoord else pe.trueCoord

      KMLUtil.writeArcLinePlacemark(out, coord1, coord2);
      KMLUtil.writePinPlacemark(out, pe.docName, coord1, "yellow");
      //KMLUtil.writePlacemark(out, pe.docName, coord1, KMLUtil.RADIUS);
      KMLUtil.writePinPlacemark(out, pe.docName, coord2, "blue");
      //KMLUtil.writePolygon(out, pe.docName, coord, KMLUtil.SIDES, KMLUtil.RADIUS, math.log(dist) * KMLUtil.BARSCALE/2)
    }

    KMLUtil.writeFooter(out)

    out.close
  }
}
