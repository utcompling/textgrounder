///////////////////////////////////////////////////////////////////////////////
//  KNNKMLGenerator.scala
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

package opennlp.textgrounder.postprocess

import java.io._
import javax.xml.datatype._
import javax.xml.stream._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util.KMLUtil
import opennlp.textgrounder.tr.util.LogUtil
import scala.collection.JavaConversions._
import org.clapper.argot._

object KNNKMLGenerator {

  val factory = XMLOutputFactory.newInstance
  val rand = new scala.util.Random

  import ArgotConverters._

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.postprocess.KNNKMLGenerator", preUsage = Some("TextGrounder"))
  val logFile = parser.option[String](List("l", "log"), "log", "log input file")
  val kmlOutFile = parser.option[String](List("k", "kml"), "kml", "kml output file")

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

    val outFile = new File(kmlOutFile.value.get)
    val stream = new BufferedOutputStream(new FileOutputStream(outFile))
    val out = factory.createXMLStreamWriter(stream, "UTF-8")

    KMLUtil.writeHeader(out, "knn")

    for(pe <- LogUtil.parseLogFile(logFile.value.get)) {

      val jPredCoord = jitter(pe.predCoord)

      KMLUtil.writePinPlacemark(out, pe.docName, pe.trueCoord)
      KMLUtil.writePinPlacemark(out, pe.docName, jPredCoord, "blue")
      KMLUtil.writePlacemark(out, "#1", jPredCoord, KMLUtil.RADIUS*10)
      KMLUtil.writeLinePlacemark(out, pe.trueCoord, jPredCoord, "redLine")

      for((neighbor, rank) <- pe.neighbors) {
        val jNeighbor = jitter(neighbor)
        /*if(rank == 1) {
          KMLUtil.writePlacemark(out, "#1", neighbor, KMLUtil.RADIUS*10)
        }*/
        if(rank != 1) {
          KMLUtil.writePlacemark(out, "#"+rank, jNeighbor, KMLUtil.RADIUS*10)
          KMLUtil.writePinPlacemark(out, pe.docName, jNeighbor, "green")
          /*if(!neighbor.equals(pe.predCoord))*/ KMLUtil.writeLinePlacemark(out, pe.trueCoord, jNeighbor)
        }
      }

    }

    KMLUtil.writeFooter(out)

    out.close
  }

  def jitter(coord:Coordinate): Coordinate = {
    Coordinate.fromDegrees(coord.getLatDegrees() + (rand.nextDouble() - 0.5) * .1,
                           coord.getLngDegrees() + (rand.nextDouble() - 0.5) * .1);
  }
}
