///////////////////////////////////////////////////////////////////////////////
//  DocumentPinKMLGenerator.scala
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

object DocumentPinKMLGenerator {

  val factory = XMLOutputFactory.newInstance
  val rand = new scala.util.Random

  import ArgotConverters._

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.postprocess.DocumentPinKMLGenerator", preUsage = Some("TextGrounder"))
  val inFile = parser.option[String](List("i", "input"), "input", "input file")
  val kmlOutFile = parser.option[String](List("k", "kml"), "kml", "kml output file")
  val tokenIndexOffset = parser.option[Int](List("o", "offset"), "offset", "token index offset")

  def main(args: Array[String]) {
    try {
      parser.parse(args)
    }
    catch {
      case e: ArgotUsageException => println(e.message); sys.exit(0)
    }

    if(inFile.value == None) {
      println("You must specify an input file via -i.")
      sys.exit(0)
    }
    if(kmlOutFile.value == None) {
      println("You must specify a KML output file via -k.")
      sys.exit(0)
    }
    val offset = if(tokenIndexOffset.value != None) tokenIndexOffset.value.get else 0

    val outFile = new File(kmlOutFile.value.get)
    val stream = new BufferedOutputStream(new FileOutputStream(outFile))
    val out = factory.createXMLStreamWriter(stream, "UTF-8")

    KMLUtil.writeHeader(out, inFile.value.get)

    for(line <- scala.io.Source.fromFile(inFile.value.get).getLines) {
      val tokens = line.split("\t")
      if(tokens.length >= 3+offset) {
        val docName = tokens(1+offset)
        val coordTextPair = tokens(2+offset).split(",")
        val coord = Coordinate.fromDegrees(coordTextPair(0).toDouble, coordTextPair(1).toDouble)
        KMLUtil.writePinPlacemark(out, docName, coord)
      }
    }

    KMLUtil.writeFooter(out)

    out.close
  }
}
