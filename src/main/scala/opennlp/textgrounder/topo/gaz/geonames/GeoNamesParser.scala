///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
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
package opennlp.textgrounder.topo.gaz.geonames

import java.io._
import scala.collection.JavaConversions._
import scala.io._

import opennlp.textgrounder.text.Corpus
import opennlp.textgrounder.text.Token
import opennlp.textgrounder.topo.Location

class GeoNamesParser(private val file: File) {
  val locs = scala.collection.mutable.Map[String, List[(Double, Double)]]()

  Source.fromFile(file).getLines.foreach { line =>
    val Array(lat, lng) = line.split("\t").map(_.toDouble)
    
  }
}

