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
package opennlp.textgrounder.topo

import scala.io._

import scala.collection.JavaConversions._

import opennlp.textgrounder.util.cluster._

object SphericalGeometry {
  implicit def g: Geometry[Coordinate] = new Geometry[Coordinate] {
    def distance(x: Coordinate)(y: Coordinate): Double = x.distance(y)
    def centroid(ps: Seq[Coordinate]): Coordinate = Coordinate.centroid(ps)
  }

  def main(args: Array[String]) {
    val max = args(1).toInt
    val k = args(2).toInt
    val style = args(3)

    val cs = Source.fromFile(args(0)).getLines.map { line =>
      val Array(lat, lng) = line.split("\t").map(_.toDouble)
      Coordinate.fromDegrees(lat, lng)
    }.toIndexedSeq
    println("Loaded...")

    val xs = scala.util.Random.shuffle(cs).take(max)

    println(Coordinate.centroid(xs))

    val clusterer = new KMeans
    val clusters = clusterer.cluster(xs, k)
    clusters.foreach {
      case c => println("<Placemark><styleUrl>" +
                style + "</styleUrl><Point><coordinates>" +
                c.getLngDegrees + "," + c.getLatDegrees +
                "</coordinates></Point></Placemark>")
    }
  }
}
