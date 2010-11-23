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
package opennlp.textgrounder.util.cluster

import java.io._
import scala.Math
import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._
import scala.util.Random

trait Geometry[A] {
  def dist(x: A)(y: A): Double
  def move(s: A)(t: A)(d: Double): A
  def cent(ps: Seq[A]): A

  def nearest(cs: Seq[A], p: A): Int = cs.map(dist(p)(_)).zipWithIndex.min._2
}

trait Clusterer {
  def cluster[A](ps: List[A], n: Int)(implicit g: Geometry[A]): List[A]
}

class KMeans extends Clusterer {
  def cluster[A](ps: List[A], n: Int)(implicit g: Geometry[A]): List[A] = {
    var cs = init(ps, n)
    var as = ps.map(g.nearest(cs, _))
    var done = false
    while (!done) {
      val clusters = List.fill(n)(Buffer[A]())
      as.zipWithIndex.foreach { case (i, j) =>
        clusters(i) += ps(j)
      }

      cs = clusters.map(g.cent(_))

      val bs = ps.map(g.nearest(cs, _))
      done = as == bs
      as = bs
    }
    cs
  }

  def init[A](ps: List[A], n: Int): List[A] = Random.shuffle(ps).take(n)
}

/* A simple instance for Euclidean geometry. */
object Geometry {
  type Point = (Double, Double)

  implicit def euclideanGeometry = new Geometry[Point] {
    def dist(x: Point)(y: Point): Double =
      Math.sqrt(Math.pow(x._1 - y._1, 2) + Math.pow(x._2 - y._2, 2))

    def move(s: Point)(t: Point)(d: Double): Point = {
      val r = d / dist(s)(t)
      (s._1 + r * (t._1 - s._1), s._2 + r * (t._2 - s._2))
    }

    def cent(ps: Seq[Point]): Point = {
      def pointPlus(x: Point, y: Point) = (x._1 + y._1, x._2 + y._2)
      ps.reduceLeft(pointPlus) match { case (a, b) => (a / 2, b / 2) }
    }
  }
}

object KMeans {
  def main(args: Array[String]) {
    import Geometry.euclideanGeometry

    val km = new KMeans()
    val ps: List[(Double, Double)] = List((0.01, 1), (0.25, 0.25), (1.3, 0), (1, 1.2), (0.1, 0))
    km.cluster(ps, 3).foreach { case (x, y) => println(x + " " + y) }
  }
}

