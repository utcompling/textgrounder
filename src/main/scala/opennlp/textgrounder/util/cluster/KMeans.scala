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
import scala.collection.immutable.Vector
import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._
import scala.util.Random

trait Geometry[A] {
  def distance(x: A)(y: A): Double
  def centroid(ps: Seq[A]): A

  def nearest(cs: Seq[A], p: A): Int =
    cs.map(distance(p)(_)).zipWithIndex.min._2
}

trait Clusterer {
  def cluster[A](ps: IndexedSeq[A], k: Int)(implicit g: Geometry[A]): Seq[A]
}

class KMeans extends Clusterer {
  def cluster[A](ps: IndexedSeq[A], k: Int)(implicit g: Geometry[A]): Seq[A] = {
    var cs = init(ps, k)
    var as = ps.map(g.nearest(cs, _))
    var done = false
    val clusters = IndexedSeq.fill(k)(Buffer[A]())
    while (!done) {
      clusters.foreach(_.clear)

      as.zipWithIndex.foreach { case (i, j) =>
        clusters(i) += ps(j)
      }

      cs = clusters.map(g.centroid(_))

      val bs = ps.map(g.nearest(cs, _))
      done = as == bs
      as = bs
    }
    cs
  }

  def init[A](ps: Seq[A], k: Int): IndexedSeq[A] = {
    (1 to k).map(_ => ps(Random.nextInt(ps.size)))
  }
}

object EuclideanGeometry {
  type Point = (Double, Double)

  implicit def g = new Geometry[Point] {
    def distance(x: Point)(y: Point): Double =
      Math.sqrt(Math.pow(x._1 - y._1, 2) + Math.pow(x._2 - y._2, 2))

    def centroid(ps: Seq[Point]): Point = {
      def pointPlus(x: Point, y: Point) = (x._1 + y._1, x._2 + y._2)
      ps.reduceLeft(pointPlus) match {
        case (a, b) => (a / ps.size, b / ps.size)
      }
    }
  }
}

