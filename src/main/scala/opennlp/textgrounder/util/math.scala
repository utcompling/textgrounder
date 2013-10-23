///////////////////////////////////////////////////////////////////////////////
//  math.scala
//
//  Copyright (C) 2011, 2012 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package util

import collection._
import scala.math._

package object math {
  def argmax[T](args: Iterable[T])(fun: T => Double) =
    argandmax(args)(fun)._1

  /** Return both the argument producing the maximum and the maximum value
    * itself, when the function is applied to the arguments. */
  def argandmax[T](args: Iterable[T])(fun: T => Double) = {
    (args zip args.map(fun)).maxBy(_._2)
  }

  /** Return the argument producing the minimum when the function is applied
    * to it. */
  def argmin[T](args: Iterable[T])(fun: T => Double) =
    argandmin(args)(fun)._1

  /** Return both the argument producing the minimum and the minimum value
    * itself, when the function is applied to the arguments. */
  def argandmin[T](args: Iterable[T])(fun: T => Double) = {
    (args zip args.map(fun)).minBy(_._2)
  }

  /**
   *  Return the median value of a list.
   */
  def median(list: Seq[Double]) = {
    val sorted = list.sorted
    val len = sorted.length
    if (len % 2 == 1)
      sorted(len / 2)
    else {
      val midp = len / 2
      0.5*(sorted(midp-1) + sorted(midp))
    }
  }
  
  /**
   *  Return the mean (average) of a list.
   */
  def mean(list: Seq[Double]) = {
    list.sum / list.length
  }

  /**
   *  Return the mode (most common value) of a list.
   */
  def mode[T](list: Seq[T]) = {
    (argmax(list.countItems) { _._2 })._1
  }

  def variance(x: Seq[Double]) = {
    val m = mean(x)
    mean(for (y <- x) yield ((y - m) * (y - m)))
  }

  def stddev(x: Seq[Double]) = sqrt(variance(x))
}

package math {
  abstract class MeanShift[Coord : Manifest](
      h: Double = 1.0,
      max_stddev: Double = 1e-10,
      max_iterations: Int = 100
    ) {
    def squared_distance(x:Coord, y:Coord): Double
    def weighted_sum(weights:Array[Double], points:Array[Coord]): Coord
    def scaled_sum(scalar:Double, points:Array[Coord]): Coord

    def vec_mean(points:Array[Coord]) = scaled_sum(1.0/points.length, points)

    def vec_variance(points:Array[Coord]) = {
      def m = vec_mean(points)
      mean(
        for (i <- 0 until points.length) yield squared_distance(m, points(i)))
    }

    def mean_shift(list: Seq[Coord]):Array[Coord] = {
      var next_stddev = max_stddev + 1
      var numiters = 0
      val points = list.toArray
      val shifted = list.toArray
      while (next_stddev >= max_stddev && numiters <= max_iterations) {
        for (j <- 0 until points.length) {
          val y = shifted(j)
          val weights =
            (for (i <- 0 until points.length)
             yield exp(-squared_distance(y, points(i))/(h*h)))
          val weight_sum = weights sum
          val normalized_weights = weights.map(_ / weight_sum).toArray
          shifted(j) = weighted_sum(normalized_weights, points)
        }
        numiters += 1
        next_stddev = sqrt(vec_variance(shifted))
      }
      shifted
    }
  }
} 
