///////////////////////////////////////////////////////////////////////////////
//  math.scala
//
//  Copyright (C) 2011-2013 Ben Wing, The University of Texas at Austin
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
   * Return whether a double is negative, handling -0.0 correctly.
   */
  def is_negative(x: Double) =
    java.lang.Double.compare(x, 0.0) < 0

  /**
   *  Return the median value of a sequence.
   *
   *  @param seq Sequence of points to retrieve median of.
   *  @param sorted Whether the sequence is already sorted; this is an
   *     optimization, as otherwise the sequence will be sorted.
   */
  def median(seq: IndexedSeq[Double], sorted: Boolean = false) =
    quantile_at(seq, 0.5, sorted)
  
  /**
   *  Return the quantile value at the given fraction. If the quantile
   *  doesn't fall exactly at a point in the array, we interpolate between
   *  the nearest two points.
   *
   *  @param seq Sequence of points to retrieve quantile of.
   *  @param fraction Quantile to retrieve.
   *  @param sorted Whether the sequence is already sorted; this is an
   *     optimization, as otherwise the sequence will be sorted.
   */
  def quantile_at(seq: IndexedSeq[Double], fraction: Double,
      sorted: Boolean = false) = {
    val els = if (sorted) seq else seq.sorted
    val len = els.size
    val leftind_float = (len - 1) * fraction
    val leftind = leftind_float.toInt
    if (leftind == leftind_float)
      els(leftind)
    else {
      val offset = leftind_float - leftind
      els(leftind) * (1.0 - offset) + els(leftind + 1) * offset
    }
  }

  /**
   * Return the mean (average) of a collection.
   */
  def mean(x: Traversable[Double]) = {
    x.sum / x.size
  }

  /**
   * Return the mode (most common value) of a collection.
   */
  def mode[T](x: Traversable[T]) = {
    (argmax(x.countItems) { _._2 })._1
  }

  /**
   * Return the variance of a collection.
   */
  def variance(x: Traversable[Double]) = {
    val m = mean(x)
    mean(for (y <- x) yield ((y - m) * (y - m)))
  }

  /**
   * Return the standard deviation (square root of the variance) of a collection.
   */
  def stddev(x: Traversable[Double]) = sqrt(variance(x))

  /**
   * Standardize a sequence so that it has mean 0, std dev 1.
   */
  def standardize(x: Iterable[Double]) = {
    val xmean = mean(x)
    val xstddev = stddev(x)
    x map { z => (z - xmean) / xstddev }
  }

  private val log2_value = log(2)
  def log2(x: Double) = log(x) / log2_value
  def logn(x: Double, base: Double) = log(x) / log(base)
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
