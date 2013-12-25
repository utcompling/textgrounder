///////////////////////////////////////////////////////////////////////////////
//  Vector.scala
//
//  Copyright (C) 2012-2013 Ben Wing, The University of Texas at Austin
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
package learning

/**
 * Simple vectors supporting minimum operations for machine learning.
 *
 * @author Ben Wing
 */

/**
 * Simple definition of a vector.  Supports only a few operations.
 */
trait SimpleVector {
  def length: Int

  def max: Double

  def min: Double

  def sum: Double

  def apply(index: Int): Double

  def update(index: Int, value: Double)

  def +=(addto: SimpleVector)

  def add_scaled(scale: Double, addto: SimpleVector)

  def *=(value: Double)

  def toIterable: Iterable[Double]
}

trait SimpleVectorFactory {
  def empty(len: Int): SimpleVector
}

/**
 * The most basic implementation of the simple vector operations, which
 * should always work (although not necessarily efficiently).
 */
trait BasicSimpleVectorImpl extends SimpleVector {
  def +=(addto: SimpleVector) {
    val l = length
    assert(addto.length == l)
    var i = 0
    while (i < l) {
      this(i) += addto(i)
      i += 1
    }
  }

  def add_scaled(scale: Double, addto: SimpleVector) {
    val l = length
    assert(addto.length == l)
    var i = 0
    while (i < l) {
      this(i) += scale * addto(i)
      i += 1
    }
  }

  def *=(value: Double) {
    val l = length
    var i = 0
    while (i < l) {
      this(i) *= value
      i += 1
    }
  }
}

/**
 * Basic implementation of vector using Java array.
 */
case class ArrayVector(array: Array[Double]) extends BasicSimpleVectorImpl {
  final def length = array.length

  def max = array.max
  def min = array.min
  def sum = array.sum

  final def apply(index: Int) = array(index)

  final def update(index: Int, value: Double) { array(index) = value }

  def toIterable = array.toIterable
}

object ArrayVector extends SimpleVectorFactory {
  def empty(len: Int) = new ArrayVector(new Array[Double](len))
}

/**
 * An aggregate of vectors, used to implement weight vectors in a classifier.
 * Allows for either a separate set of per-label vectors or a single shared
 * vector.
 */
trait VectorAggregate {
  def length: Int
  def depth: Int
  def max_label: Int

  def apply(label: LabelIndex): SimpleVector

  def foreach[U](f: SimpleVector => U) {
    for (i <- 0 until depth) {
      f(this(i))
    }
  }

  def map[U](f: SimpleVector => U) = {
    (0 until depth).map(i => f(this(i))).toIndexedSeq
  }

  def +=(addto: VectorAggregate) {
    assert(addto.depth == depth)
    for (i <- 0 until depth) {
      this(i) += addto(i)
    }
  }

  def add_scaled(scale: Double, addto: VectorAggregate) {
    assert(addto.depth == depth)
    for (i <- 0 until depth) {
      this(i).add_scaled(scale, addto(i))
    }
  }

  def *=(value: Double) { foreach(_ *= value) }

  def max: Double = map(_.max).max
  def min: Double = map(_.min).min
  def sum: Double = map(_.sum).sum
}

trait VectorAggregateFactory {
  val vector_factory: SimpleVectorFactory
  def empty(len: Int): VectorAggregate
}

case class SingleVectorAggregate(vec: SimpleVector) extends VectorAggregate {
  def length = vec.length
  def depth = 1
  def max_label = Int.MaxValue

  def apply(label: LabelIndex) = vec
}

case class MultiVectorAggregate(vecs: IndexedSeq[SimpleVector])
    extends VectorAggregate {
  def length = vecs(0).length
  def depth = vecs.length
  def max_label = depth - 1

  def apply(label: LabelIndex) = vecs(label)
}

class SingleVectorAggregateFactory(
  val vector_factory: SimpleVectorFactory
) extends VectorAggregateFactory {
  def empty(len: Int) =
    new SingleVectorAggregate(vector_factory.empty(len))
}

class MultiVectorAggregateFactory(
  val vector_factory: SimpleVectorFactory,
  val num_labels: Int
) extends VectorAggregateFactory {
  def empty(len: Int) =
    new MultiVectorAggregate(
      IndexedSeq(
        (for (i <- 0 until num_labels) yield vector_factory.empty(len)) :_*)
    )
}
