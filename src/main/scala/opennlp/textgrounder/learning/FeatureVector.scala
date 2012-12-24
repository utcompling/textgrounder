///////////////////////////////////////////////////////////////////////////////
//  FeatureVector.scala
//
//  Copyright (C) 2012 Ben Wing, The University of Texas at Austin
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
 * Feature vectors for machine learning.
 *
 * @author Ben Wing
 */

import collection.mutable
import io.Source

import util.memoizer._
import util.print._
import gridlocate.GridLocateDriver.Debug._

/**
 * A vector of real-valued features.  In general, features are indexed
 * both by a non-negative integer and by a class label (i.e. a label for
 * the class that is associated with a particular instance by a classifier).
 * Commonly, the class label is ignored when looking up a feature's value.
 * Some implementations might want to evaluate the features on-the-fly
 * rather than store an actual vector of values.
 */
trait FeatureVector {
  /** Return the length of the feature vector.  This is the number of weights
    * that need to be created -- not necessarily the actual number of items
    * stored in the vector (which will be different especially in the case
    * of sparse vectors). */
  def length: Int

  /** Return the number of items stored in the vector.  This will be different
    * from the length in the case of sparse vectors. */
  def stored_entries: Int

  /** Return the value at index `i`, for class `label`. */
  def apply(i: Int, label: Int): Double

  /** Return the squared magnitude of the feature vector for class `label`,
    * i.e. dot product of feature vector with itself */
  def squared_magnitude(label: Int): Double

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  def diff_squared_magnitude(label1: Int, label2: Int): Double

  /** Return the dot product of the given weight vector with the feature
    * vector for class `label`. */
  def dot_product(weights: WeightVector, label: Int): Double

  /** Update a weight vector by adding a scaled version of the feature vector,
    * with class `label`. */
  def update_weights(weights: WeightVector, scale: Double, label: Int)
}

/**
 * A feature vector that ignores the class label.
 */
trait SimpleFeatureVector extends FeatureVector {
  /** Return the value at index `i`. */
  def apply(i: Int): Double

  def apply(i: Int, label: Int) = apply(i)
}

/**
 * A feature vector in which the features are stored densely, i.e. as
 * an array of values.
 */
trait DenseFeatureVector extends FeatureVector {
  def stored_entries = length

  def dot_product(weights: WeightVector, label: Int) =
    (for (i <- 0 until length) yield apply(i, label)*weights(i)).sum

  def squared_magnitude(label: Int) =
    (for (i <- 0 until length; va = apply(i, label)) yield va*va).sum

  def diff_squared_magnitude(label1: Int, label2: Int) =
    (for (i <- 0 until length; va = apply(i, label1) - apply(i, label2))
       yield va*va).sum

  def update_weights(weights: WeightVector, scale: Double, label: Int) {
    (0 until length).foreach { i => weights(i) += scale*apply(i, label) }
  }
}

/**
 * A vector of real-valued features, stored explicitly.  The values passed in
 * are used exactly as the values of the feature; no additional term is
 * inserted to handle a "bias" or "intercept" weight.
 */
class RawArrayFeatureVector(
  values: WeightVector
) extends DenseFeatureVector with SimpleFeatureVector {
  /** Add two feature vectors. */
  def +(other: SimpleFeatureVector) = {
    val len = length
    val res = new WeightVector(len)
    for (i <- 0 until len)
      res(i) = this(i) + other(i)
    new RawArrayFeatureVector(res)
  }
  
  /** Subtract two feature vectors. */
  def -(other: SimpleFeatureVector) = {
    val len = length
    val res = new WeightVector(len)
    for (i <- 0 until len)
      res(i) = this(i) - other(i)
    new RawArrayFeatureVector(res)
  }

  /** Scale a feature vector. */
  def *(scalar: Double) = {
    val len = length
    val res = new WeightVector(len)
    for (i <- 0 until len)
      res(i) = this(i)*scalar
    new RawArrayFeatureVector(res)
  }

  /** Return the length of the feature vector. */
  def length = values.length

  /** Return the value at index `i`. */
  def apply(i: Int) = values(i)

  def update(i: Int, value: Double) { values(i) = value }
}

/**
 * A vector of real-valued features, stored explicitly.  An additional value
 * set to a constant 1 is automatically stored at the end of the vector.
 */
class ArrayFeatureVector(
  values: WeightVector
) extends DenseFeatureVector with SimpleFeatureVector {
  /** Return the length of the feature vector; + 1 including the extra bias
    * term. */
  def length = values.length + 1

  /** Return the value at index `i`, but return 1.0 at the last index. */
  def apply(i: Int) = {
    if (i == values.length) 1.0
    else values(i)
  }

  def update(i: Int, value: Double) {
    if (i == values.length) {
      if (value != 1.0) {
        throw new IllegalArgumentException(
          "Element at the last index (index %s) unmodifiable, fixed at 1.0"
          format i)
      }
    } else { values(i) = value }
  }
}

/**
 * A factory object for creating sparse feature vectors for classification.
 */
class SparseFeatureVectorFactory[T](
  display_feature: T => String
) {
  // Use Trove for fast, efficient hash tables.
  val hashfact = new TroveHashTableFactory
  // Alternatively, just use the normal Scala hash tables.
  // val hashfact = new ScalaHashTableFactory

  // Set the minimum index to 1 so we can use 0 for the intercept term
  val feature_mapper = new ToIntMemoizer[T](hashfact, minimum_index = 1)

  trait SparseFeatureVectorLike {
    def length = {
      // +1 because of the intercept term
      feature_mapper.number_of_entries + 1
    }

    def diff_squared_magnitude(label1: Int, label2: Int) = 0.0

    def compute_toString(prefix: String,
        feature_values: Iterable[(Int, Double)]) =
      "%s(%s)" format (prefix,
        feature_values.filter { case (index, value) => index > 0 }.
          toSeq.sorted.map {
            case (index, value) =>
              "%s(%s)=%.2f" format (
                display_feature(feature_mapper.unmemoize(index)),
                index, value
              )
          }.mkString(",")
      )
  }

  /**
   * A feature vector in which the features are stored sparsely, i.e. only
   * the features with non-zero values are stored, using a hash table or
   * similar.  The features are indexed by integers, using the mapping
   * stored in `feature_mapper`. There will always be a feature with the
   * index 0, value 1.0, to handle the intercept term.
   */
  protected class CompressedSparseFeatureVector(
    keys: Array[Int], values: Array[Double]
  ) extends SimpleFeatureVector with SparseFeatureVectorLike {
    assert(keys.length == values.length)

    def stored_entries = keys.length

    def apply(index: Int) = {
      val keyind = java.util.Arrays.binarySearch(keys, index)
      if (keyind < 0) 0.0 else values(keyind)
    }

    def squared_magnitude(label: Int) = {
      var i = 0
      var res = 0.0
      while (i < keys.length) {
        res += values(i) * values(i)
        i += 1
      }
      res
    }

    def dot_product(weights: WeightVector, label: Int) = {
      var i = 0
      var res = 0.0
      while (i < keys.length) {
        res += values(i) * weights(keys(i))
        i += 1
      }
      res
    }

    def update_weights(weights: WeightVector, scale: Double, label: Int) {
      var i = 0
      while (i < keys.length) {
        weights(keys(i)) += scale * values(i)
        i += 1
      }
    }

    override def toString = compute_toString(
      "CompressedSparseFeatureVector", keys zip values)
  }

  /**
   * A feature vector in which the features are stored sparsely, i.e. only
   * the features with non-zero values are stored, using a hash table or
   * similar.  The features are indexed by integers, using the mapping
   * stored in `feature_mapper`. There will always be a feature with the
   * index 0, value 1.0, to handle the intercept term.
   */
  protected abstract class SparseFeatureVector(
    feature_values: Iterable[(Int, Double)]
  ) extends SimpleFeatureVector with SparseFeatureVectorLike {
    def stored_entries = feature_values.size

    // def apply(index: Int): Double --- needs to be defined

    def squared_magnitude(label: Int) =
      feature_values.map {
        case (index, value) => value * value
      }.sum

    def dot_product(weights: WeightVector, label: Int) =
      feature_values.map {
        case (index, value) => value * weights(index)
      }.sum

    def update_weights(weights: WeightVector, scale: Double, label: Int) {
      feature_values.map {
        case (index, value) => weights(index) += scale * value
      }
    }

    def string_prefix: String

    override def toString = compute_toString(string_prefix, feature_values)
  }

  /**
   * An implementation of a sparse feature vector storing the non-zero
   * features in a `Map`.
   */
  protected class MapSparseFeatureVector(
    feature_values: collection.Map[Int, Double]
  ) extends SparseFeatureVector(feature_values) {
    def apply(index: Int) = feature_values.getOrElse(index, 0.0)

    def string_prefix = "MapSparseFeatureVector"
  }

  protected class TupleArraySparseFeatureVector(
    feature_values: mutable.Buffer[(Int, Double)]
  ) extends SparseFeatureVector(feature_values) {
    // Use an O(n) algorithm to look up a value at a given index.  Luckily,
    // this operation isn't performed very often (if at all).  We could
    // speed it up by storing the items sorted and use binary search.
    def apply(index: Int) = feature_values.find(_._1 == index) match {
      case Some((index, value)) => value
      case None => 0.0
    }

    def string_prefix = "TupleArraySparseFeatureVector"
  }

  val vector_impl = debugval("featvec") match {
    case "Compressed" | "compressed" => "Compressed"
    case "TupleArray" | "tuplearray" | "tuple-array" => "TupleArray"
    case "Map" | "map" => "Map"
    case _ => "Compressed"
  }

  errprint(vector_impl match {
    case "Compressed" => "Using compressed feature vectors"
    case "TupleArray" => "Using tuple array (semi-compressed) feature vectors"
    case "Map" => "Using map (uncompressed) feature vectors"
  })

  /**
   * Generate a feature vector.  If not at training time, we need to be
   * careful to skip features not seen during training because there won't a
   * corresponding entry in the weight vector, and the resulting feature would
   * containing a non-existent index, causing a crash during lookup (e.g.
   * during the dot-product operation).
   */
  def make_feature_vector(feature_values: Iterable[(T, Double)],
      is_training: Boolean) = {
    val memoized_features = Iterable(0 -> 1.0) ++: (// the intercept term
      if (is_training)
        feature_values.map {
          case (name, value) => (feature_mapper.memoize(name), value)
        }
       else
        for { (name, value) <- feature_values;
               index = feature_mapper.memoize_if(name);
               if index != None }
          yield (index.get, value)
      )
    vector_impl match {
      case "Compressed" => {
        val (keys, values) =
          memoized_features.toIndexedSeq.sortWith(_._1 < _._1).unzip
        new CompressedSparseFeatureVector(keys.toArray, values.toArray)
      }
      case "TupleArray" =>
        new TupleArraySparseFeatureVector(memoized_features.toBuffer)
      case "Map" =>
        new MapSparseFeatureVector(memoized_features.toMap)
    }
  }
}

/**
 * A factory object for creating sparse nominal instances for classification,
 * consisting of a nominal label and a set of nominal features.  Nominal
 * labels have no ordering or other numerical significance.
 */
class SparseNominalInstanceFactory extends
  SparseFeatureVectorFactory[String](identity) {
  val label_mapper = new ToIntMemoizer[String](hashfact, minimum_index = 0)
  def label_to_index(label: String) = label_mapper.memoize(label)
  def index_to_label(index: Int) = label_mapper.unmemoize(index)
  def number_of_labels = label_mapper.number_of_entries

  def make_labeled_instance(features: Iterable[String], label: String,
      is_training: Boolean) = {
    val featvals = features.map((_, 1.0))
    val featvec = make_feature_vector(featvals, is_training)
    // FIXME: What about labels not seen during training?
    val labelind = label_to_index(label)
    (featvec, labelind)
  }

  def get_csv_labeled_instances(source: Source, is_training: Boolean) = {
    val lines = source.getLines
    for (line <- lines) yield {
      val atts = line.split(",")
      val label = atts.last
      val features = atts.dropRight(1)
      make_labeled_instance(features, label, is_training)
    }
  }
}
