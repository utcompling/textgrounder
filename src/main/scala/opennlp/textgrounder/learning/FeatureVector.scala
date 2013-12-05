///////////////////////////////////////////////////////////////////////////////
//  FeatureVector.scala
//
//  Copyright (C) 2012, 2013 Ben Wing, The University of Texas at Austin
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
import util.debug._

/**
 * A vector of real-valued features.  In general, features are indexed
 * both by a non-negative integer and by a class label (i.e. a label for
 * the class that is associated with a particular instance by a classifier).
 * Commonly, the class label is ignored when looking up a feature's value.
 * Some implementations might want to evaluate the features on-the-fly
 * rather than store an actual vector of values.
 */
trait FeatureVector extends DataInstance {
  final def feature_vector = this

  /** Return the length of the feature vector.  This is the number of weights
    * that need to be created -- not necessarily the actual number of items
    * stored in the vector (which will be different especially in the case
    * of sparse vectors). */
  def length: Int

  /** Return number of labels for which label-specific information is stored.
    * For a simple feature vector that ignores the label, this will be 1.
    * For an aggregate feature vector storing a separate set of features for
    * each label, this will be the number of different sets of features stored.
    * This is used only in a variable-depth linear classifier, where the
    * number of possible labels may vary depending on the particular instance
    * being classified. */
  def depth: Int

  /** Return maximum label index compatible with this feature vector.  For
    * feature vectors that ignore the label, return a large number, e.g.
    * `Int.MaxValue`. */
  def max_label: Int

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

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for `label1` and another feature vector for
    * `label2`. */
  def diff_squared_magnitude_2(label1: Int, other: FeatureVector,
    label2: Int): Double

  /** Return the dot product of the given weight vector with the feature
    * vector for class `label`. */
  def dot_product(weights: SimpleVector, label: Int): Double

  /** Update a weight vector by adding a scaled version of the feature vector,
    * with class `label`. */
  def update_weights(weights: SimpleVector, scale: Double, label: Int)

  /** Display the feature at the given index as a string. */
  def format_feature(index: Int) = index.toString
}

object FeatureVector {
  /** Check that all feature vectors have the same length, and return it. */
  def check_same_length(fvs: Iterable[FeatureVector]) = {
    // Written this way because the length might change as we iterate
    // the first time through the data (this will be the case if we are
    // using SparseFeatureVector). The call to `max` iterates through the
    // whole data first before checking the length again. (Previously we
    // compared the first against the rest, which ran into problems.)
    val len = fvs.map(_.length).max
    for (fv <- fvs) {
      assert(fv.length == len)
    }
    len
  }
}

/**
 * A feature vector that ignores the class label.
 */
trait SimpleFeatureVector extends FeatureVector {
  /** Return the value at index `i`. */
  def apply(i: Int): Double

  def apply(i: Int, label: Int) = apply(i)

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  override def diff_squared_magnitude(label1: Int, label2: Int) = 0

  val depth = 1
  val max_label = Int.MaxValue
}

object BasicFeatureVectorImpl {
  def diff_squared_magnitude_2(fv1: FeatureVector, label1: Int,
      fv2: FeatureVector, label2: Int) = {
    assert(fv1.length == fv2.length)
    (for (i <- 0 until fv1.length; va = fv1(i, label1) - fv2(i, label2))
       yield va*va).sum
  }
}

/**
 * The most basic implementation of the feature vector operations, which
 * should always work (although not necessarily efficiently).
 */
trait BasicFeatureVectorImpl extends FeatureVector {
  def dot_product(weights: SimpleVector, label: Int) =
    (for (i <- 0 until length) yield apply(i, label)*weights(i)).sum

  def squared_magnitude(label: Int) =
    (for (i <- 0 until length; va = apply(i, label)) yield va*va).sum

  def diff_squared_magnitude(label1: Int, label2: Int) =
    (for (i <- 0 until length; va = apply(i, label1) - apply(i, label2))
       yield va*va).sum

  def diff_squared_magnitude_2(label1: Int, other: FeatureVector, label2: Int) =
    BasicFeatureVectorImpl.diff_squared_magnitude_2(this, label1, other, label2)

  def update_weights(weights: SimpleVector, scale: Double, label: Int) {
    (0 until length).foreach { i => weights(i) += scale*apply(i, label) }
  }
}

/**
 * A vector of real-valued features, stored explicitly.  The values passed in
 * are used exactly as the values of the feature; no additional term is
 * inserted to handle a "bias" or "intercept" weight.
 */
class ArrayFeatureVector(
  values: SimpleVector
) extends BasicFeatureVectorImpl with SimpleFeatureVector {
  /** Return the length of the feature vector. */
  final def length = values.length

  def stored_entries = length

  /** Return the value at index `i`. */
  final def apply(i: Int) = values(i)

  final def update(i: Int, value: Double) { values(i) = value }
}

trait SparseFeatureVectorLike extends SimpleFeatureVector {
  def include_displayed_feature: Boolean = true

  def compute_toString(prefix: String,
      feature_values: Iterable[(Int, Double)]) =
    "%s(%s)" format (prefix,
      feature_values.toSeq.sorted.map {
          case (index, value) => {
            if (include_displayed_feature)
              "%s(%s)=%.2f" format (format_feature(index), index, value)
            else
              "%s=%.2f" format (index, value)
          }
        }.mkString(",")
    )

  def pretty_feature_string(prefix: String,
      feature_values: Iterable[(Int, Double)]) =
    feature_values.toSeq.sorted.map {
      case (index, value) => {
        val featstr =
          if (include_displayed_feature)
            "%s(%s)" format (format_feature(index), index)
          else
            "%s" format index
        "  %s: %-40s = %.2f" format (prefix, featstr, value)
      }
    }.mkString("\n")

  def toIterable: Iterable[(Int, Double)]

  def string_prefix: String

  override def toString = compute_toString(string_prefix, toIterable)

  def pretty_print(prefix: String) = {
    "%s: %s".format(prefix, string_prefix) + "\n" +
      pretty_feature_string(prefix, toIterable)
  }

  // Simple implementation of this to optimize in the common case
  // where the other vector is sparse.
  def diff_squared_magnitude_2(label1: Int, other: FeatureVector,
      label2: Int) = {
    other match {
      // When the other vector is sparse, we need to handle: first, all
      // the indices defined in this vector, and second, all the
      // indices defined in the other vector but not in this one.
      // We don't currently have an "is-defined-in" predicate, but we
      // effectively have "is-defined-and-non-zero" by just checking
      // the return value; so we deal with the possibility of indices
      // defined in this vector with a zero value by skipping them.
      // If the other vector has a value at that index, it will be
      // handled when we loop over the other vector. (Otherwise, the
      // index is 0 in both vectors and contributes nothing.)
      case sp2: SparseFeatureVectorLike => {
        var res = 0.0
        for ((ind, value) <- toIterable) {
          if (value != 0.0) {
            val va = value - sp2(ind)
            res += va * va
          }
        }
        for ((ind, value) <- sp2.toIterable) {
          if (this(ind) == 0.0) {
            res += value * value
          }
        }
        res
      }
      case _ => BasicFeatureVectorImpl.diff_squared_magnitude_2(
        this, label1, other, label2)
    }
  }
}

// abstract class CompressedSparseFeatureVector private[learning] (
//   keys: Array[Int], values: Array[Double]
// ) extends SparseFeatureVectorLike {
// (in FeatureVector.scala.template)
// }

// class BasicCompressedSparseFeatureVector private[learning] (
//   keys: Array[Int], values: Array[Double], val length: Int
// ) extends CompressedSparseFeatureVector(keys, values) {
// (in FeatureVector.scala.template)
// }

//object SparseFeatureVector {
//  def apply(len: Int, fvs: (Int, Double)*) = {
//    val (keys, values) =
//      fvs.toIndexedSeq.sortWith(_._1 < _._1).unzip
//    new BasicCompressedSparseFeatureVector(keys.toArray, values.toArray, len)
//  }
//}

/**
 * A feature vector in which the features are stored sparsely, i.e. only
 * the features with non-zero values are stored, using a hash table or
 * similar.  The features are indexed by integers. (Features indexed by
 * other types, e.g. strings, should use a memoizer to convert to integers.)
 */
abstract class SimpleSparseFeatureVector(
  feature_values: Iterable[(Int, Double)]
) extends SparseFeatureVectorLike {
  def stored_entries = feature_values.size

  def squared_magnitude(label: Int) =
    feature_values.map {
      case (index, value) => value * value
    }.sum

  def dot_product(weights: SimpleVector, label: Int) =
    feature_values.map {
      case (index, value) => value * weights(index)
    }.sum

  def update_weights(weights: SimpleVector, scale: Double, label: Int) {
    feature_values.map {
      case (index, value) => weights(index) += scale * value
    }
  }

  def toIterable = feature_values
}

/**
 * An implementation of a sparse feature vector storing the non-zero
 * features in a `Map`.
 */
abstract class MapSparseFeatureVector(
  feature_values: collection.Map[Int, Double]
) extends SimpleSparseFeatureVector(feature_values) {
  def apply(index: Int) = feature_values.getOrElse(index, 0.0)

  def string_prefix = "MapSparseFeatureVector"
}

abstract class TupleArraySparseFeatureVector(
  feature_values: mutable.Buffer[(Int, Double)]
) extends SimpleSparseFeatureVector(feature_values) {
  // Use an O(n) algorithm to look up a value at a given index.  Luckily,
  // this operation isn't performed very often (if at all).  We could
  // speed it up by storing the items sorted and use binary search.
  def apply(index: Int) = feature_values.find(_._1 == index) match {
    case Some((index, value)) => value
    case None => 0.0
  }

  def string_prefix = "TupleArraySparseFeatureVector"
}

class FeatureMapper[T] extends ToIntMemoizer[T] {
  // Reserve 0 for the bias (intercept) term
  override val minimum_index = 1
}

/**
 * A factory object for creating sparse feature vectors for classification.
 * Sparse feature vectors store only the features with non-zero values.
 * The features are indexed by entities of type T, which are internally
 * mapped to integers, using the mapping stored in `feature_mapper`.
 * There will always be a feature with the index 0, value 1.0, to handle
 * the intercept term.
 */
class SparseFeatureVectorFactory[T](
  format_feature: T => String
) { self =>
  val vector_impl = debugval("featvec") match {
    case f@("DoubleCompressed" | "FloatCompressed" | "IntCompressed" |
            "ShortCompressed" | "TupleArray" | "Map") => f
    case "" => "DoubleCompressed"
  }

  val feature_mapper = new FeatureMapper[T]

  errprint("Feature vector implementation: %s", vector_impl match {
    case "DoubleCompressed" => "compressed feature vectors, using doubles"
    case "FloatCompressed" => "compressed feature vectors, using floats"
    case "IntCompressed" => "compressed feature vectors, using ints"
    case "ShortCompressed" => "compressed feature vectors, using shorts"
    // case "BitCompressed" => "compressed feature vectors, using bits"
    case "TupleArray" => "tuple-array-backed sparse vectors"
    case "Map" => "map-backed sparse vectors"
  })

  trait SparseFeatureVectorMixin extends SparseFeatureVectorLike {
    override def format_feature(index: Int) =
      self.format_feature(feature_mapper.unmemoize(index))
    def length = feature_mapper.number_of_valid_indices
  }

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
      case "TupleArray" =>
        new TupleArraySparseFeatureVector(memoized_features.toBuffer) with SparseFeatureVectorMixin
      case "Map" =>
        new MapSparseFeatureVector(memoized_features.toMap) with SparseFeatureVectorMixin
      case _ => {
        val (keys, values) =
          memoized_features.toIndexedSeq.sortWith(_._1 < _._1).unzip
        vector_impl match {
          case "DoubleCompressed" =>
            new DoubleCompressedSparseFeatureVector(keys.toArray,
              values.toArray) with SparseFeatureVectorMixin
          case "FloatCompressed" =>
            new FloatCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toFloat).toArray) with SparseFeatureVectorMixin
          case "IntCompressed" =>
            new IntCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toInt).toArray) with SparseFeatureVectorMixin
          case "ShortCompressed" =>
            new ShortCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toShort).toArray) with SparseFeatureVectorMixin
        }
      }
    }
  }
}

class LabelMapper[T] extends ToIntMemoizer[T] {
  // Reserve 0 for the bias (intercept) term
  override val minimum_index = 1
}

/**
 * A factory object for creating sparse nominal instances for classification,
 * consisting of a nominal label and a set of nominal features.  Nominal
 * labels have no ordering or other numerical significance.
 */
class SparseNominalInstanceFactory extends
  SparseFeatureVectorFactory[String](identity) {
  val label_mapper = new LabelMapper[String]
  def label_to_index(label: String) = label_mapper.memoize(label)
  def index_to_label(index: Int) = label_mapper.unmemoize(index)
  def number_of_labels = label_mapper.number_of_valid_indices

  def make_labeled_instance(features: Iterable[String], label: String,
      is_training: Boolean) = {
    val featvals = features.map((_, 1.0))
    val featvec = make_feature_vector(featvals, is_training)
    // FIXME: What about labels not seen during training?
    val labelind = label_to_index(label)
    (featvec, labelind)
  }

  def get_csv_labeled_instances(lines: Iterator[String],
      is_training: Boolean) = {
    for (line <- lines) yield {
      val atts = line.split(",", -1)
      val label = atts.last
      val features = atts.dropRight(1)
      make_labeled_instance(features, label, is_training)
    }
  }
}

/**
 * An aggregate feature vector that stores a separate individual feature
 * vector for each of a set of labels.
 */
class AggregateFeatureVector(val fv: IndexedSeq[FeatureVector])
    extends FeatureVector {
  FeatureVector.check_same_length(fv)

  def length = fv.head.length

  def depth = fv.length
  def max_label = depth - 1

  def stored_entries = fv.map(_.stored_entries).sum

  def apply(i: Int, label: Int) = fv(label)(i, label)

  /** Return the squared magnitude of the feature vector for class `label`,
    * i.e. dot product of feature vector with itself */
  def squared_magnitude(label: Int) = fv(label).squared_magnitude(label)

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  def diff_squared_magnitude(label1: Int, label2: Int) =
    fv(label1).diff_squared_magnitude_2(label1, fv(label2), label2)

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  def diff_squared_magnitude_2(label1: Int, other: FeatureVector, label2: Int) = {
    val fv2 = other match {
      case afv2: AggregateFeatureVector => afv2.fv(label2)
      case _ => other
    }
    fv(label1).diff_squared_magnitude_2(label1, fv2, label2)
  }

  def dot_product(weights: SimpleVector, label: Int) =
    fv(label).dot_product(weights, label)

  def update_weights(weights: SimpleVector, scale: Double, label: Int) =
    fv(label).update_weights(weights, scale, label)

  /** Display the feature at the given index as a string. */
  override def format_feature(index: Int) = fv.head.format_feature(index)
}
