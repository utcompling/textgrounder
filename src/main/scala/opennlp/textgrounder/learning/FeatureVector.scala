///////////////////////////////////////////////////////////////////////////////
//  FeatureVector.scala
//
//  Copyright (C) 2012-2014 Ben Wing, The University of Texas at Austin
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

import util.collection.{doublemap, intmap}
import util.error.internal_error
import util.memoizer._
import util.print.errprint

/** Classification of the different types of values used for features.
 * Currently this is only for features that will be rescaled to make
 * them comparable across different query documents. Only features that
 * do involve the query document are candidates for this, and even then
 * only when the feature represents a score that differs dramatically
 * in scale from one query to the other and where only the difference
 * really matters (e.g. for KL-divergence). Currently only FeatRescale
 * below will be rescaled.
 */
sealed abstract class FeatureValue
/** A feature value not to be scaled. */
case object FeatRaw extends FeatureValue
/** A feature value needing scaling. */
case object FeatRescale extends FeatureValue
case object FeatBinary extends FeatureValue
case object FeatProb extends FeatureValue
case object FeatLogProb extends FeatureValue
case object FeatCount extends FeatureValue

/**
 * A more general classification of features, just into nominal vs.
 * numeric.
 */
sealed abstract class FeatureClass
case object NominalFeature extends FeatureClass
case object NumericFeature extends FeatureClass

/**
 * Mapping of features and labels into integers. In the context of a
 * classifier, "features" need to be distinguished into "feature properties"
 * and "combined features". The former, sometimes termed a "contextual
 * predicate" in the context of binary features, is a semantic concept
 * describing a property that may be found of data instances or combinations
 * of instances and labels, while the latter describes the combination of
 * a feature property and a given label. For instance, in the context of
 * GridLocate where instances are documents and labels are cells, the
 * binary feature property "london" has the value of 1 for a document
 * containing the word "london" regardless of the cell it is paired with,
 * and the corresponding combined feature "london@53.5,2.5" having a value
 * of 1 means that the document when paired with the cell centered at
 * 53.5,2.5 has the value of 1. The same document will have a distinct
 * combined feature "london@37.5,-120.0" when combined with a different cell.
 * In this case the identity of the label (i.e. cell) isn't needed to
 * compute the feature value but in some cases it is, e.g. the matching
 * feature "london$matching-unigram-binary" indicating that the word
 * "london" occurred in both document and cell. Regardless, the
 * separation of feature properties into distinct combined features is
 * necessary so that different weights are computed in TADM.
 *
 * For a ranker, combined features do not include the identity of the
 * cell, and thus all combined features with the same feature property
 * will be treated as the same feature, and only a single weight will be
 * assigned. This is correct in the context of a ranker, where the labels
 * can be thought of as numbers 1 through N, which are not meaningfully
 * distinguishable in the way that different labels of a classifier are.
 *
 * To combine a feature property and a label, we don't actually create
 * a string like "london@53.5,2.5"; instead, we map the feature property's
 * name to an integer, and map the label to an integer, and combine them
 * in a Long, which is then memoized to an Int. This is done to save
 * memory.
 */
abstract class FeatureLabelMapper {
  protected val label_mapper = new ToIntMemoizer[String]
  protected val property_mapper = new ToIntMemoizer[String]
  def feature_to_index(feature: String, label: LabelIndex): FeatIndex
  def feature_to_index_if(feature: String, label: LabelIndex): Option[FeatIndex]
  def feature_to_string(index: FeatIndex): String
  def feature_vector_length: Int

  val features_to_rescale = mutable.BitSet()
  protected def add_feature_property(feattype: FeatureValue, prop: String) = {
    val index = property_mapper.to_index(prop)
    feattype match {
      case FeatRescale => features_to_rescale += index
      case _ => ()
    }
    index
  }

  def note_feature(feattype: FeatureValue, feature: String, label: LabelIndex
  ): FeatIndex

  def label_to_index(label: String): LabelIndex =
    label_mapper.to_index(label)
  def label_to_index_if(label: String): Option[LabelIndex] =
    label_mapper.to_index_if(label)
  def label_to_string(index: LabelIndex): String =
    label_mapper.to_raw(index)
  def number_of_labels: Int = label_mapper.number_of_indices
}

class SimpleFeatureLabelMapper extends FeatureLabelMapper {
  def feature_to_index(feature: String, label: LabelIndex) =
    property_mapper.to_index(feature)
  def feature_to_index_if(feature: String, label: LabelIndex) =
    property_mapper.to_index_if(feature)
  def feature_to_string(index: FeatIndex) = property_mapper.to_raw(index)
  def feature_vector_length = property_mapper.number_of_indices

  def note_feature(feattype: FeatureValue, feature: String, label: LabelIndex
  ) = add_feature_property(feattype, feature)
}

class LabelAttachedFeatureLabelMapper extends FeatureLabelMapper {
  val combined_mapper = new LongToIntMemoizer

  private def combined_to_long(prop_index: FeatIndex, label: LabelIndex) = {
    // If label could be negative, we need to and it with 0xFFFFFFFFL to
    // convert it to an unsigned value, but that should never happen.
    assert(label >= 0)
    (prop_index.toLong << 32) + label
  }

  def feature_to_index(feature: String, label: LabelIndex) = {
    val prop_index = property_mapper.to_index(feature)
    combined_mapper.to_index(combined_to_long(prop_index, label))
  }
  def feature_to_index_if(feature: String, label: LabelIndex) = {
    // Only map to Some(...) if both feature property and combined feature
    // already exist.
    property_mapper.to_index_if(feature).flatMap { prop_index =>
      combined_mapper.to_index_if(combined_to_long(prop_index, label))
    }
  }
  def feature_to_string(index: FeatIndex) = {
    val longval = combined_mapper.to_raw(index)
    val prop_index = (longval >> 32).toInt
    val label_index = (longval & 0xFFFFFFFF).toInt
    property_mapper.to_raw(prop_index) + "@" + label_mapper.to_raw(label_index)
  }
  def feature_vector_length = combined_mapper.number_of_indices

  def note_feature(feattype: FeatureValue, feature: String, label: LabelIndex
  ) = {
    val prop_index = add_feature_property(feattype, feature)
    combined_mapper.to_index(combined_to_long(prop_index, label))
  }
}

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

  def mapper: FeatureLabelMapper

  /** Return the length of the feature vector.  This is the number of weights
    * that need to be created -- not necessarily the actual number of items
    * stored in the vector (which will be different especially in the case
    * of sparse vectors). */
  def length: Int = mapper.feature_vector_length

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
  def max_label: LabelIndex

  /** Return the number of items stored in the vector.  This will be different
    * from the length in the case of sparse vectors. */
  def stored_entries: Int

  /** Return the value at index `i`, for class `label`. */
  def apply(i: FeatIndex, label: LabelIndex): Double

  /** Return the squared magnitude of the feature vector for class `label`,
    * i.e. dot product of feature vector with itself */
  def squared_magnitude(label: LabelIndex): Double

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  def diff_squared_magnitude(label1: LabelIndex, label2: LabelIndex): Double

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for `label1` and another feature vector for
    * `label2`. */
  def diff_squared_magnitude_2(label1: LabelIndex, other: FeatureVector,
    label2: LabelIndex): Double

  /** Return the dot product of the given weight vector with the feature
    * vector for class `label`. */
  def dot_product(weights: SimpleVector, label: LabelIndex): Double

  /** Update a weight vector by adding a scaled version of the feature vector,
    * with class `label`. */
  def update_weights(weights: SimpleVector, scale: Double, label: LabelIndex)

  /** Display the feature at the given index as a string. */
  def format_feature(index: FeatIndex) = mapper.feature_to_string(index)

  /** Display the label at the given index as a string. */
  def format_label(index: FeatIndex) = mapper.label_to_string(index)

  def pretty_format(prefix: String): String

  def pretty_print_labeled(prefix: String, correct: LabelIndex) {
    errprint("$prefix: Label: %s(%s)", correct, mapper.label_to_string(correct))
    errprint("$prefix: Featvec: %s", pretty_format(prefix))
  }
}

object FeatureVector {
  /** Check that all feature vectors have the same mappers. */
  def check_same_mappers(fvs: Iterable[FeatureVector]) {
    val mapper = fvs.head.mapper
    for (fv <- fvs) {
      assert(fv.mapper == mapper)
    }
  }
}

/**
 * A feature vector that ignores the class label.
 */
trait SimpleFeatureVector extends FeatureVector {
  /** Return the value at index `i`. */
  def apply(i: FeatIndex): Double

  def apply(i: FeatIndex, label: LabelIndex) = apply(i)

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  override def diff_squared_magnitude(label1: LabelIndex, label2: LabelIndex
  ) = 0

  val depth = 1
  val max_label = Int.MaxValue
}

object BasicFeatureVectorImpl {
  def diff_squared_magnitude_2(fv1: FeatureVector, label1: LabelIndex,
      fv2: FeatureVector, label2: LabelIndex) = {
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
  def dot_product(weights: SimpleVector, label: LabelIndex) =
    (for (i <- 0 until length) yield apply(i, label)*weights(i)).sum

  def squared_magnitude(label: LabelIndex) =
    (for (i <- 0 until length; va = apply(i, label)) yield va*va).sum

  def diff_squared_magnitude(label1: LabelIndex, label2: LabelIndex) =
    (for (i <- 0 until length; va = apply(i, label1) - apply(i, label2))
       yield va*va).sum

  def diff_squared_magnitude_2(label1: LabelIndex, other: FeatureVector,
      label2: LabelIndex) =
    BasicFeatureVectorImpl.diff_squared_magnitude_2(this, label1, other, label2)

  def update_weights(weights: SimpleVector, scale: Double, label: LabelIndex) {
    (0 until length).foreach { i => weights(i) += scale*apply(i, label) }
  }
}

/**
 * A vector of real-valued features, stored explicitly. If you want a "bias"
 * or "intercept" weight, you need to add it yourself.
 */
case class ArrayFeatureVector(
  values: SimpleVector,
  mapper: FeatureLabelMapper
) extends BasicFeatureVectorImpl with SimpleFeatureVector {
  assert(values.length == length)

  def stored_entries = length

  /** Return the value at index `i`. */
  final def apply(i: FeatIndex) = values(i)

  final def update(i: FeatIndex, value: Double) { values(i) = value }

  def pretty_format(prefix: String) = "  %s: %s" format (prefix, values)
}

trait SparseFeatureVector extends SimpleFeatureVector {
  def include_displayed_feature: Boolean = true

  def compute_toString(prefix: String,
      feature_values: Iterable[(FeatIndex, Double)]) =
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
      feature_values: Iterable[(FeatIndex, Double)]) =
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

  def toIterable: Iterable[(FeatIndex, Double)]

  def string_prefix: String

  override def toString = compute_toString(string_prefix, toIterable)

  def pretty_format(prefix: String) = {
    "%s: %s".format(prefix, string_prefix) + "\n" +
      pretty_feature_string(prefix, toIterable)
  }

  // Simple implementation of this to optimize in the common case
  // where the other vector is sparse.
  def diff_squared_magnitude_2(label1: LabelIndex, other: FeatureVector,
      label2: LabelIndex) = {
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
      case sp2: SparseFeatureVector => {
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
//   keys: Array[FeatIndex], values: Array[Double]
// ) extends SparseFeatureVector {
// (in FeatureVector.scala.template)
// }

// class BasicCompressedSparseFeatureVector private[learning] (
//   keys: Array[FeatIndex], values: Array[Double], val length: Int
// ) extends CompressedSparseFeatureVector(keys, values) {
// (in FeatureVector.scala.template)
// }

/**
 * A feature vector in which the features are stored sparsely, i.e. only
 * the features with non-zero values are stored, using a hash table or
 * similar.  The features are indexed by integers. (Features indexed by
 * other types, e.g. strings, should use a memoizer to convert to integers.)
 */
abstract class SimpleSparseFeatureVector(
  feature_values: Iterable[(FeatIndex, Double)]
) extends SparseFeatureVector {
  def stored_entries = feature_values.size

  def squared_magnitude(label: LabelIndex) =
    feature_values.map {
      case (index, value) => value * value
    }.sum

  def dot_product(weights: SimpleVector, label: LabelIndex) =
    feature_values.map {
      case (index, value) => value * weights(index)
    }.sum

  def update_weights(weights: SimpleVector, scale: Double, label: LabelIndex) {
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
class MapSparseFeatureVector(
  feature_values: collection.Map[FeatIndex, Double],
  val mapper: FeatureLabelMapper
) extends SimpleSparseFeatureVector(feature_values) {
  def apply(index: FeatIndex) = feature_values.getOrElse(index, 0.0)

  def string_prefix = "MapSparseFeatureVector"
}

class TupleArraySparseFeatureVector(
  feature_values: mutable.Buffer[(FeatIndex, Double)],
  val mapper: FeatureLabelMapper
) extends SimpleSparseFeatureVector(feature_values) {
  // Use an O(n) algorithm to look up a value at a given index.  Luckily,
  // this operation isn't performed very often (if at all).  We could
  // speed it up by storing the items sorted and use binary search.
  def apply(index: FeatIndex) = feature_values.find(_._1 == index) match {
    case Some((index, value)) => value
    case None => 0.0
  }

  def string_prefix = "TupleArraySparseFeatureVector"
}

/**
 * An aggregate feature vector that stores a separate individual feature
 * vector for each of a set of labels.
 */
case class AggregateFeatureVector(
    fv: Array[FeatureVector]
) extends FeatureVector {
  FeatureVector.check_same_mappers(fv)

  def mapper = fv.head.mapper

  def depth = fv.length
  def max_label = depth - 1

  def stored_entries = fv.map(_.stored_entries).sum

  def apply(i: FeatIndex, label: LabelIndex) = fv(label)(i, label)

  /** Return the squared magnitude of the feature vector for class `label`,
    * i.e. dot product of feature vector with itself */
  def squared_magnitude(label: LabelIndex) = fv(label).squared_magnitude(label)

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  def diff_squared_magnitude(label1: LabelIndex, label2: LabelIndex) =
    fv(label1).diff_squared_magnitude_2(label1, fv(label2), label2)

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  def diff_squared_magnitude_2(label1: LabelIndex, other: FeatureVector, label2: LabelIndex) = {
    val fv2 = other match {
      case afv2: AggregateFeatureVector => afv2.fv(label2)
      case _ => other
    }
    fv(label1).diff_squared_magnitude_2(label1, fv2, label2)
  }

  def dot_product(weights: SimpleVector, label: LabelIndex) =
    fv(label).dot_product(weights, label)

  def update_weights(weights: SimpleVector, scale: Double,
      label: LabelIndex) = fv(label).update_weights(weights, scale, label)

  /** Display the feature at the given index as a string. */
  override def format_feature(index: FeatIndex) = fv.head.format_feature(index)

  def pretty_format(prefix: String) = {
    (for (d <- 0 until depth) yield
      "Featvec at depth %s(%s): %s" format (
        d, mapper.label_to_string(d),
        fv(d).pretty_format(prefix))).mkString("\n")
  }

  /**
   * Return the component feature vectors as a lazy sequence of compressed
   * sparse feature vectors, by casting each item. Will throw an error if
   * the feature vectors are of the wrong type.
   */
  def fetch_sparse_featvecs = {
    // Retrieve as the appropriate type of compressed sparse feature vectors.
    // This will be one of DoubleCompressedSparseFeatureVector,
    // FloatCompressedSparseFeatureVector, etc.
    fv.view map { x => x match {
      case y:CompressedSparseFeatureVectorType => y
      case _ => ???
    } }
  }

  /**
   * Return the set of all features found in the aggregate.
   */
  def find_all_features = {
    fetch_sparse_featvecs.foldLeft(Set[FeatIndex]()) { _ union _.keys.toSet }
  }

  /**
   * Find the features that do not have the same value for every component
   * feature vector. Return a set of such features.
   */
  def find_diff_features = {
    // OK, we do this in a purely iterative fashion to create as little
    // garbage as possible, because we may be working with very large
    // arrays. The basic idea, since there may be different features in
    // different sparse feature vectors, is that we keep track of the
    // number of times each feature has been seen and the max and min
    // value of the feature. Then, a feature is the same across all
    // feature vectors if either:
    //
    // 1. It hasn't been seen at all (its count is 0); or,
    // 2. Its maxval and minval are the same and either
    //    a. both are 0, or
    //    b. both are non-zero and the feature was seen in every vector.
    //
    // We return a set of the features that aren't the same, because
    // all unseen features by definition are the same across all
    // feature vectors. Then, to compute the total set of features that
    // aren't the same, we take the union of the individual results --
    // something we can do incrementally to save memory.

    val stats = new FeatureStats

    stats.accumulate(this, do_minmax = true)

    (
      for ((k, count) <- stats.count;
           same = stats.min(k) == stats.max(k) &&
             (stats.min(k) == 0 || count == depth);
           if !same)
        yield k
    ).toSet
  }

  /**
   * Destructively remove columns that are non-choice-specific, i.e.
   * not listed among the list of choice-specific features passed in.
   */
  def remove_non_choice_specific_columns(diff_features: Set[FeatIndex]) {
    for (vec <- fetch_sparse_featvecs) {
      val need_to_remove = vec.keys.exists(!diff_features.contains(_))
      if (need_to_remove) {
        val feats =
          (vec.keys zip vec.values).filter(diff_features contains _._1)
        vec.keys = feats.map(_._1).toArray
        vec.values = feats.view.map(_._2).map(to_feat_value(_)).toArray
      }
    }
  }

  /**
   * Destructively rescale the appropriate features in the given
   * feature vectors by subtracting their mean and then dividing by their
   * standard deviation. This only operates on features that have been
   * marked as needing standardization. Standardization is done to make it
   * easier to compare numeric feature values across different query
   * documents, which otherwise may differ greatly in scale. (This is the
   * case for KL-divergence scores, for example.) Whether standardization
   * is done depends on the type of the feature's value. For example,
   * binary features definitely don't need to be rescaled and
   * probabilities aren't currently rescaled, either. Features of only
   * a candidate also don't need to be rescaled as they don't suffer
   * from the problem of being compared with different query documents.)
   */
  def rescale_featvecs() {
    val sparsevecs = fetch_sparse_featvecs

    // Get the sum and count of all features seen in any of the keys.
    // Here and below, when computing mean, std dev, etc., we ignore
    // unseen features rather than counting them as 0's. This wouldn't
    // work well for binary features, where we normally only store the
    // features with a value of 1, but we don't adjust those features
    // in any case. For other features, we assume that features that
    // are meant to be present and happen to have a value of 0 will be
    // noted as such. This means that we calculate mean and stddev
    // only for features that are present, and likewise rescale
    // only features that are present. Otherwise, we'd end up converting
    // all the absent features to present features with some non-zero
    // value, which seems non-sensical besides making the coding more
    // difficult as we currently can't expand a sparse feature vector,
    // only change the value of an existing feature.
    val stats = new FeatureStats
    stats.accumulate(this, do_sum = true)

    // Compute the mean of all seen instances of a feature.
    // Unseen features are ignored rather than counted as 0's.
    val meanmap =
      stats.sum map { case (key, sum) => (key, sum / stats.count(key)) }

    // Compute the sum of squared differences from the mean of all seen
    // instances of a feature.
    // Unseen features are ignored rather than counted as 0's.
    val sumsqmap = doublemap[FeatIndex]()
    for (vec <- sparsevecs; i <- 0 until vec.keys.size) {
      val k = vec.keys(i)
      val v = vec.values(i)
      val mean = meanmap(k)
      sumsqmap(k) += (v - mean) * (v - mean)
    }

    // Compute the standard deviation of all features.
    // Unseen features are ignored rather than counted as 0's.
    val stddevmap =
      sumsqmap map { case (key, sumsq) =>
        (key, math.sqrt(sumsq / stats.count(key))) }

    // Now rescale the features that need to be.
    for (vec <- sparsevecs; i <- 0 until vec.keys.size) {
      val key = vec.keys(i)
      if (mapper.features_to_rescale contains key) {
        val stddev = stddevmap(key)
        // Must skip 0, NaN and infinity! Comparison between anything and
        // NaN will be false and won't pass the > 0 check.
        if (stddev > 0 && !stddev.isInfinite) {
          vec.values(i) =
            /*
            to_feat_value((vec.values(i) - meanmap(key))/stddev)
            // FIXME! We add 1 here somewhat arbitrarily because TADM ignores
            // features that have a mean of 0 (don't know why).
            to_feat_value((vec.values(i) - meanmap(key))/stddev + 1)
            */
            // FIXME: Try just dividing by the standard deviation so the
            // spread is at least scaled correctly. This also ensures that
            // 0 values would remain as 0 so no problems arise due to
            // ignoring them. An alternative that might work for KL-divergence
            // and similar scores is to shift so that the minimum gets a value
            // of 0, as well as scaling by the stddev.
            // FIXME! In the case of the initial ranking, it's also rescaled
            // and adjusted in `evaluate_with_initial_ranking`.
            to_feat_value(vec.values(i)/stddev)
        }
      }
    }
  }
}

object AggregateFeatureVector {
  def check_aggregate(di: DataInstance) = {
    val fv = di.feature_vector
    fv match {
      case agg: AggregateFeatureVector => agg
      case _ => internal_error("Only operates with AggregateFeatureVectors")
    }
  }

  /**
   * Destructively remove all features that have the same values among all
   * components of each aggregate feature vector (even if they have different
   * values among different aggregate feature vectors). Features that don't
   * differ at all within any aggregate feature vector aren't useful for
   * distinguishing one candidate from another and may cause singularity
   * errors in R's mlogit() function, so need to be deleted.
   *
   * Return the set of removed features.
   */
  def remove_all_non_choice_specific_columns(
      featvecs: Iterable[(DataInstance, LabelIndex)]
  ) = {
    val agg_featvecs = featvecs.view.map { x => check_aggregate(x._1) }
    // Find the set of all features that have different values in at least
    // one pair of component feature vectors in at least one aggregate
    // feature vector.
    val diff_features =
      agg_featvecs.map(_.find_diff_features).reduce(_ union _)
    val all_features =
      agg_featvecs.map(_.find_all_features).reduce(_ union _)
    val removed_features = all_features diff diff_features
    val headfv = featvecs.head._1.feature_vector
    errprint("Removing non-choice-specific features: %s",
      removed_features.toSeq.sorted.map(headfv.format_feature(_)) mkString " ")
    agg_featvecs.foreach(_.remove_non_choice_specific_columns(diff_features))
    removed_features
  }
}

class FeatureStats {
  // Count of features
  val count = intmap[FeatIndex]()
  // Count of feature vectors
  var num_fv = 0
  // Min value of features
  val min = doublemap[FeatIndex]()
  // Max value of features
  val max = doublemap[FeatIndex]()
  // Sum of features
  val sum = doublemap[FeatIndex]()
  // Sum of absolute value of features
  val abssum = doublemap[FeatIndex]()

  /**
   * Find the features that have different values from one component
   * feature vector to another. Return a set of such features.
   */
  def accumulate(agg: AggregateFeatureVector, do_minmax: Boolean = false,
      do_sum: Boolean = false, do_abssum: Boolean = false) {
    num_fv += agg.depth

    // We do this in a purely iterative fashion to create as little
    // garbage as possible, because we may be working with very large
    // arrays. See find_diff_features.
    for (vec <- agg.fetch_sparse_featvecs; i <- 0 until vec.keys.size) {
      val k = vec.keys(i)
      val v = vec.values(i)
      count(k) += 1
      if (do_minmax) {
        if (!(min contains k) || v < min(k))
          min(k) = v
        if (!(max contains k) || v > max(k))
          max(k) = v
      }
      if (do_sum)
        sum(k) += v
      if (do_abssum)
        abssum(k) += v.abs
    }
  }
}

/**
 * For each aggregate, we compute the fraction of times a given feature
 * in the correct label's feature vector is greater than, less than or
 * equal to the value in the other feature vectors in the aggregate, and
 * average over all aggregates. These three fraction will add up to 1
 * for a given aggregate, and the averages will likewise add up to 1.
 * Note that for any feature seen anywhere in the aggregate we process
 * all feature vectors in the aggregate, treating unseen features as 0.
 * However, for features seen nowhere in the aggregate, they won't be
 * recorded, and hence we need to treat them as cases of all 100%
 * "equal to". In practice, because the averages add up to 1, we don't
 * need to keep track of the "equal to" fractions separately but instead
 * compute them from the other two; the sums of those two fractions
 * won't be affected by cases where a feature isn't seen in an aggregate.
 *
 * FIXME: Should we instead do this comparing the correct label to the
 * second-best rather than all the others?
 */
class FeatureDiscriminationStats {
  // Count of number of aggregates seen
  var num_agg = 0
  // Sum of fractions of fv's other than the correct one where the
  // feature's value in the correct one is less than the value in the
  // others. See above.
  val less_than_other = doublemap[FeatIndex]()
  // Same for "greater than".
  val greater_than_other = doublemap[FeatIndex]()

  /**
   * Find the features that have different values from one component
   * feature vector to another. Return a set of such features.
   */
  def accumulate(agg: AggregateFeatureVector, corrlab: LabelIndex) {
    num_agg += 1

    for (feat <- agg.find_all_features) {
      val corrval = agg(feat, corrlab)
      var num_lto = 0
      var num_gto = 0
      for (lab <- 0 until agg.depth; if lab != corrlab) {
        val othval = agg(feat, lab)
        if (corrval < othval)
          num_lto += 1
        else if (corrval > othval)
          num_gto += 1
      }
      less_than_other(feat) += num_lto.toDouble / (agg.depth - 1)
      greater_than_other(feat) += num_gto.toDouble / (agg.depth - 1)
    }
  }
}
