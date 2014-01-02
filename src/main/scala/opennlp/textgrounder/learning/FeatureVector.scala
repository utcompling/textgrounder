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

import util.collection._
import util.error.internal_error
import util.memoizer._
import util.print._
import util.debug._

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

class FeatureMapper extends ToIntMemoizer[String] {
  def vector_length = number_of_indices
  val features_to_rescale = mutable.BitSet()
  def note_feature(feattype: FeatureValue, feature: String) = {
    val index = to_index(feature)
    feattype match {
      case FeatRescale => features_to_rescale += index
      case _ => ()
    }
    index
  }
}

class LabelMapper extends ToIntMemoizer[String] {
}

case class FeatureLabelMapper(
  feature_mapper: FeatureMapper = new FeatureMapper,
  label_mapper: LabelMapper = new LabelMapper
)

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

  def mappers: FeatureLabelMapper

  final def feature_mapper = mappers.feature_mapper

  final def label_mapper = mappers.label_mapper

  /** Return the length of the feature vector.  This is the number of weights
    * that need to be created -- not necessarily the actual number of items
    * stored in the vector (which will be different especially in the case
    * of sparse vectors). */
  def length: Int = feature_mapper.vector_length

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
  def format_feature(index: FeatIndex) = feature_mapper.to_string(index)

  /** Display the label at the given index as a string. */
  def format_label(index: FeatIndex) = label_mapper.to_string(index)

  def pretty_print(prefix: String): String
}

object FeatureVector {
  /** Check that all feature vectors have the same mappers. */
  def check_same_mappers(fvs: Iterable[FeatureVector]) {
    val mappers = fvs.head.mappers
    for (fv <- fvs) {
      assert(fv.mappers == mappers)
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
  mappers: FeatureLabelMapper
) extends BasicFeatureVectorImpl with SimpleFeatureVector {
  assert(values.length == length)

  def stored_entries = length

  /** Return the value at index `i`. */
  final def apply(i: FeatIndex) = values(i)

  final def update(i: FeatIndex, value: Double) { values(i) = value }

  def pretty_print(prefix: String) = "  %s: %s" format (prefix, values)
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

  def pretty_print(prefix: String) = {
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
  val mappers: FeatureLabelMapper
) extends SimpleSparseFeatureVector(feature_values) {
  def apply(index: FeatIndex) = feature_values.getOrElse(index, 0.0)

  def string_prefix = "MapSparseFeatureVector"
}

class TupleArraySparseFeatureVector(
  feature_values: mutable.Buffer[(FeatIndex, Double)],
  val mappers: FeatureLabelMapper
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
 * A factory object for creating sparse feature vectors for classification.
 * Sparse feature vectors store only the features with non-zero values.
 * The features are indexed by entities of type T, which are internally
 * mapped to integers, using the mapping stored in `feature_mapper`.
 */
class SparseFeatureVectorFactory { self =>
  val mappers = new FeatureLabelMapper
  def feature_mapper = mappers.feature_mapper
  def label_mapper = mappers.label_mapper

  val vector_impl = debugval("featvec") match {
    case f@("DoubleCompressed" | "FloatCompressed" | "IntCompressed" |
            "ShortCompressed" | "TupleArray" | "Map") => f
    case "" => feature_vector_implementation
  }

  errprint("Feature vector implementation: %s", vector_impl match {
    case "DoubleCompressed" => "compressed feature vectors, using doubles"
    case "FloatCompressed" => "compressed feature vectors, using floats"
    case "IntCompressed" => "compressed feature vectors, using ints"
    case "ShortCompressed" => "compressed feature vectors, using shorts"
    // case "BitCompressed" => "compressed feature vectors, using bits"
    case "TupleArray" => "tuple-array-backed sparse vectors"
    case "Map" => "map-backed sparse vectors"
  })

  /**
   * Generate a feature vector.  If not at training time, we need to be
   * careful to skip features not seen during training because there won't a
   * corresponding entry in the weight vector, and the resulting feature would
   * containing a non-existent index, causing a crash during lookup (e.g.
   * during the dot-product operation).
   */
  def make_feature_vector(
      feature_values: Iterable[(FeatureValue, String, Double)],
      is_training: Boolean) = {
    val memoized_features =
      // DON'T include an intercept term. Not helpful since it's non-
      // label-specific.
      if (is_training)
        feature_values.map {
          case (ty, name, value) =>
            (feature_mapper.note_feature(ty, name), value)
        }
       else
        for { (_, name, value) <- feature_values;
               index = feature_mapper.to_index_if(name);
               if index != None }
          yield (index.get, value)
    vector_impl match {
      case "TupleArray" =>
        new TupleArraySparseFeatureVector(memoized_features.toBuffer, mappers)
      case "Map" =>
        new MapSparseFeatureVector(memoized_features.toMap, mappers)
      case _ => {
        val (keys, values) =
          memoized_features.toIndexedSeq.sortWith(_._1 < _._1).unzip
        vector_impl match {
          case "DoubleCompressed" =>
            new DoubleCompressedSparseFeatureVector(keys.toArray,
              values.toArray, mappers)
          case "FloatCompressed" =>
            new FloatCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toFloat).toArray, mappers)
          case "IntCompressed" =>
            new IntCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toInt).toArray, mappers)
          case "ShortCompressed" =>
            new ShortCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toShort).toArray, mappers)
        }
      }
    }
  }
}

class SparseInstanceFactory extends SparseFeatureVectorFactory {
  // Format a series of lines for debug display.
  def format_lines(lines: TraversableOnce[Array[String]]) = {
    lines.map(line => errfmt(line mkString "\t")).mkString("\n")
  }

  def get_index(numcols: Int, colind: Int) = {
    val retval =
      if (colind < 0) numcols + colind
      else colind
    require(retval >= 0 && retval < numcols,
      "Column index %s out of bounds: Should be in [%s,%s)" format (
        colind, -numcols, numcols))
    retval
  }

  def get_columns(lines_iter: Iterator[String], split_re: String) = {
    val all_lines = lines_iter.toIterable
    val columns = all_lines.head.split(split_re)
    val coltype = mutable.Map[String, FeatureClass]()
    for (col <- columns) {
      coltype(col) = NumericFeature
    }
    // Figure out whether features are nominal or numeric
    val numcols = columns.size
    val lines = all_lines.tail.map(_.split(split_re))
    for (line <- lines) {
      require(line.size == numcols, "Expected %s columns but saw %s: %s"
        format (numcols, line.size, line mkString "\t"))
      for ((value, col) <- (line zip columns)) {
        try {
          value.toDouble
        } catch {
          case e: Exception => coltype(col) = NominalFeature
        }
      }
    }
    val column_types = columns map { col =>
      (col, coltype(col))
    }
    (lines, column_types)
  }

  def raw_linefeats_to_featvec(
      raw_linefeats: Iterable[(String, (String, FeatureClass))],
      is_training: Boolean
  ) = {
    // Generate appropriate features based on column values, names, types.
    val linefeats = raw_linefeats.map { case (value, (colname, coltype)) =>
      coltype match {
        case NumericFeature => (FeatRaw, colname, value.toDouble)
        case NominalFeature =>
          (FeatBinary, "%s$%s" format (colname, value), 1.0)
      }
    }

    // Create feature vector
    make_feature_vector(linefeats, is_training)
  }
}


/**
 * A factory object for creating sparse aggregate instances for
 * classification or ranking, consisting of a nominal label and a
 * set of features, which may be nominal or numeric. Nominal values
 * have no ordering or other numerical significance.
 */
class SparseSimpleInstanceFactory extends SparseInstanceFactory {
  /**
   * Return a pair of `(aggregate, label)` where `aggregate` is an
   * aggregate feature vector derived from the given lines.
   *
   * @param line Line specifying feature values.
   * @param columns Names of columns and corresponding feature type.
   *   There should be the same number of columns as items in each of the
   *   arrays.
   * @param label_column Column specifying the label; used to fetch
   *   the label and removed before creating features.
   * @param is_training Whether we are currently training or testing a model.
   */
  def import_labeled_instance(line: Array[String],
      columns: Iterable[(String, FeatureClass)], label_column: Int,
      is_training: Boolean) = {

    // Check the right length for the line
    require(line.size == columns.size, "Expected %s columns but saw %s: %s"
      format (columns.size, line.size, line mkString "\t"))

    val label = label_mapper.to_index(line(label_column))

    // Filter out the columns we don't use, pair each with its column spec.
    val raw_linefeats = line.zip(columns).zipWithIndex.filter {
      case (value, index) => index != label_column
    }.map(_._1)

    // Generate feature vector based on column values, names, types.
    val featvec = raw_linefeats_to_featvec(raw_linefeats, is_training)

    // Return instance
    if (debug("features")) {
      errprint("Label: %s(%s)", label, label_mapper.to_string(label))
      errprint("Featvec: %s", featvec.pretty_print(""))
    }
    (featvec, label)
  }

  /**
   * Return a sequence of pairs of `(aggregate, label)` where `aggregate`
   * is an aggregate feature vector derived from the given lines. The
   * first line should list the column headings.
   *
   * @param lines_iter Iterator over lines in the file.
   * @param split_re Regexp to split columns in a line.
   * @param label_column Column specifying the label; used to fetch
   *   the label and removed before creating features.
   * @param is_training Whether we are currently training or testing a model.
   */
  def import_labeled_instances(lines_iter: Iterator[String],
      split_re: String, label_column: Int,
      is_training: Boolean) = {
    val (lines, column_types) = get_columns(lines_iter, split_re)
    val numcols = column_types.size
    val label_colind = get_index(numcols, label_column)
    for (inst <- lines) yield {
      import_labeled_instance(inst, column_types, label_colind, is_training)
    }
  }
}

/**
 * A factory object for creating sparse aggregate instances for
 * classification or ranking, consisting of a nominal label and a
 * set of features, which may be nominal or numeric. Nominal values
 * have no ordering or other numerical significance.
 */
class SparseAggregateInstanceFactory extends SparseInstanceFactory {
  /**
   * Return a pair of `(aggregate, label)` where `aggregate` is an
   * aggregate feature vector derived from the given lines.
   *
   * @param lines Lines corresponding to different labels of an individual.
   * @param columns Names of columns and corresponding feature type.
   *   There should be the same number of columns as items in each of the
   *   arrays.
   * @param indiv_colind Column specifying the individual index; removed
   *   before creating features. This is a zero-based index into the
   *   list of columns.
   * @param label_colind Column specifying the label; used to fetch
   *   the label and removed before creating features.
   * @param choice_colind Column specifying "yes" or "no" (or "true" or
   *   "false") identifying whether the label on this line was chosen by
   *   this individual. There should be exactly one per set of lines.
   * @param is_training Whether we are currently training or testing a model.
   */
  def import_labeled_instance(lines: Iterable[Array[String]],
      columns: Iterable[(String, FeatureClass)],
      indiv_colind: Int, label_colind: Int, choice_colind: Int,
      is_training: Boolean) = {

    val numcols = columns.size

    // Check the right lengths for the lines
    for (line <- lines) {
      require(line.size == numcols, "Expected %s columns but saw %s: %s"
        format (numcols, line.size, line mkString "\t"))
    }

    // Retrieve the label, make sure there's exactly 1
    val choice = lines.map { line =>
      (label_mapper.to_index(line(label_colind)), line(choice_colind))
    }
    val label_lines = choice.filter {
      Seq("yes", "true") contains _._2.toLowerCase
    }
    require(label_lines.size == 1,
      "Expected exactly one label but saw %s: %s: lines:\n%s\n" format (
        label_lines.size, label_lines.map(_._1), format_lines(lines)))
    val label = label_lines.head._1

    // Make sure all possible labels seen.
    val labels_seen = choice.map(_._1)
    val num_labels_seen = labels_seen.toSet.size
    require(num_labels_seen == label_mapper.number_of_indices,
      "Not all labels found: Expected %s labels but saw %s: %s" format (
        label_mapper.number_of_indices, num_labels_seen,
        labels_seen.toSeq.sorted))

    // Extract the feature vectors
    val fvs = lines.map { line =>
      // Filter out the columns we don't use, pair each with its column spec.
      val raw_linefeats = line.zip(columns).zipWithIndex.filter {
        case (value, index) =>
          index != indiv_colind && index != label_colind &&
          index != choice_colind
      }.map(_._1)

      // Generate feature vector based on column values, names, types.
      raw_linefeats_to_featvec(raw_linefeats, is_training)
    }

    // Order by increasing label so that all aggregates have the labels in
    // the same order.
    assert(fvs.size == labels_seen.size)
    val sorted_fvs = (fvs zip labels_seen).toSeq.sortBy(_._2).map(_._1)

    // Aggregate feature vectors, return aggregate with label
    val agg = new AggregateFeatureVector(fvs.toArray)
    if (debug("features")) {
      errprint("Label: %s(%s)", label, label_mapper.to_string(label))
      errprint("Feature vector: %s", agg.pretty_print(""))
    }
    (agg, label)
  }

  /**
   * Return a sequence of pairs of `(aggregate, label)` where `aggregate`
   * is an aggregate feature vector derived from the given lines. The
   * first line should list the column headings.
   *
   * @param lines_iter Iterator over lines in the file.
   * @param split_re Regexp to split columns in a line.
   * @param indiv_column Column specifying the individual index; removed
   *   before creating features. This is a zero-based index into the
   *   list of columns.
   * @param label_column Column specifying the label; used to fetch
   *   the label and removed before creating features.
   * @param choice_column Column specifying "yes" or "no" identifying
   *   whether the label on this line was chosen by this individual. There
   *   should be exactly one per set of lines.
   * @param is_training Whether we are currently training or testing a model.
   */
  def import_labeled_instances(lines_iter: Iterator[String],
      split_re: String,
      indiv_column: Int, label_column: Int, choice_column: Int,
      is_training: Boolean) = {
    val (lines, column_types) = get_columns(lines_iter, split_re)
    val numcols = column_types.size
    val indiv_colind = get_index(numcols, indiv_column)
    val label_colind = get_index(numcols, label_column)
    val choice_colind = get_index(numcols, choice_column)

    val grouped_instances = new GroupByIterator(lines.toIterator,
      { line: Array[String] => line(indiv_column).toInt })
    val retval = (for ((_, inst) <- grouped_instances) yield {
      import_labeled_instance(inst.toIterable, column_types,
        indiv_column, label_column, choice_column,
        is_training)
    }).toIterable

    if (debug("export-instances")) {
      val (headers, datarows) =
        AggregateFeatureVector.export_training_data(
          TrainingData(retval, Set[FeatIndex]()))
      errprint("Headers: %s", headers mkString " ")
      errprint("Data rows:")
      errprint(datarows.map(_.toString) mkString "\n")
    }

    retval
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

  /**
   * Undo the conversion in `import_labeled_instance`, converting the instance
   * back to the 2-d matrix format used in R's mlogit() function. For F
   * features and L labels this will have the following type:
   *
   * Iterable[(Int, String, Boolean), Iterable[Double]]
   *
   * where there are L elements ("rows") in the first-level array and
   * F elements ("columns") in the second-level array. The tuple is of
   * `(indiv, label, choice)` where `indiv` is the 1-based
   * index of the instance (same for all rows), `label` is the label name,
   * and `choice` is "yes" if this is the correct label, "no"
   * otherwise. This format is returned to make it possible to generate
   * the right sort of data frame in R using the current Scala-to-R
   * interface, which can only pass arrays and arrays of arrays, of
   * fixed type.
   *
   * @param inst Instance, as an aggregate feature vector, to convert into
   *   a matrix.
   * @param correct_label Correct label of instance.
   * @param index 0-based index of the instance.
   */
  def export_aggregate_labeled_instance(inst: AggregateFeatureVector,
      correct_label: LabelIndex, index: Int, removed_features: Set[FeatIndex]
  ) = {
    // This is easier than in the other direction.
    for ((fv, label) <- inst.fv.view.zipWithIndex) yield {
      val indiv = index + 1
      val labelstr = inst.label_mapper.to_string(label)
      val choice = label == correct_label
      assert(fv.length == inst.feature_mapper.vector_length)
      val nums =
        for (i <- 0 until fv.length if !(removed_features contains i)) yield
          fv(i, label)
      ((indiv, labelstr, choice), nums)
    }
  }

  /**
   * Undo the conversion in `import_labeled_instances` to get the long-format
   * 2-d matrix used in R's mlogit() function. For F features, L labels and
   * N instances, The return value is of the following type:
   *
   * (headers: Iterable[String],
   *   data: Iterable[((Int,String,Boolean), Iterable[Double])])
   *
   * where `headers` is a size-F row vector listing column headers,
   * `rows` is a N*L by F matrix of values, and `extraprops` is of
   * size N*L and specifies three columns * `(indiv, label, choice)`,
   * where `indiv` identifies the instance (from 1 to N), `label` is
   * a string identifying the label of the row (cycling through all L
   * labels N times), and `choice` is a boolean, specifying 'true' for
   * the rows whose label is the correct one and 'false' otherwise.
   *
   * This format in turn can be converted into separate variables for
   * `indiv`, `label` and `choice` for easier stuffing into R (the current
   * Scala-to-R interface can only pass 1-d and 2-d arrays of fixed type),
   * or made into a 2-d array of strings for outputting to a file.
   */

  def export_training_data[DI <: DataInstance](
      training_data: TrainingData[DI]
  ) = {
    val insts = training_data.data
    val removed_features = training_data.removed_features

    val frame = "frame" // Name of variable to use for data, etc.

    // This is easier than in the other direction.

    val head = insts.head._1.feature_vector
    val F = head.feature_mapper.number_of_indices
    val headers =
      for (i <- 0 until F if !(removed_features contains i))
        yield head.feature_mapper.to_string(i)
    (headers, insts.view.zipWithIndex.flatMap {
      case ((inst, correct_label), index) =>
        inst.feature_vector match {
          case agg: AggregateFeatureVector =>
            export_aggregate_labeled_instance(agg, correct_label, index,
              removed_features)
          case _ => ???
        }
    })
  }
}

class FeatureStats {
  // Count of features
  val count = intmap[Int]()
  // Count of feature vectors
  var num_fv = 0
  // Min value of features
  val min = doublemap[Int]()
  // Max value of features
  val max = doublemap[Int]()
  // Sum of features
  val sum = doublemap[Int]()
  // Sum of absolute value of features
  val abssum = doublemap[Int]()
}

/**
 * An aggregate feature vector that stores a separate individual feature
 * vector for each of a set of labels.
 */
case class AggregateFeatureVector(
    fv: Array[FeatureVector]
) extends FeatureVector {
  FeatureVector.check_same_mappers(fv)

  def mappers = fv.head.mappers

  def depth = fv.length
  def max_label = depth - 1

  def stored_entries = fv.map(_.stored_entries).sum

  def apply(i: Int, label: LabelIndex) = fv(label)(i, label)

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

  def pretty_print(prefix: String) = {
    (for (d <- 0 until depth) yield
      "Featvec at depth %s(%s): %s" format (
        d, label_mapper.to_string(d),
        fv(d).pretty_print(prefix))).mkString("\n")
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
   * Find the features that have different values from one component
   * feature vector to another. Return a set of such features.
   */
  def accumulate_stats(stats: FeatureStats, do_minmax: Boolean = false,
      do_sum: Boolean = false, do_abssum: Boolean = false) {
    stats.num_fv += depth

    // We do this in a purely iterative fashion to create as little
    // garbage as possible, because we may be working with very large
    // arrays. See find_diff_features.
    for (vec <- fetch_sparse_featvecs; i <- 0 until vec.keys.size) {
      val k = vec.keys(i)
      val v = vec.values(i)
      stats.count(k) += 1
      if (do_minmax) {
        if (!(stats.min contains k) || v < stats.min(k))
          stats.min(k) = v
        if (!(stats.max contains k) || v > stats.max(k))
          stats.max(k) = v
      }
      if (do_sum)
        stats.sum(k) += v
      if (do_abssum)
        stats.abssum(k) += v.abs
    }
  }

  /**
   * Find the features that have different values from one component
   * feature vector to another. Return a set of such features.
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

    accumulate_stats(stats, do_minmax = true)

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
    accumulate_stats(stats, do_sum = true)

    // Compute the mean of all seen instances of a feature.
    // Unseen features are ignored rather than counted as 0's.
    val meanmap =
      stats.sum map { case (key, sum) => (key, sum / stats.count(key)) }

    // Compute the sum of squared differences from the mean of all seen
    // instances of a feature.
    // Unseen features are ignored rather than counted as 0's.
    val sumsqmap = doublemap[Int]()
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
      if (feature_mapper.features_to_rescale contains key) {
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
