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

  def pretty_print(prefix: String): String
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

  def pretty_print(prefix: String) = "  %s: %s" format (prefix, values)
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

class FeatureMapper extends ToIntMemoizer[String] {
  val intercept_feature = memoize("$intercept")
}

class LabelMapper extends ToIntMemoizer[String] {
  def to_index(label: String) = memoize(label) - minimum_index
  def to_label(index: Int) = unmemoize(index + minimum_index)
  def number_of_labels = number_of_valid_indices
}

/**
 * A factory object for creating sparse feature vectors for classification.
 * Sparse feature vectors store only the features with non-zero values.
 * The features are indexed by entities of type T, which are internally
 * mapped to integers, using the mapping stored in `feature_mapper`.
 * There will always be a feature with the index 0, value 1.0, to handle
 * the intercept term.
 */
class SparseFeatureVectorFactory { self =>
  val label_mapper = new LabelMapper

  val vector_impl = debugval("featvec") match {
    case f@("DoubleCompressed" | "FloatCompressed" | "IntCompressed" |
            "ShortCompressed" | "TupleArray" | "Map") => f
    case "" => "DoubleCompressed"
  }

  val feature_mapper = new FeatureMapper

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
    override def format_feature(index: Int) = feature_mapper.unmemoize(index)
    def length = feature_mapper.maximum_index + 1
  }

  /**
   * Generate a feature vector.  If not at training time, we need to be
   * careful to skip features not seen during training because there won't a
   * corresponding entry in the weight vector, and the resulting feature would
   * containing a non-existent index, causing a crash during lookup (e.g.
   * during the dot-product operation).
   */
  def make_feature_vector(feature_values: Iterable[(String, Double)],
      is_training: Boolean) = {
    val memoized_features =
      // Include an intercept term
      Iterable(feature_mapper.intercept_feature -> 1.0) ++: (
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

sealed abstract class FeatureType
case object NominalFeature extends FeatureType
case object NumericFeature extends FeatureType

class SparseInstanceFactory extends SparseFeatureVectorFactory {
  // Format a series of lines for debug display.
  def format_lines(lines: TraversableOnce[Array[String]]) = {
    lines.map(line => errfmt(line mkString "\t")).mkString("\n")
  }

  def get_index(numcols: Int, index: Int) = {
    val retval =
      if (index < 0) numcols + index
      else index
    require(retval >= 0 && retval < numcols,
      "Index %s out of bounds: Should be in [%s,%s)" format (
        index, -numcols, numcols))
    retval
  }

  def get_columns(lines_iter: Iterator[String], split_re: String) = {
    val all_lines = lines_iter.toIterable
    val columns = all_lines.head.split(split_re)
    val coltype = mutable.Map[String, FeatureType]()
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
      raw_linefeats: Iterable[(String, (String, FeatureType))],
      is_training: Boolean
  ) = {
    // Generate appropriate features based on column values, names, types.
    val linefeats = raw_linefeats.map { case (value, (colname, coltype)) =>
      coltype match {
        case NumericFeature => (colname, value.toDouble)
        case NominalFeature => ("%s$%s" format (colname, value), 1.0)
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
   * @param choice_column Column specifying the choice; used to fetch
   *   the label and removed before creating features.
   * @param is_training Whether we are currently training or testing a model.
   */
  def get_labeled_instance(line: Array[String],
      columns: Iterable[(String, FeatureType)], choice_column: Int,
      is_training: Boolean) = {

    // Check the right length for the line
    require(line.size == columns.size, "Expected %s columns but saw %s: %s"
      format (columns.size, line.size, line mkString "\t"))

    val label = label_mapper.to_index(line(choice_column))

    // Filter out the columns we don't use, pair each with its column spec.
    val raw_linefeats = line.zip(columns).zipWithIndex.filter {
      case (value, index) => index != choice_column
    }.map(_._1)

    // Generate feature vector based on column values, names, types.
    val featvec = raw_linefeats_to_featvec(raw_linefeats, is_training)

    // Return instance
    if (debug("features")) {
      errprint("Label: %s(%s)", label, label_mapper.to_label(label))
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
   * @param choice_column Column specifying the choice; used to fetch
   *   the label and removed before creating features.
   * @param is_training Whether we are currently training or testing a model.
   */
  def get_labeled_instances(lines_iter: Iterator[String],
      split_re: String, choice_column: Int,
      is_training: Boolean) = {
    val (lines, column_types) = get_columns(lines_iter, split_re)
    val numcols = column_types.size
    val choice_colind = get_index(numcols, choice_column)
    for (inst <- lines) yield {
      get_labeled_instance(inst, column_types, choice_colind, is_training)
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
   * @param lines Lines corresponding to different choices of an individual.
   * @param columns Names of columns and corresponding feature type.
   *   There should be the same number of columns as items in each of the
   *   arrays.
   * @param indiv_colind Column specifying the individual index; removed
   *   before creating features. This is a zero-based index into the
   *   list of columns.
   * @param choice_colind Column specifying the choice; used to fetch
   *   the label and removed before creating features.
   * @param choice_yesno_colind Column specifying "yes" or "no" identifying
   *   whether the choice on this line was made by this individual. There
   *   should be exactly one per set of lines.
   * @param is_training Whether we are currently training or testing a model.
   */
  def get_labeled_instance(lines: Iterable[Array[String]],
      columns: Iterable[(String, FeatureType)],
      indiv_colind: Int, choice_colind: Int, choice_yesno_colind: Int,
      is_training: Boolean) = {

    val numcols = columns.size

    // Check the right lengths for the lines
    for (line <- lines) {
      require(line.size == numcols, "Expected %s columns but saw %s: %s"
        format (numcols, line.size, line mkString "\t"))
    }

    // Retrieve the label, make sure there's exactly 1
    val choice_yesno = lines.map { line =>
      (label_mapper.to_index(line(choice_colind)), line(choice_yesno_colind))
    }
    val label_lines = choice_yesno.filter { _._2 == "yes" }
    require(label_lines.size == 1,
      "Expected exactly one label but saw %s: %s: lines:\n%s\n" format (
        label_lines.size, label_lines.map(_._1), format_lines(lines)))
    val label = label_lines.head._1

    // Make sure all possible choices seen.
    val choices_seen = choice_yesno.map(_._1)
    val num_choices_seen = choices_seen.toSet.size
    require(num_choices_seen == label_mapper.number_of_labels,
      "Not all choices found: Expected %s choices but saw %s: %s" format (
        label_mapper.number_of_labels, num_choices_seen,
        choices_seen.toSeq.sorted))

    // Extract the feature vectors
    val fvs = lines.map { line =>
      // Filter out the columns we don't use, pair each with its column spec.
      val raw_linefeats = line.zip(columns).zipWithIndex.filter {
        case (value, index) =>
          index != indiv_colind && index != choice_colind &&
          index != choice_yesno_colind
      }.map(_._1)

      // Generate feature vector based on column values, names, types.
      raw_linefeats_to_featvec(raw_linefeats, is_training)
    }

    // Order by increasing label so that all aggregates have the choices in
    // the same order.
    assert(fvs.size == choices_seen.size)
    val sorted_fvs = (fvs zip choices_seen).toSeq.sortBy(_._2).map(_._1)

    // Aggregate feature vectors, return aggregate with label
    val agg = new AggregateFeatureVector(fvs.toIndexedSeq,
      feature_mapper, label_mapper)
    if (debug("features")) {
      errprint("Label: %s(%s)", label, label_mapper.to_label(label))
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
   * @param choice_column Column specifying the choice; used to fetch
   *   the label and removed before creating features.
   * @param choice_yesno_column Column specifying "yes" or "no" identifying
   *   whether the choice on this line was made by this individual. There
   *   should be exactly one per set of lines.
   * @param is_training Whether we are currently training or testing a model.
   */
  def get_labeled_instances(lines_iter: Iterator[String],
      split_re: String,
      indiv_column: Int, choice_column: Int, choice_yesno_column: Int,
      is_training: Boolean) = {
    val (lines, column_types) = get_columns(lines_iter, split_re)
    val numcols = column_types.size
    val indiv_colind = get_index(numcols, indiv_column)
    val choice_colind = get_index(numcols, choice_column)
    val choice_yesno_colind = get_index(numcols, choice_yesno_column)

    val grouped_instances = new GroupByIterator(lines.toIterator,
      { line: Array[String] => line(indiv_column).toInt })
    val retval = (for ((_, inst) <- grouped_instances) yield {
      get_labeled_instance(inst.toIterable, column_types,
        indiv_column, choice_column, choice_yesno_column,
        is_training)
    }).toIterable

    if (debug("put-instances")) {
      val (headers, indiv, choice, choice_yesno, data_rows) =
        put_labeled_instances(retval)
      errprint("Headers: %s", headers mkString " ")
      errprint("Indiv (column vector): %s", indiv mkString " ")
      errprint("Choice (column vector): %s", choice mkString " ")
      errprint("Choice-yesno (column vector): %s", choice_yesno mkString " ")
      errprint("Data rows:")
      errprint(data_rows.map(_ mkString " ") mkString "\n")
    }

    retval
  }

  /**
   * Undo the conversion in `get_labeled_instance`, converting the instance
   * back to the 2-d matrix format used in R's mlogit() function. For F
   * features and L labels this will have the following type:
   *
   * Array[(Int, String, String), Array[Double]]
   *
   * where there are L elements ("rows") in the first-level array and
   * F elements ("columns") in the second-level array. The tuple is of
   * `(indiv-index, choice, choice-yesno)` where `indiv-index` is the
   * index of the instance (same for all rows), `choice` is the label name,
   * and `choice-yesno` is "yes" if this is the correct label, "no"
   * otherwise. This format is returned to make it possible to generate
   * the right sort of data frame in R using the current Scala-to-R
   * interface, which can only pass arrays and arrays of arrays, of
   * fixed type.
   */
  def put_labeled_instance(inst: AggregateFeatureVector,
      correct_label: Int, index: Int) = {
    // This is easier than in the other direction.
    (for ((fv, label) <- inst.fv.zipWithIndex) yield {
      val indiv = index + 1
      val choice = label_mapper.to_label(label)
      val choice_yesno = if (label == correct_label) "yes" else "no"
      // Feature vectors are currently indexed directly by the memoized
      // index (even though index 0 isn't used).
      assert(fv.length == feature_mapper.maximum_index + 1)
      val nums =
        // Discard invalid indices (currently, index 0)
        (for (i <- feature_mapper.minimum_index until fv.length)
          yield fv(i, label)).toArray
      ((indiv, choice, choice_yesno), nums)
    }).toArray
  }

  /**
   * Undo the conversion in `get_labeled_instances` to get the long-format
   * 2-d matrix used in R's mlogit() function. The return value is of the
   * following type:
   *
   * (headers: Array[String], indiv:Array[Int], choices:Array[String],
   *   choices_yesno:Array[String], data_rows:Array[Array[Double]])
   *
   * where `data_rows` has N*L rows and F columns, `headers` is a size-F
   * row vector listing column headers, `indiv` is a size-N*L column
   * vector identifying the instance (from 1 to N) each row occurs in,
   * `choice` is a size-N*L column vector specifying the label of each
   * row (cycling through all labels repeated N times), and `choices_yesno`
   * is a size-N*L column vector specifying "yes" for the rows with
   * correct labels and "no" otherwise. This format is used to make it
   * possible to generate the right sort of data frame in R using the
   * current Scala-to-R interface, which can only pass arrays and arrays
   * of arrays, of fixed type.
   */
  def put_labeled_instances(insts: Iterable[(AggregateFeatureVector, Int)]) = {
    // This is easier than in the other direction.
    val headers =
      for (i <- feature_mapper.minimum_index to feature_mapper.maximum_index)
        yield feature_mapper.unmemoize(i)
    val rows =
      insts.zipWithIndex.flatMap {
        case ((inst, correct_label), index) =>
          put_labeled_instance(inst, correct_label, index)
      }
    val (extra_props, data_rows) = rows.unzip
    val (indiv, choice, choice_yesno) = extra_props.unzip3
    val N = insts.size
    val L = label_mapper.number_of_labels
    val F = feature_mapper.number_of_valid_indices
    assert(headers.size == F)
    assert(choice.size == N*L)
    assert(choice_yesno.size == N*L)
    assert(indiv.size == N*L)
    assert(data_rows.size == N*L)
    data_rows.map { row => assert(row.size == F) }
    (headers, indiv, choice, choice_yesno, data_rows)
  }
}

/**
 * An aggregate feature vector that stores a separate individual feature
 * vector for each of a set of labels.
 */
case class AggregateFeatureVector(
    fv: IndexedSeq[FeatureVector],
    feature_mapper: FeatureMapper,
    label_mapper: LabelMapper
) extends FeatureVector {
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

  def pretty_print(prefix: String) = {
    (for (d <- 0 until depth) yield
      "Featvec at depth %s(%s): %s" format (
        d, label_mapper.to_label(d),
        fv(d).pretty_print(prefix))).mkString("\n")
  }
}
