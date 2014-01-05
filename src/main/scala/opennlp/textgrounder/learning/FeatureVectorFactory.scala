///////////////////////////////////////////////////////////////////////////////
//  FeatureVectorFactory.scala
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
 * Factories for generating feature vectors for machine learning.
 *
 * @author Ben Wing
 */

import collection.mutable

import util.collection.GroupByIterator
import util.debug.{debug, debugval}
import util.print.{errprint, errfmt}

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
      errprint("Featvec: %s", featvec.pretty_format(""))
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
  def import_dense_labeled_instance(lines: Iterable[Array[String]],
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
      errprint("Feature vector: %s", agg.pretty_format(""))
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
  def import_dense_labeled_instances(lines_iter: Iterator[String],
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
      import_dense_labeled_instance(inst.toIterable, column_types,
        indiv_column, label_column, choice_column,
        is_training)
    }).toIndexedSeq

    if (debug("export-instances")) {
      val (headers, datarows) =
        TrainingData(retval, Set[FeatIndex]()).export_for_mlogit
      errprint("Headers: %s", headers mkString " ")
      errprint("Data rows:")
      errprint(datarows.map(_.toString) mkString "\n")
    }

    retval
  }

  /**
   * Return a pair of `(aggregate, label)` where `aggregate` is an
   * aggregate feature vector derived from the given input source.
   * The input source should have the following format, similar to
   * the TADM event-file format:
   *
   * -- Each data instance consists of a number of lines: first a line
   *    containing only a number giving the number of candidates, followed
   *    by one line per candidate.
   * -- Each candidate line consists of a "frequency of observation"
   *    followed by alternating features and corresponding values.
   *    All items are separated by spaces.
   * -- The correct candidate should have a frequency of 1, and the other
   *    candidates should have a frequency of 0.
   *
   * The difference from the TADM event-file format is that the latter
   * requires that features are given in memoized form (numeric indices
   * starting from 0) and that the second item in a line is the number of
   * feature-value pairs (whereas in this format it is computed from the
   * number of items in the line).
   *
   * @param lines Source lines.
   * @param split_re Regexp to split items in a line.
   * @param is_binary Whether the features are binary-only (no value given)
   *   or potentially numeric (value given).
   * @param is_training Whether we are currently training or testing a model.
   */
  def import_sparse_labeled_instance(lines: Iterator[String],
      split_re: String, is_binary: Boolean, is_training: Boolean) = {

    val nvecs = lines.next.toInt
    var correct_label = -1
    val fvs = for (i <- 0 until nvecs) yield {
      val fields = lines.next.split(split_re)
      val freq = fields(0)
      if (freq == "1") {
        require(correct_label == -1, "Saw two correct labels in same instance")
        correct_label = i
      }
      val feats =
        if (is_binary)
          (for (feat <- fields.drop(1)) yield (FeatBinary, feat, 1.0)).toSeq
        else
          (for (Array(feat, value) <- fields.drop(1).grouped(2))
            yield (FeatRaw, feat, value.toDouble)).toSeq
      make_feature_vector(feats, is_training)
    }

    // Synthesize labels as necessary
    for (i <- 0 until nvecs) {
      label_mapper.to_index(s"#${i + 1}")
    }

    // Aggregate feature vectors, return aggregate with label
    val agg = new AggregateFeatureVector(fvs.toArray)
    if (debug("features")) {
      errprint("Label: %s(%s)", correct_label,
        label_mapper.to_string(correct_label))
      errprint("Feature vector: %s", agg.pretty_format(""))
    }
    (agg, correct_label)
  }

  def import_sparse_labeled_instances(lines: Iterator[String],
      split_re: String, is_binary: Boolean, is_training: Boolean) = {
    val aggs = mutable.Buffer[(AggregateFeatureVector, LabelIndex)]()
    while (lines.hasNext)
      aggs += import_sparse_labeled_instance(lines, split_re, is_binary,
        is_training)
    aggs.toIndexedSeq
  }
}
