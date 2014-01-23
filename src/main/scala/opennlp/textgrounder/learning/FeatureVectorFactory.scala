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
 * The features are strings, which are internally mapped to integers
 * (of alias type FeatIndex), using the mapping stored in `mapper`.
 */
class SparseFeatureVectorFactory(attach_label: Boolean) { self =>
  val mapper =
    if (attach_label)
      new LabelAttachedFeatureLabelMapper
    else
      new SimpleFeatureLabelMapper

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
      label: LabelIndex, is_training: Boolean) = {
    val memoized_features =
      // DON'T include an intercept term. Not helpful since it's non-
      // label-specific.
      if (is_training)
        feature_values.map {
          case (ty, name, value) =>
            (mapper.note_feature(ty, name, label), value)
        }
       else
        for { (_, name, value) <- feature_values;
               index = mapper.feature_to_index_if(name, label);
               if index != None }
          yield (index.get, value)
    vector_impl match {
      case "TupleArray" =>
        new TupleArraySparseFeatureVector(memoized_features.toBuffer, mapper)
      case "Map" =>
        new MapSparseFeatureVector(memoized_features.toMap, mapper)
      case _ => {
        val (keys, values) =
          memoized_features.toIndexedSeq.sortWith(_._1 < _._1).unzip
        vector_impl match {
          case "DoubleCompressed" =>
            new DoubleCompressedSparseFeatureVector(keys.toArray,
              values.toArray, mapper)
          case "FloatCompressed" =>
            new FloatCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toFloat).toArray, mapper)
          case "IntCompressed" =>
            new IntCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toInt).toArray, mapper)
          case "ShortCompressed" =>
            new ShortCompressedSparseFeatureVector(keys.toArray,
              values.map(_.toShort).toArray, mapper)
        }
      }
    }
  }
}

/**
 * A factory object for creating training data for a classifier. Subclasses
 * have additional class parameters specifying different options for the
 * external format of the data, and the function `import_instances` retrieves
 * the data instances from the external source.
 *
 * @param attach_label Whether we need to make features distinguished according
 *   to the different possible labels, so we get different weights for
 *   different labels when the underlying classifier is single-weight
 *   (as in TADM).
 */
abstract class ExternalDataInstanceFactory(attach_label: Boolean) extends
    SparseFeatureVectorFactory(attach_label) {
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

  def raw_linefeats_to_features(
      raw_linefeats: Iterable[(String, (String, FeatureClass))]
  ) = {
    // Generate appropriate features based on column values, names, types.
    raw_linefeats.map { case (value, (colname, coltype)) =>
      coltype match {
        case NumericFeature => (FeatRaw, colname, value.toDouble)
        case NominalFeature =>
          (FeatBinary, "%s$%s" format (colname, value), 1.0)
      }
    }
  }

  def raw_linefeats_to_featvec(
      raw_linefeats: Iterable[(String, (String, FeatureClass))],
      label: LabelIndex, is_training: Boolean
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
    make_feature_vector(linefeats, label, is_training)
  }

  def import_instances(lines: Iterator[String], is_training: Boolean
    ): IndexedSeq[(FeatureVector, LabelIndex)]
}

/**
 * A factory object for creating training data for a classifier from an
 * external file consisting of one instance per line.
 *
 * @param attach_label Whether we need to make features distinguished according
 *   to the different possible labels, so we get different weights for
 *   different labels when the underlying classifier is single-weight
 *   (as in TADM).
 */
abstract class SimpleDataInstanceFactory(
  attach_label: Boolean
) extends ExternalDataInstanceFactory(attach_label) {
  /**
   * Return a sequence of pairs of `(feature_vector, label)` consisting of
   * a feature vector derived from a given and the corresponding correct
   * label. The first line should list the column headings.
   *
   * @param lines_iter Iterator over lines in the file.
   * @param is_training Whether we are currently training or testing a model.
   */
  def compute_instances(
      features_labels: Iterator[(Iterable[(FeatureValue, String, Double)],
        LabelIndex)],
      is_training: Boolean) = {

    (for ((featvals, correct_label) <- features_labels) yield {

      val featvec = if (attach_label) {
        // The aggregate feature vector is derived from the given line and
        // contains separate feature vectors for each possible label, with
        // separate label-attached features.
        val fvs = for (i <- 0 until mapper.number_of_labels) yield
          make_feature_vector(featvals, i, is_training)
        AggregateFeatureVector(fvs.toArray)
      } else {
        // The label isn't used here.
        make_feature_vector(featvals, -1, is_training)
      }

      // Return instance
      if (debug("features")) {
        errprint("Label: %s(%s)", correct_label,
          mapper.label_to_string(correct_label))
        errprint("Featvec: %s", featvec.pretty_format(""))
      }

      (featvec, correct_label)
    }).toIndexedSeq
  }
}

/**
 * A factory object for creating training data for a classifier from an
 * external file consisting of one instance per line, in a dense format
 * where each feature is a separate column and one of the columns specifies
 * the correct label. The features in the columns may be nominal or
 * numeric, and this is figured out automatically. (Nominal values
 * have no ordering or other numerical significance and are treated as
 * separate binary-valued features. The label is always treated as nominal.)
 *
 * All lines should have the same number of columns. The regexp to split
 * columns can be specified. The first line is a header, specifying the
 * names of the columns.
 *
 * @param attach_label Whether we need to make features distinguished according
 *   to the different possible labels, so we get different weights for
 *   different labels when the underlying classifier is single-weight
 *   (as in TADM).
 * @param split_re Regexp to split columns in a line.
 * @param label_column Column specifying the label; used to fetch
 *   the label and removed before creating features. A numeric index
 *   starting at 0; can be negative, in which case it counts from the
 *   end. Most common values are 0 (first column holds label) and -1
 *   (last column holds label).
 */
class SimpleDenseDataInstanceFactory(
  attach_label: Boolean, split_re: String, label_column: Int
) extends SimpleDataInstanceFactory(attach_label) {
  /**
   * Return a pair of `(raw_linefeats, label)` consisting of the raw features
   * extracted from the given line along with the correct label.
   *
   * @param line Line specifying feature values, already split.
   * @param columns Names of columns and corresponding feature type.
   *   There should be the same number of columns as items in each of the
   *   arrays.
   * @param label_colind Column specifying the label; always non-negative.
   * @param is_training Whether we are currently training or testing a model.
   */
  private def get_raw_features(line: Array[String],
      columns: Iterable[(String, FeatureClass)],
      label_colind: Int) = {
    // Check the right length for the line
    require(line.size == columns.size, "Expected %s columns but saw %s: %s"
      format (columns.size, line.size, line mkString "\t"))

    val label = mapper.label_to_index(line(label_colind))

    // Filter out the columns we don't use, pair each with its column spec.
    val raw_linefeats = line.zip(columns).zipWithIndex.filter {
      case (value, index) => index != label_colind
    }.map(_._1)

    (raw_linefeats, label)
  }

  /**
   * Return a sequence of pairs of `(feature_vector, label)` consisting of
   * a feature vector derived from a given and the corresponding correct
   * label. The first line should list the column headings.
   *
   * @param lines_iter Iterator over lines in the file.
   * @param is_training Whether we are currently training or testing a model.
   */
  def import_instances(lines_iter: Iterator[String], is_training: Boolean) = {
    val (lines, columns) = get_columns(lines_iter, split_re)
    val numcols = columns.size
    val label_colind = get_index(numcols, label_column)
    // If `attach_label`, we need to go through and find all the labels first,
    // so we can create separate feature vectors for each one.
    if (attach_label) {
      for (line <- lines)
        mapper.label_to_index(line(label_colind))
    }

    val features_labels = lines.map { line =>
      // Compute raw features based on column values, names, types.
      val (raw_linefeats, label) = get_raw_features(line, columns, label_colind)
      (raw_linefeats_to_features(raw_linefeats), label)
    }

    compute_instances(features_labels.toIterator, is_training)
  }
}

/**
 * A factory object for creating training data for a classifier from an
 * external file consisting of label-specific data, where each data instance
 * has separate feature values for each possible label. For example, the
 * labels may represent different transportation modes (car, bus, train,
 * plane) and the data instances are particular trips to be taken by
 * particular people, where some features (e.g. income and number of people
 * in the party) are the same for all labels but some features differ from
 * label to label (e.g. cost of trip, time to take trip, comfort of trip).
 *
 * The data is in the dense format used by R's conditional logit package
 * ('mlogit'). Each data instance consists of a number of lines, with one
 * line per label, with a fixed number of columns specifying the feature
 * values for each combination of instance and label. The features in the
 * columns may be nominal or numeric, and this is figured out automatically.
 * (Nominal values have no ordering or other numerical significance and are
 * treated as separate binary-valued features. The label is always treated
 * as nominal.) There are three extra columns per line that do not specify
 * feature values:
 *
 * -- The 'indiv' column specifies the individual index, i.e. the index of
 *    the data instance. This should be the same for all lines in a given
 *    instance and should generally be numeric, starting at 0.
 * -- The 'label' column specifies the label of the line. This is a nominal
 *    (non-numeric) value.
 * -- The 'choice' column indicates whether the label on this line is the
 *    correct one. It should be either "yes" or "true" if so, "no" or
 *    "false" otherwise (either upper or lower case).
 *
 * All lines should have the same number of columns. The regexp to split
 * columns can be specified. The first line is a header, specifying the
 * names of the columns.
 *
 * @param split_re Regexp to split columns in a line.
 * @param indiv_column Column specifying the individual index; removed
 *   before creating features. This is a zero-based index into the
 *   list of columns.
 * @param label_column Column specifying the label; used to fetch
 *   the label and removed before creating features.
 * @param choice_column Column specifying "yes" or "no" identifying
 *   whether the label on this line was chosen by this individual. There
 *   should be exactly one per set of lines.
 */
class MLogitDenseLabelSpecificDataInstanceFactory(attach_label: Boolean,
  split_re: String, indiv_column: Int, label_column: Int, choice_column: Int
) extends ExternalDataInstanceFactory(attach_label) {
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
  private def import_instance(lines: Iterable[Array[String]],
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
      (mapper.label_to_index(line(label_colind)), line(choice_colind))
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
    require(num_labels_seen == mapper.number_of_labels,
      "Not all labels found: Expected %s labels but saw %s: %s" format (
        mapper.number_of_labels, num_labels_seen,
        labels_seen.toSeq.sorted))

    // Extract the feature vectors
    val fvs = lines.map { line =>
      // Filter out the columns we don't use, pair each with its column spec.
      val raw_linefeats = line.zip(columns).zipWithIndex.filter {
        case (value, index) =>
          index != indiv_colind && index != label_colind &&
          index != choice_colind
      }.map(_._1)
      val label = mapper.label_to_index(line(label_colind))

      // Generate feature vector based on column values, names, types.
      raw_linefeats_to_featvec(raw_linefeats, label, is_training)
    }

    // Order by increasing label so that all aggregates have the labels in
    // the same order.
    assert(fvs.size == labels_seen.size)
    val sorted_fvs = (fvs zip labels_seen).toSeq.sortBy(_._2).map(_._1)

    // Aggregate feature vectors, return aggregate with label
    val agg = AggregateFeatureVector(fvs.toArray)
    if (debug("features")) {
      errprint("Label: %s(%s)", label, mapper.label_to_string(label))
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
   * @param is_training Whether we are currently training or testing a model.
   */
  def import_instances(lines_iter: Iterator[String], is_training: Boolean) = {
    val (lines, column_types) = get_columns(lines_iter, split_re)
    val numcols = column_types.size
    val indiv_colind = get_index(numcols, indiv_column)
    val label_colind = get_index(numcols, label_column)
    val choice_colind = get_index(numcols, choice_column)

    val grouped_instances = new GroupByIterator(lines.toIterator,
      { line: Array[String] => line(indiv_column).toInt })
    val retval = (for ((_, inst) <- grouped_instances) yield {
      import_instance(inst.toIterable, column_types,
        indiv_colind, label_colind, choice_colind, is_training)
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
}

/**
 * A factory object for creating training data for a classifier from an
 * external file consisting of one instance per line, in a sparse format.
 * The first item on the line is the label, and the remainder are the
 * features, either alternating feature names and values, or (when
 * `is_binary` is specified) just binary feature names, which will be given
 * a value of 1.
 *
 * @param attach_label Whether we need to make features distinguished according
 *   to the different possible labels, so we get different weights for
 *   different labels when the underlying classifier is single-weight
 *   (as in TADM).
 * @param split_re Regexp to split items in a line.
 * @param is_binary Whether the features are binary-only (no value given)
 *   or potentially numeric (value given).
 */
class SimpleSparseDataInstanceFactory(
  attach_label: Boolean,
  split_re: String,
  is_binary: Boolean
) extends SimpleDataInstanceFactory(attach_label) {
  /**
   * Return a pair of `(featvals, label)` where `featvals` is the
   * features/values to be passed to `make_feature_vector`.
   *
   * @param fields Source line, split into fields.
   */
  private def get_features_labels(fields: Array[String]) = {
    val label = mapper.label_to_index(fields(0))
    val feats =
      if (is_binary)
        (for (feat <- fields.drop(1)) yield (FeatBinary, feat, 1.0)).toSeq
      else
        (for (Array(feat, value) <- fields.drop(1).grouped(2))
          yield (FeatRaw, feat, value.toDouble)).toSeq
    (feats, label)
  }

  def import_instances(lines_iter: Iterator[String], is_training: Boolean) = {
    val split_lines = lines_iter.map { _.split(split_re) }.toIterable
    if (attach_label) {
      for (fields <- split_lines)
        mapper.label_to_index(fields(0))
    }
    val features_labels = split_lines.map(get_features_labels)
    compute_instances(features_labels.toIterator, is_training)
  }
}

/**
 * A factory object for creating training data for a classifier from an
 * external file consisting of label-specific data, where each data instance
 * has separate feature values for each possible label. See
 * `DenseLabelSpecificDataInstanceFactory` for a description of an example
 * of data of this sort.
 *
 * This data format is often used for creating rankers, where in place of
 * semantically distinct labels there are a series of candidates, which are
 * labeled only by numeric rank. The different ranks are typically considered
 * semantically equivalent, meaning there aren't separate weights for a
 * given feature combined with different labels.
 *
 * The input source should have the following format, similar to
 * the TADM event-file format:
 *
 * -- Each data instance consists of a number of lines: first a line
 *    containing only a number giving the number of candidates, followed
 *    by one line per candidate.
 * -- Each candidate line consists of a "frequency of observation"
 *    followed by alternating feature names and corresponding values.
 *    (Alternatively, if all features are binary, only the feature names
 *    need to be specified, and all features present will be given the
 *    value of 1. This is controlled by the `is_binary` argument below.)
 * -- The correct candidate should have a frequency of 1, and the other
 *    candidates should have a frequency of 0.
 * -- Commonly, the items on a line are separated by spaces, but this is
 *    controllable.
 *
 * The differences from the TADM event-file format are that the latter
 * requires that features are given in memoized form (numeric indices
 * starting from 0); that the second item in a line is the number of
 * feature-value pairs (whereas in this format it is computed from the
 * number of items in the line); that items must be separated by spaces;
 * and that the binary feature-only format is not allowed.
 */
class TADMSparseLabelSpecificDataInstanceFactory(
  attach_label: Boolean,
  split_re: String,
  is_binary: Boolean
) extends ExternalDataInstanceFactory(attach_label) {
  /**
   * Return a pair of `(aggregate, label)` where `aggregate` is an
   * aggregate feature vector derived from the given input source.
   *
   * @param lines Source lines.
   * @param split_re Regexp to split items in a line.
   * @param is_binary Whether the features are binary-only (no value given)
   *   or potentially numeric (value given).
   * @param is_training Whether we are currently training or testing a model.
   */
  private def import_instance(lines: Iterator[String], is_training: Boolean) = {
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
      make_feature_vector(feats, i, is_training)
    }

    // Synthesize labels as necessary
    for (i <- 0 until nvecs) {
      mapper.label_to_index(s"#${i + 1}")
    }

    // Aggregate feature vectors, return aggregate with label
    val agg = AggregateFeatureVector(fvs.toArray)
    if (debug("features")) {
      errprint("Label: %s(%s)", correct_label,
        mapper.label_to_string(correct_label))
      errprint("Feature vector: %s", agg.pretty_format(""))
    }
    (agg, correct_label)
  }

  def import_instances(lines: Iterator[String], is_training: Boolean) = {
    val aggs = mutable.Buffer[(AggregateFeatureVector, LabelIndex)]()
    while (lines.hasNext)
      aggs += import_instance(lines, is_training)
    aggs.toIndexedSeq
  }
}
