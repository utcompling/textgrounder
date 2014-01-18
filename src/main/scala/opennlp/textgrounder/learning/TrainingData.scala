///////////////////////////////////////////////////////////////////////////////
//  TrainingData.scala
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

import java.io.PrintStream

import util.io.localfh
import util.metering._
import util.print.errprint

/**
 * Training data for a classifier or similar.
 *
 * @author Ben Wing
 */

case class TrainingData[DI <: DataInstance](
  data: Iterable[(DI, LabelIndex)],
  removed_features: Set[FeatIndex]
) {
  def check_valid() {
    val head = data.head._1.feature_vector
    // This should force evaluation of `insts` if it's a stream
    // (necessary to ensure we end up outputting the correct length of
    // vector).
    val N = data.size
    val L = head.mapper.number_of_labels
    val F = head.mapper.feature_vector_length
    for (((inst, label), index) <- data.view.zipWithIndex) {
      val fv = inst.feature_vector
      // Equivalent to assert but adds the instance index and the fv itself
      def check(cond: Boolean, msg: => String) {
        assert(cond, "Instance #%s: %s: %s" format (index + 1, msg, fv))
      }
      check(fv.depth == head.depth,
        "has depth %s instead of %s" format (fv.depth, fv.depth))
      check(fv.length == head.length,
        "has length %s instead of %s" format (fv.length, head.length))
      check(fv.mapper == head.mapper, "wrong mapper")
      check(F == fv.length,
        "feature mapper has length %s instead of %s" format (F, fv.length))
      check(L == fv.depth,
        "label mapper has depth %s instead of %s" format (L, fv.depth))
      check(label >= 0 && label < L,
        "label %s out of bounds [0,%s)" format (label, L))

      fv match {
        case agg: AggregateFeatureVector =>
          for (i <- 0 until agg.depth) {
            assert(agg.fv(i).length == agg.length,
              "feature vector %s has length %s instead of %s" format (
                i, agg.fv(i).length, agg.length))
          }
        // assert each row in instance has size == F
        case _ => {}
      }
    }
  }

  check_valid()

  def pretty_print() {
    for (((inst, correct), index) <- data.view.zipWithIndex) {
      val prefix = s"#${index + 1}"
      inst.pretty_print_labeled(prefix, correct)
    }
  }

  /**
   * Compute statistics for determining for how relevant a feature is
   * for discriminating the correct label against the others. Computes
   * two factors, a "correlation factor" and a "significance" factor.
   * The correlation factor compares, among the cases where the feature's
   * value is not the same between the correct label and an incorrect label,
   * the extent to which the the feature's value is greater for the correct
   * label. Values of 1 and 0 indicate perfect correlation, with positive
   * and negative correlation respectively, while a value of 0.5 indicates
   * no correlation. The significance factor measures the extent to which
   * the feature's value is different between the correct label and an
   * incorrect label. A significance factor near 1 means the feature is
   * almost always relevant for comparing two labels while a factor near 0
   * means the feature usually has the same value for both labels (typically
   * it is absent on both, or equivalently has the value 0), and has no
   * power to separate the two.
   *
   * Averaging is actually done an an instance-by-instance basis, rather
   * than a label-by-label (feature-vector-by-feature-vector) basis. If
   * all instances have the same number of labels there will be no
   * difference, but otherwise instances with fewer labels will not be
   * counted less in aggregate, as they would be if averaging were
   * done on a label-by-label basis.
   */
  def compute_feature_discrimination = {
    val stats = new FeatureDiscriminationStats
    for ((inst, label) <- data) {
      val agg = AggregateFeatureVector.check_aggregate(inst)
      stats.accumulate(agg, label)
    }

    stats.greater_than_other.map { case (feat, fracsum) =>
      val greaterfrac = fracsum // / stats.num_agg
      val lessfrac = stats.less_than_other(feat) // / stats.num_agg
      val neqfrac = greaterfrac + lessfrac
      (feat, greaterfrac / neqfrac, neqfrac)
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

  def export_for_mlogit = {
    val frame = "frame" // Name of variable to use for data, etc.

    // This is easier than in the other direction.

    val head = data.head._1.feature_vector
    val F = head.mapper.feature_vector_length
    val headers =
      for (i <- 0 until F if !(removed_features contains i))
        yield head.mapper.feature_to_string(i)
    (headers, data.view.zipWithIndex.flatMap {
      case ((inst, correct_label), index) =>
        inst.feature_vector match {
          case agg: AggregateFeatureVector =>
            TrainingData.export_aggregate_for_mlogit(agg, correct_label,
              index, removed_features)
          case _ => ???
        }
    })
  }

  def export_to_file(filename: String, include_length: Boolean = false,
    memoized_features: Boolean = false, no_values: Boolean = false) {
    val file = localfh.openw(filename)
    errprint("Writing training data to file: %s", filename)
    val task = new Meter("writing", "rerank training instance")
    data.foreachMetered(task) { case (inst, correct_label) =>
      val agg = AggregateFeatureVector.check_aggregate(inst)
      TrainingData.export_aggregate_to_file(file, agg, correct_label,
        include_length = include_length, memoized_features = memoized_features,
        no_values = no_values)
    }
    file.close()
  }
  def export_for_tadm = {
    val frame = "frame" // Name of variable to use for data, etc.

    // This is easier than in the other direction.

    val head = data.head._1.feature_vector
    val F = head.mapper.feature_vector_length
    val headers =
      for (i <- 0 until F if !(removed_features contains i))
        yield head.mapper.feature_to_string(i)
    (headers, data.view.zipWithIndex.flatMap {
      case ((inst, correct_label), index) =>
        inst.feature_vector match {
          case agg: AggregateFeatureVector =>
            TrainingData.export_aggregate_for_mlogit(agg, correct_label,
              index, removed_features)
          case _ => ???
        }
    })
  }
}

object TrainingData {
  def apply[DI <: DataInstance](data: Iterable[(DI, LabelIndex)]):
      TrainingData[DI] = {
    assert(data.size > 0)
    data.head._1.feature_vector match {
      case agg: AggregateFeatureVector =>
        val removed_features =
          AggregateFeatureVector.remove_all_non_choice_specific_columns(data)
        apply(data, removed_features)
      case _ =>
        apply(data, Set[FeatIndex]())
    }
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
  def export_aggregate_for_mlogit(inst: AggregateFeatureVector,
      correct_label: LabelIndex, index: Int, removed_features: Set[FeatIndex]
  ) = {
    // This is easier than in the other direction.
    for ((fv, label) <- inst.fv.view.zipWithIndex) yield {
      val indiv = index + 1
      val labelstr = inst.mapper.label_to_string(label)
      val choice = label == correct_label
      assert(fv.length == inst.mapper.feature_vector_length)
      val nums =
        for (i <- 0 until fv.length if !(removed_features contains i)) yield
          fv(i, label)
      ((indiv, labelstr, choice), nums)
    }
  }

  def export_aggregate_to_file(file: PrintStream,
      inst: AggregateFeatureVector, correct_label: LabelIndex,
      include_length: Boolean = false, memoized_features: Boolean = false,
      no_values: Boolean = false) = {
    val fvs = inst.fetch_sparse_featvecs
    file.println(fvs.size)
    for ((fv, label) <- fvs.zipWithIndex) {
      file.print(if (label == correct_label) "1" else "0")
      val nfeats = fv.keys.size
      if (include_length)
        file.print(" " + nfeats)
      for (i <- 0 until nfeats) {
        val k = fv.keys(i)
        val v = fv.values(i)
        assert(!v.isNaN && !v.isInfinity,
          s"For feature ${fv.format_feature(k)}($k), disallowed value $v")
        if (!memoized_features)
          file.print(" " + fv.format_feature(k))
        else
          file.print(" " + k)
        if (!no_values)
          file.print(" " + v)
      }
      file.println("")
    }
  }
}
