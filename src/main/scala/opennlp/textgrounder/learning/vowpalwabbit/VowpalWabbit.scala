///////////////////////////////////////////////////////////////////////////////
//  VowpalWabbit.scala
//
//  Copyright (C) 2014 Ben Wing, The University of Texas at Austin
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
package learning.vowpalwabbit

import scala.sys.process._

import learning._
import util.debug._
import util.io.localfh
import util.metering._
import util.print.errprint

/**
 * This implements an interface onto the Vowpal Wabbit machine learning
 * package.
 *
 * Vowpal Wabbit assumes the following format for data in the
 * non-label-specific format:
 *
 * Each data instance consists of one line: A class (pretty much any string
 * without a colon, | or space in it), followed by a |, a space, and
 * either features or feature:value pairs, separated by spaces, e.g.
 *
 * 1 | height:1.5 length:2.0 NumberOfLegs:4.0 HasStripes:1.0
 *
 * It's also possible to group features into "namespaces", which is
 * used for presentation purposes mainly (note the lack of space after
 * the pipe symbol):
 *
 *1 |MetricFeatures height:1.5 length:2.0 |OtherFeatures NumberOfLegs:4.0 HasStripes:1.0
 *
 * There's also a cost-sensitive format (with option -csoaa or -wap,
 * standing for "cost-sensitive one against all" or "weighted all pairs"):
 *
 * 1:0 2:3 3:1.5 4:1 | foo:1 bar:2 baz bat
 *
 * In this example, there are four labels, with given costs. The correct
 * label is #1, with 0 cost.
 *
 * There's also a cost-sensitive label-dependent format (with option
 * -csoaa_ldf or -wap_ldf, which takes an argument that should be either
 * "multiline" or "singleline"; presumably "multiline" is what's described
 * below). See this post from Hal Daume:
 *
 * https://groups.yahoo.com/neo/groups/vowpal_wabbit/conversations/topics/626
 *
 * Hal gives the following cost-sensitive example:
 *
 * 1:1.0 2:0.0 3:2.0 | a b c
 * 1:1.0 2:0.0 | b c d
 * 1:1.0 3:2.0 | a b c
 *
 * and the equivalent in label-dependent format:
 *
 * 1 1.0 | a_1 b_1 c_1
 * 2 0.0 | a_2 b_2 c_2
 * 3 2.0 | a_3 b_3 c_3
 *
 * 1 1.0 | b_1 c_1 d_1
 * 2 0.0 | b_2 c_2 d_2
 *
 * 1 1.0 | a_1 b_1 c_1
 * 3 2.0 | a_3 b_3 c_3
 *
 * It's not necessary to memoize features to indices, and might be better
 * not to, because VW will do that for you using feature hashing.
 *
 * The raw prediction file looks like this:
 *
 * 1:-1.98894 2:-1.45385 3:-0.857979 4:-0.495489 5:-1.13179
 * 1:-1.86325 2:-1.32872 3:-0.73537 4:-0.617061 5:-1.01349
 * 1:-1.97973 2:-1.20363 3:-0.853884 4:-0.736385 5:-1.13149
 * 1:-1.86325 2:-1.32872 3:-0.73537 4:-0.617061 5:-1.01349
 * 1:-1.86325 2:-1.32872 3:-0.73537 4:-0.617061 5:-1.01349
 * 1:-1.97981 2:-1.44635 3:-0.611598 4:-0.736412 5:-1.1315
 * 1:-1.97967 2:-1.44623 3:-0.853883 4:-0.736452 5:-0.889268
 * 1:-2.03975 2:-1.51089 3:-0.743874 4:-0.568494 5:-1.21727
 * ...
 */

/**
 * Common code to the various Vowpal Wabbit trainers and classifiers.
 */
protected trait VowpalWabbitBase {
  def train_vw(feats_filename: String, gaussian: Double, lasso: Double,
      vw_loss_function: String, vw_args: String, extra_args: Seq[String]) = {
    // We make the writing happen in a different step because the number of
    // classes might not be known until we do so.
    //
    // We need two temporary files in addition to the feature file:
    // one used by VW as a cache, and one where the model is written to.
    // The last one needs to be preserved for evaluation.
    val cache_filename =
      java.io.File.createTempFile("textgrounder.vw.cache.train", null).toString
    val model_filename =
      java.io.File.createTempFile("textgrounder.vw.model", null).toString
    errprint(s"Writing VW training cache to $cache_filename")
    errprint(s"Writing VW model to $model_filename")
    // The options mean:
    //
    // -k: If the cache file already exists, overwrite it instead of using it
    // -cache_file: Store a processed version of the training data here
    // -data: Where to read raw training data from
    // -f: Where to write computed model to
    // --oaa: Use one-against-all reduction for handling multiclass learning,
    //        with specified number of classes
    // --loss_function: Use this loss function. Possibilities are "logistic"
    //        (do logistic regression), "hinge" (implement an SVM),
    //        "squared" (do least-squares linear regression), "quantile"
    //        (do quantile regression, trying to predict the median or some
    //        other quantile, rather than the mean as with least squares).
    // --compressed: Compress the cache file.
    val vw_cmd_line =
      Seq("vw", "-k", "--cache_file", cache_filename, "--data", feats_filename,
          "-f", model_filename) ++
        extra_args ++
        Seq("--loss_function", vw_loss_function, "--compressed") ++
        (if (gaussian > 0) Seq("--l2", s"$gaussian") else Seq()) ++
        (if (lasso > 0) Seq("--l1", s"$lasso") else Seq()) ++
        // Split on an empty string wrongly returns Array("")
        (if (vw_args == "") Seq() else vw_args.split("""\s+""").toSeq)
    errprint("Executing: %s", vw_cmd_line mkString " ")
    time_action("running VowpalWabbit") {
      vw_cmd_line !
    }
    new VowpalWabbitClassifier(model_filename)
  }

  /**
   * Write out a set of data instances, described by their features,
   * to a file.
   *
   * @param features An iterator over data instances. Each instance is
   *   a tuple of an iterable over features and a memoized correct label.
   *   Each feature is described by a feature type (ignored), a
   *   feature name and a value.
   * @param file File to write features to.
   */
  def write_feature_file(
    features: Iterator[(Iterable[(FeatureValue, String, Double)], LabelIndex)]
  ) = {
    var feats_filename =
      java.io.File.createTempFile("textgrounder.vw.feats.test", null).toString
    errprint(s"Writing features to $feats_filename")

    val task = new Meter("processing features in", "document")

    val f = localfh.openw(feats_filename)
    features.foreachMetered(task) { case (feats, correct) =>
      if (debug("features")) {
        val prefix = "#%s" format (task.num_processed + 1)
        errprint(s"$prefix: Correct = $correct")
        feats.foreach { case (_, feat, value) =>
          errprint(s"  $prefix: $feat = $value")
        }
      }
      // Map 0-based memoized labels to 1-based labels for VW, which
      // seems to want labels in this format.
      val line = s"${correct + 1} | " +
        feats.map { case (_, feat, value) =>
          val goodfeat = feat.replace(":", "_")
          s"$goodfeat:$value"
        }.mkString(" ")
      f.println(line)
    }
    f.close()
    feats_filename
  }

  /**
   * Write out a set of cost-sensitive data instances, described by their
   * features and costs per label, to a file.
   *
   * @param features An iterator over data instances. Each instance is
   *   a tuple of an iterable over features and an iterable over per-label
   *   costs. Each feature is described by a feature type (ignored), a
   *   feature name and a value.
   * @param file File to write features to.
   */
  def write_cost_sensitive_feature_file(
    features: Iterator[(Iterable[(FeatureValue, String, Double)],
      Iterable[(LabelIndex, Double)])]
  ) = {
    var feats_filename =
      java.io.File.createTempFile("textgrounder.vw.feats.test", null).toString
    errprint(s"Writing features to $feats_filename")

    val task = new Meter("processing features in", "document")

    val f = localfh.openw(feats_filename)
    features.foreachMetered(task) { case (feats, costs) =>
      if (debug("features")) {
        val prefix = "#%s" format (task.num_processed + 1)
        errprint(s"$prefix: ${costs.size} labels, ${feats.size} features")
        costs.foreach { case (label, cost) =>
          errprint(s"  $prefix: Label $label: cost $cost")
        }
        feats.foreach { case (_, feat, value) =>
          errprint(s"  $prefix: $feat = $value")
        }
      }
      // Map 0-based memoized labels to 1-based labels for VW, which
      // seems to want labels in this format.
      val line =
        costs.map { case (label, cost) =>
          s"${label + 1}:$cost"
        }.mkString(" ") +
        " | " +
        feats.map { case (_, feat, value) =>
          val goodfeat = feat.replace(":", "_")
          s"$goodfeat:$value"
        }.mkString(" ")
      f.println(line)
    }
    f.close()
    feats_filename
  }
}

/**
 * Classify data instances using VowpalWabbit. When applied to a set of
 * feature vectors, return a set of vectors holding scores, which should
 * be convertible to probabilities by exponentiating and normalizing.
 * As when training, you need to write out the features first, then pass
 * in the feature file name.
 */
class VowpalWabbitClassifier private[vowpalwabbit] (
  val model_filename: String
) extends VowpalWabbitBase {
  def apply(feats_filename: String, verbose: Boolean = true) = {
    // We need two temporary files: one used by VW as a cache, and one
    // where VW writes the raw predictions. In addition we need the file
    // holding the features and the file holding the written-out model.
    val cache_filename =
      java.io.File.createTempFile("textgrounder.vw.cache.test", null).toString
    val pred_filename =
      java.io.File.createTempFile("textgrounder.vw.pred", null).toString
    val vw_cmd_line =
      Seq("vw", "-k", "--cache_file", cache_filename, "--data", feats_filename,
          "-i", model_filename, "--raw_predictions", pred_filename,
          "--compressed", "-t") ++ (if (verbose) Seq() else Seq("--quiet"))
    if (verbose) {
      errprint(s"Writing VW eval cache to $cache_filename")
      errprint(s"Writing VW raw predictions to $pred_filename")
      errprint("Executing: %s", vw_cmd_line mkString " ")
    }
    time_action("running VowpalWabbit") {
      vw_cmd_line !
    }
    (for (line <- localfh.openr(pred_filename)) yield {
      val preds_1 = line.split("""\s+""")
      preds_1.map(_.split(":")).map {
        // Map 1-based VW labels back to the 0-based memoized labels we use.
        case Array(label, value) => (label.toInt - 1, value.toDouble)
      }
    }).toIndexedSeq
  }
}

/**
 * Train a classifying model using VowpalWabbit. When applied to a set of
 * feature vectors, return a classifier object. You need to write out the
 * features first, then pass in the feature file name.
 */
class VowpalWabbitTrainer(
  val gaussian: Double,
  val lasso: Double,
  val vw_loss_function: String = "logistic",
  val vw_multiclass: String = "oaa",
  val vw_args: String = ""
) extends VowpalWabbitBase {
  def apply(num_classes: Int, feats_filename: String) = {
    val extra_args = Seq("--" + vw_multiclass, s"$num_classes")
    train_vw(feats_filename, gaussian, lasso, vw_loss_function, vw_args,
      extra_args)
  }
}

/**
 * Train a classifying model using VowpalWabbit. When applied to a set of
 * feature vectors, return a classifier object. You need to call
 * write_feature_file() first, then pass in the feature file name.
 */
class VowpalWabbitCostSensitiveTrainer(
  val gaussian: Double,
  val lasso: Double,
  val vw_loss_function: String = "logistic",
  val vw_multiclass: String = "oaa",
  val vw_args: String = ""
) extends VowpalWabbitBase {
  def apply(num_classes: Int, feats_filename: String) = {
    val multiclass_arg =
      if (vw_multiclass == "oaa") "csoaa" else "wap"
    val extra_args = Seq("--" + multiclass_arg, s"$num_classes")
    train_vw(feats_filename, gaussian, lasso, vw_loss_function, vw_args,
      extra_args)
  }
}
