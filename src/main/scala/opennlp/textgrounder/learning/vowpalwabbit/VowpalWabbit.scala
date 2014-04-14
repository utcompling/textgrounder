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
import util.error._
import util.io.localfh
import util.metering._
import util.print.errprint
import util.verbose._

import java.io.{File, PrintStream}

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
 * -csoaa_ldf or -wap_ldf, which takes an argument that should be one of
 * "multiline", "multiline-classifier", "singleline", or
 * "singleline-classifier" (or equivalently "m", "mc", "s", "sc"); the
 * classifier variants use the same input format as the non-classifier
 * formats but are implemented using a binary classifier instead of by
 * regression. The multiline format is what's described below.
 * See this post from Hal Daume:
 *
 * https://groups.yahoo.com/neo/groups/vowpal_wabbit/conversations/topics/626
 *
 * Hal gives the following cost-sensitive example:
 *
 * 1:1.0 2:0.0 3:2.0 | a b c
 * 1:1.0 2:0.0 | b c d
 * 1:1.0 3:2.0 | a b c
 *
 * and the equivalent in label-dependent format (fixed up from the email):
 *
 * ============== cut =============
 * 1:1.0 | a_1 b_1 c_1
 * 2:0.0 | a_2 b_2 c_2
 * 3:2.0 | a_3 b_3 c_3
 *
 * 1:1.0 | b_1 c_1 d_1
 * 2:0.0 | b_2 c_2 d_2
 *
 * 1:1.0 | a_1 b_1 c_1
 * 3:2.0 | a_3 b_3 c_3
 *
 * ============== cut =============
 *
 * The format for a data instance for testing is the same as for training
 * except that the correct label or cost may be omitted (it should be ignored
 * if given).
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
 * Bad model was created (bug in Vowpal Wabbit, probably).
 */
case class VowpalWabbitModelError(
  message: String,
  cause: Option[Throwable] = None
) extends RethrowableRuntimeException(message, cause)

/**
 * Common code to the various Vowpal Wabbit trainers and classifiers.
 */
protected trait VowpalWabbitBase {
  def train_vw_model(feats_filename: String, gaussian: Double, lasso: Double,
      vw_loss_function: String, vw_args: String, extra_args: Seq[String],
      verbose: MsgVerbosity = MsgNormal) = {
    // We make the writing happen in a different step because the number of
    // classes might not be known until we do so.
    //
    // We need two temporary files in addition to the feature file:
    // one used by VW as a cache, and one where the model is written to.
    // The last one needs to be preserved for evaluation.
    val cache_filename =
      File.createTempFile("textgrounder.vw.cache.train", null).toString
    val model_filename =
      File.createTempFile("textgrounder.vw.model", null).toString
    if (verbose != MsgQuiet) {
      errprint(s"Writing VW training cache to $cache_filename")
      errprint(s"Writing VW model to $model_filename")
    }
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
        Seq("--loss_function", vw_loss_function) ++
        (if (gaussian > 0) Seq("--l2", s"$gaussian") else Seq()) ++
        (if (lasso > 0) Seq("--l1", s"$lasso") else Seq()) ++
        // Split on an empty string wrongly returns Array("")
        (if (vw_args == "") Seq() else vw_args.split("""\s+""").toSeq)
    errprint("Executing: %s", vw_cmd_line mkString " ")
    time_action("running VowpalWabbit", verbose) {
      vw_cmd_line !
    }
    if (!debug("preserve-tmp-files")) {
      (new File(cache_filename)).delete
      (new File(feats_filename)).delete
      (new File(model_filename)).deleteOnExit
    }
    val model_length = (new File(model_filename)).length
    if (model_length > 0 && model_length < 256)
        errprint(s"Model filename $model_filename has length $model_length < 256, probably bad")
    if (model_length == 0) {
        val badmess = s"Model filename $model_filename has length 0, definitely bad"
      if (debug("warn-on-bad-model"))
        errprint(badmess)
      else
        throw new VowpalWabbitModelError(badmess)
    }

    model_filename
  }

  def sanitize_feature(feat: String) = {
    feat.replace(":", "_").replace("|", "_").replace(" ", "_").
      replace("\r", "_").replace("\n", "_").replace("\t", "_")
  }

  /**
   * Convert a single data instance into external format.
   */
  def externalize_data_instance(
    feats: Iterable[(FeatureValue, String, Double)],
    correct: LabelIndex
  ) = {
    // Map 0-based memoized labels to 1-based labels for VW, which
    // seems to want labels in this format.
    s"${correct + 1} | " +
      feats.map { case (_, feat, value) =>
        val goodfeat = sanitize_feature(feat)
        s"$goodfeat:$value"
      }.mkString(" ")
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
  def gen_write_feature_file(verbose: MsgVerbosity = MsgNormal)(
      process_features: (PrintStream, Meter) => Unit
  ) = {
    var feats_filename =
      File.createTempFile("textgrounder.vw.feats.test", null).toString
    if (verbose != MsgQuiet)
      errprint(s"Writing features to $feats_filename")

    val task = new Meter("processing features in", "document", verbose)

    val f = localfh.openw(feats_filename)
    process_features(f, task)
    f.close()
    feats_filename
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
    features: Iterator[(Iterable[(FeatureValue, String, Double)], LabelIndex)],
    verbose: MsgVerbosity = MsgNormal
  ) = {
    gen_write_feature_file(verbose) { (f, task) =>
      features.foreachMetered(task) { case (feats, correct) =>
        if (debug("features")) {
          val prefix = "#%s" format (task.num_processed + 1)
          errprint(s"$prefix: Correct = $correct")
          feats.foreach { case (_, feat, value) =>
            errprint(s"  $prefix: $feat = $value")
          }
        }
        f.println(externalize_data_instance(feats, correct))
      }
    }
  }

  /**
   * Convert a single cost-sensitive data instance into external format.
   *
   * @param feats Iterable over features. Each feature is described by
   *   a feature type (ignored), a feature name and a value.
   * @param costs Costs associated with distinct labels.
   */
  def externalize_cost_sensitive_data_instance(
    feats: Iterable[(FeatureValue, String, Double)],
    costs: Iterable[(LabelIndex, Double)]
  ) = {
    // Map 0-based memoized labels to 1-based labels for VW, which
    // seems to want labels in this format.
    costs.map { case (label, cost) =>
      s"${label + 1}:$cost"
    }.mkString(" ") +
    " | " +
    feats.map { case (_, feat, value) =>
      val goodfeat = sanitize_feature(feat)
      s"$goodfeat:$value"
    }.mkString(" ")
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
      Iterable[(LabelIndex, Double)])],
    verbose: MsgVerbosity = MsgNormal
  ) = {
    gen_write_feature_file(verbose) { (f, task) =>
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
        f.println(externalize_cost_sensitive_data_instance(feats, costs))
      }
    }
  }

  /**
   * Convert a single label-dependent data instance
   * into external format. Meant to be used in a println statement.
   *
   * @param label_info Iterable over per-label info, consisting of
   *   an iterable over per-feature info. Each feature is
   *   described by a feature type (ignored), a feature name and a value.
   * @param correct Correct label
   */
  def externalize_label_dependent_data_instance(
    label_info: Iterable[Iterable[(FeatureValue, String, Double)]],
    correct: LabelIndex
  ) = {
    externalize_cost_sensitive_label_dependent_data_instance(
      label_info.zipWithIndex.map { case (feats, index) =>
        (if (index == correct) 0.0 else 1.0, feats)
      }
    )
  }

  /**
   * Write out a set of label-dependent data instances to
   * a file, where each label is described by features, and there is a
   * single correct label.
   *
   * @param features An iterator over data instances. Each instance is
   *   an iterable of per-label info, consisting of the features of the
   *   label, an iterable over per-feature info. Each feature is described
   *   by a feature type (ignored), a feature name and a value.
   * @param file File to write features to.
   */
  def write_label_dependent_feature_file(
    features: Iterator[(Iterable[Iterable[(FeatureValue, String, Double)]], LabelIndex)],
    verbose: MsgVerbosity = MsgNormal
  ) = {
    gen_write_feature_file(verbose) { (f, task) =>
      features.foreachMetered(task) { case (label_info, correct) =>
        if (debug("features")) {
          val outer_prefix = "#%s" format (task.num_processed + 1)
          errprint("$outer_prefix: ${label_info.size} labels, correct: ${correct + 1 }")
          label_info.zipWithIndex.map { case (feats, index) =>
            val label = index + 1
            val prefix = "$outer_prefix.$label"
            errprint(s"  $prefix: ${feats.size} features")
            feats.foreach { case (_, feat, value) =>
              errprint(s"    $prefix: $feat = $value")
            }
          }
        }
        f.println(externalize_label_dependent_data_instance(
          label_info, correct))
      }
    }
  }

  /**
   * Convert a single cost-sensitive label-dependent data instance
   * into external format. Meant to be used in a println statement.
   *
   * @param label_info Iterable over per-label info, consisting of a tuple
   *   of the cost associated with the label and the features of the
   *   label, an iterable over per-feature info. Each feature is
   *   described by a feature type (ignored), a feature name and a value.
   */
  def externalize_cost_sensitive_label_dependent_data_instance(
    label_info: Iterable[(Double, Iterable[(FeatureValue, String, Double)])]
  ) = {
    // Remember that VW wants 1-based labels.
    label_info.zipWithIndex.map { case ((cost, feats), index) =>
      s"${index + 1}:$cost | " +
        feats.map { case (_, feat, value) =>
          val goodfeat = sanitize_feature(feat)
          s"$goodfeat:$value"
        }.mkString(" ")
    }.mkString("\n") + "\n"
  }

  /**
   * Write out a set of cost-sensitive label-dependent data instances to
   * a file, where each label is described by features and a cost.
   *
   * @param features An iterator over data instances. Each instance is
   *   an iterable of per-label info, consisting of a tuple of the cost
   *   associated with the label and the features of the label, an iterable
   *   over per-feature info. Each feature is described by a feature type
   *   (ignored), a feature name and a value.
   * @param file File to write features to.
   */
  def write_cost_sensitive_label_dependent_feature_file(
    features: Iterator[Iterable[(Double, Iterable[(FeatureValue, String, Double)])]],
    verbose: MsgVerbosity = MsgNormal
  ) = {
    gen_write_feature_file(verbose) { (f, task) =>
      features.foreachMetered(task) { label_info =>
        if (debug("features")) {
          val outer_prefix = "#%s" format (task.num_processed + 1)
          errprint("$outer_prefix: ${label_info.size} labels")
          label_info.zipWithIndex.map { case ((cost, feats), index) =>
            val label = index + 1
            val prefix = "$outer_prefix.$label"
            errprint(s"  $prefix: cost $cost, ${feats.size} features")
            feats.foreach { case (_, feat, value) =>
              errprint(s"    $prefix: $feat = $value")
            }
          }
        }
        f.println(externalize_cost_sensitive_label_dependent_data_instance(
          label_info))
      }
    }
  }
}

/**
 * Classify data instances using VowpalWabbit. When applied to a set of
 * feature vectors, return a set of vectors holding scores, which should
 * be convertible to probabilities by exponentiating and normalizing.
 * As when training, you need to write out the features first, then pass
 * in the feature file name.
 */
abstract class VowpalWabbitClassifier(
  val model_filename: String
) extends VowpalWabbitBase {
}

/**
 * Classify data instances using VowpalWabbit. When applied to a set of
 * feature vectors, return a set of vectors holding scores, which should
 * be convertible to probabilities by exponentiating and normalizing.
 * As when training, you need to write out the features first, then pass
 * in the feature file name.
 */
class VowpalWabbitBatchClassifier private[vowpalwabbit] (
  model_filename: String
) extends VowpalWabbitClassifier(model_filename) {
  /**
   * Apply a Vowpal Wabbit multi-class batch classifier.
   *
   * @param feats_filename Filename containing sparse feature vectors
   *   describing the test instances. This should be created using
   *   `write_feature_file` or `write_cost_sensitive_feature_file`.
   *   NOTE: This filename will be deleted after the classifier has been
   *   run, on the assumption that it is a temporary file, unless the
   *   debug flag 'preserve-tmp-files' is set. There is currently no
   *   programmatic way to specify that a given feature file should be
   *   preserved.
   * @return An Iterable of arrays (with one array per test instance) of
   *   pairs of class labels and scores. The class labels are integers
   *   starting at 0. Whether the scores can be transformed into
   *   probabilities depends on the parameters set during training.
   *   For example, with logistic loss, the scores can be transformed to
   *   probabilities using the logistic function followed by normalization.
   */
  def apply(feats_filename: String, verbose: MsgVerbosity = MsgNormal
      ): Iterable[Array[(LabelIndex, Double)]] = {
    // We need two temporary files: one used by VW as a cache, and one
    // where VW writes the raw predictions. In addition we need the file
    // holding the features and the file holding the written-out model.
    val cache_filename =
      File.createTempFile("textgrounder.vw.cache.test", null).toString
    val pred_filename =
      File.createTempFile("textgrounder.vw.pred", null).toString
    val vw_cmd_line =
      Seq("vw", "-k", "--cache_file", cache_filename, "--data", feats_filename,
          "-i", model_filename, "--raw_predictions", pred_filename,
          "--compressed", "-t") ++ (
        if (verbose == MsgVerbose) Seq() else Seq("--quiet")
      )
    if (verbose != MsgQuiet) {
      errprint(s"Writing VW eval cache to $cache_filename")
      errprint(s"Writing VW raw predictions to $pred_filename")
      errprint("Executing: %s", vw_cmd_line mkString " ")
    }
    time_action("running VowpalWabbit", verbose) {
      vw_cmd_line !
    }
    val results = (for (line <- localfh.openr(pred_filename)) yield {
      val preds_1 = line.split("""\s+""")
      preds_1.map(_.split(":")).map {
        // Map 1-based VW labels back to the 0-based memoized labels we use.
        case Array(label, value) => (label.toInt - 1, value.toDouble)
      }
    }).toIndexedSeq
    if (!debug("preserve-tmp-files")) {
      (new File(cache_filename)).delete
      (new File(pred_filename)).delete
      (new File(feats_filename)).delete
    }
    results
  }
}

class VowpalWabbitDaemonClassifier private[vowpalwabbit] (model_filename: String,
  raw_filename: String, pid: Int, port: Int
) extends VowpalWabbitClassifier(model_filename) {
  var raw_file_seek_point = 0L
  /**
   * Apply a Vowpal Wabbit multi-class daemon classifier.
   *
   * @param input String describing the test instance(s). The format of
   *   the string will vary depending on whether the classifier is
   *   cost-sensitive or not and label-dependent or not.  It should
   *   have been created with one of the `externalize*_data_instance`
   *   functions.
   * @param label_dependent True if the classifier is label-dependent
   *   (required because the output is different in this case)
   * @return An Iterable of arrays (with one array per test instance) of
   *   pairs of class labels and scores. The class labels are integers
   *   starting at 0. Whether the scores can be transformed into
   *   probabilities depends on the parameters set during training.
   *   For example, with logistic loss, the scores can be transformed to
   *   probabilities using the logistic function followed by normalization.
   *   (FIXME: Or simply by exponentiating and normalizing?)
   */
  def apply(input: String, label_dependent: Boolean = false,
      verbose: MsgVerbosity = MsgNormal
  ): Iterable[Array[(LabelIndex, Double)]] = {
    import java.net.Socket
    import java.io._
    val socket = new Socket("localhost", port)
    val outstream = socket.getOutputStream
    val out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outstream)))
    val instream = new InputStreamReader(socket.getInputStream)
    val in = new BufferedReader(instream)
    if (debug("vw-daemon"))
      errprint("Outputting to socket: [%s]", input)
    out.println(input)
    out.flush()
    val linein = in.readLine
    if (debug("vw-daemon"))
      errprint("Read from socket: [%s]", linein)
    // Thread.sleep(1000)
    val rawf = new RandomAccessFile(raw_filename, "r")
    rawf.seek(raw_file_seek_point)
    val bytes = new Array[Byte]((rawf.length - rawf.getFilePointer).toInt)
    rawf.read(bytes)
    raw_file_seek_point = rawf.length
    val instr = new String(bytes)
    if (debug("vw-daemon"))
      errprint("Read from raw file: [%s]", instr)
    // In the label-dependent case, we have one score per line, with
    // a blank line separating sets of scores. Otherwise, we have all the
    // scores on the same line, separated by blanks. This corresponds to
    // the input, which is multi-line in the label-dependent case, single-line
    // otherwise.
    val lines = instr.split(if (label_dependent) "\n\n" else "\n")
    val results = (for (line <- lines) yield {
      val preds_1 = line.split("""\s+""")
      preds_1.map(_.split(":")).map {
        // Map 1-based VW labels back to the 0-based memoized labels we use.
        case Array(label, value) => (label.toInt - 1, value.toDouble)
      }
    }).toIndexedSeq
    socket.close
    results
  }
}

/**
 * Train a batch classifying model using VowpalWabbit. When applied to a set of
 * feature vectors, return a classifier object. You need to write out the
 * features first, then pass in the feature file name.
 */
class VowpalWabbitBatchTrainer(
  val gaussian: Double,
  val lasso: Double,
  val vw_loss_function: String = "logistic"
) extends VowpalWabbitBase {
  def train_vw_batch(feats_filename: String, gaussian: Double, lasso: Double,
      vw_loss_function: String, vw_args: String, extra_args: Seq[String],
      verbose: MsgVerbosity = MsgNormal) = {
    val model_filename = train_vw_model(feats_filename, gaussian, lasso,
      vw_loss_function, vw_args, extra_args, verbose)
    new VowpalWabbitBatchClassifier(model_filename)
  }

  /**
   * Train a Vowpal Wabbit multi-class batch classifier.
   *
   * @param feats_filename Filename containing sparse feature vectors
   *   describing the training data. This should be created using
   *   `write_feature_file` or `write_cost_sensitive_feature_file` (for
   *   the cost-sensitive version). NOTE: This filename will be deleted after
   *   the classifier has been trained, on the assumption that it is a
   *   temporary file, unless the debug flag 'preserve-tmp-files' is set.
   *   There is currently no programmatic way to specify that a given
   *   feature file should be preserved.
   * @return A `VowpalWabbitBatchClassifier` object, used to classify test
   *   instances.
   */
  def apply(feats_filename: String, vw_args: String,
      extra_args: Seq[String]) = {
    train_vw_batch(feats_filename, gaussian, lasso, vw_loss_function, vw_args,
      extra_args)
  }
}

/**
 * Train a daemon classifying model using VowpalWabbit. When applied to a set
 * of feature vectors, return a classifier object. You need to write out the
 * features first, then pass in the feature file name.
 */
class VowpalWabbitDaemonTrainer(
  val gaussian: Double,
  val lasso: Double,
  val vw_loss_function: String = "logistic"
) extends VowpalWabbitBase {
  def train_vw_daemon(feats_filename: String, gaussian: Double, lasso: Double,
      vw_loss_function: String, vw_args: String, extra_args: Seq[String],
      verbose: MsgVerbosity = MsgNormal) = {
    val model_filename = train_vw_model(feats_filename, gaussian, lasso,
      vw_loss_function, vw_args, extra_args, verbose)

    // We need four temporary files: one used by VW as a cache, one
    // where VW writes the raw predictions, and two to (very temporarily)
    // hold the process ID and port used for communication. In addition we
    // need the file holding the written-out model.
    val cache_filename =
      File.createTempFile("textgrounder.vw.cache.test", null).toString
    val pred_filename =
      File.createTempFile("textgrounder.vw.pred", null).toString
    val pid_file_filename =
      File.createTempFile("textgrounder.vw.pid-file", null).toString
    val port_file_filename =
      File.createTempFile("textgrounder.vw.port-file", null).toString
    val vw_cmd_line =
      Seq("vw", "-k", "--cache_file", cache_filename,
          "-i", model_filename, "--raw_predictions", pred_filename,
          "-t", "--daemon", "--pid_file", pid_file_filename,
          "--port", "0", "--port_file", port_file_filename) ++ (
        if (verbose == MsgVerbose) Seq() else Seq() // Seq("--quiet")
      )
    if (verbose != MsgQuiet) {
      errprint(s"Writing VW eval cache to $cache_filename")
      errprint(s"Writing VW raw predictions to $pred_filename")
      errprint("Executing: %s", vw_cmd_line mkString " ")
    }
    val cmdline = vw_cmd_line mkString " "
    // We need to redirect the output and stderr to /dev/null or the
    // process hangs instead of completing. FIXME: Not sure why.
    val redir_cmdline = s"$cmdline > /dev/null 2>&1"
    val sh_cmdline = Seq("/bin/sh", "-c", redir_cmdline)
    //val proc = new java.lang.ProcessBuilder(vw_cmd_line: _*).start()
    (sh_cmdline !)
    // Thread.sleep(1000)
    val pid_lines = localfh.openr(pid_file_filename)
    val pid = pid_lines.next.toInt
    pid_lines.close()
    val port_lines = localfh.openr(port_file_filename)
    val port = port_lines.next.toInt
    port_lines.close()
    if (debug("vw-daemon"))
      errprint("PID is %s, port is %s", pid, port)
    if (!debug("preserve-tmp-files")) {
      (new File(cache_filename)).deleteOnExit
      (new File(pred_filename)).deleteOnExit
      (new File(pid_file_filename)).delete
      (new File(port_file_filename)).delete
    }
    if (!debug("preserve-vw-daemon")) {
      scala.sys.addShutdownHook {
        s"kill $pid" !
      }
    }
    new VowpalWabbitDaemonClassifier(model_filename, pred_filename, pid, port)
  }

  /**
   * Train a Vowpal Wabbit multi-class daemon classifier.
   *
   * @param feats_filename Filename containing sparse feature vectors
   *   describing the training data. This should be created using
   *   `write_feature_file` or `write_cost_sensitive_feature_file` (for
   *   the cost-sensitive version). NOTE: This filename will be deleted after
   *   the classifier has been trained, on the assumption that it is a
   *   temporary file, unless the debug flag 'preserve-tmp-files' is set.
   *   There is currently no programmatic way to specify that a given
   *   feature file should be preserved.
   * @return A `VowpalWabbitDaemonClassifier` object, used to classify test
   *   instances.
   */
  def apply(feats_filename: String, vw_args: String,
      extra_args: Seq[String]) = {
    train_vw_daemon(feats_filename, gaussian, lasso, vw_loss_function, vw_args,
      extra_args)
  }
}
