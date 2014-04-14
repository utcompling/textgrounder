//  Classify.scala
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

import util.argparser._
import util.debug._
import util.experiment._
import util.io
import util.print._
import util.numeric.min_format_double
import util.table.table_column_format

import perceptron._
import mlogit._
import tadm._
import vowpalwabbit._


/**
 * General class retrieving command-line arguments or storing programmatic
 * configuration parameters for a Classify application.
 *
 * @param parser ArgParser for retrieving the value of command-line
 *   arguments from the command line.  Provided that the parser has been
 *   created and initialized by creating a previous instance of this same
 *   class with the same parser (a "shadow field" class), the variables
 *   below will be initialized with the values given by the user on the
 *   command line.  Otherwise, they will be initialized with the default
 *   values for the parameters.  Because they are vars, they can be freely
 *   set to other values.
 */
class ClassifyParameters(ap: ArgParser) {
  var method =
    ap.option[String]("method", "m",
       choices = Seq("perceptron", "avg-perceptron", "pa-perceptron", "mlogit",
         "tadm", "vw-batch", "vw-daemon"),
       default = "perceptron",
       help = """Method to use for classification: 'perceptron'
(perceptron using the basic algorithm); 'avg-perceptron' (perceptron using
the basic algorithm, where the weights from the various iterations are averaged
-- this usually improves results if the weights oscillate around a certain
error rate, rather than steadily improving); 'pa-perceptron'
(passive-aggressive perceptron, which usually leads to steady but gradually
dropping-off error rate improvements with increased number of iterations);
'mlogit' (use a conditional logit model implemented by R's 'mlogit()'
function); 'tadm' (use Rob Malouf's TADM package); 'vw-batch'
(use John Langford's Vowpal Wabbit package in batch mode); 'vw-daemon'
(use Vowpal Wabbit in daemon mode). Default %default.""")

  var trainSource =
    ap.option[String]("t", "train",
      metavar = "FILE",
      must = be_specified,
      help = """Labeled file for training model.""")

  var predictSource =
    ap.option[String]("p", "predict",
      metavar = "FILE",
      must = be_specified,
      help = """(Labeled) file to make predictions on.""")

  var ranker =
    ap.flag("ranker",
      help = """If true, this is a ranker rather than a classifier. This
controls how features are generated and requires that one of the
label-specific input formats be given.""")

  var input_format =
    ap.option[String]("input-format",
      choices = Seq("simple-dense", "simple-sparse",
        "mlogit-dense-label-specific", "tadm-sparse-label-specific"),
      default = "simple-sparse",
      help = """If true, features are label-specific, i.e. for a given
instance there are different features for each possible label. This
requires a different format for the training file.""")

  var split_re =
    ap.option[String]("split-re", "sre", "sr",
      default = """[\s]+""",
      help = """Regexp for splitting a line into column values. By default,
split on whitespace.""")

  var binary =
    ap.flag("binary",
      help = """If true, all features are binary and the value is omitted.
Only possible with sparse input formats.""")

  var label_column =
    ap.option[Int]("label-column", "lc",
      default = -1,
      help = """Column specifying the label of the instance. Numeric,
zero-based. If negative, count from the end. Default %default.""")

  var instance_index_column =
    ap.option[Int]("instance-index-column", "iic",
      default = 0,
      help = """Column specifying the index of the instance in question,
when doing label-specific classification. Numeric, zero-based. If negative,
count from the end. The indices themselves should be numeric.
Default %default.""")

  var choice_column =
    ap.option[Int]("choice-column", "lyc",
      default = 1,
      help = """Column specifying, for a given row in a label-specific
training set, whether the label on this line is the correct label for
this instance. There should be exactly one per set of lines. Numeric,
zero-based. If negative, count from the end. The values themselves should
be "yes", "no", "true" or "false", optionally upper-cased.
Default %default.""")

  var output =
    ap.option[String]("o", "out",
      metavar = "FILE",
      help = """File to make output predictions to.""")

  var lambda =
    ap.option[Double]("l", "lambda",
      metavar = "DOUBLE",
      must = be_>=(0.0),
      help = """For Naive Bayes: smoothing amount >= 0.0 (default: %default).""")

  var variant =
    ap.option[Int]("v", "variant",
      metavar = "INT",
      choices = Seq(0, 1, 2),
      help = """For passive-aggressive perceptron: variant (0, 1, 2 default %default).""")

  var error_threshold =
    ap.option[Double]("e", "error-threshold", "thresh",
      metavar = "DOUBLE",
      default = 1e-10,
      must = be_>(0.0),
      help = """For perceptron: Total error threshold below which training stops (default: %default).""")

  var lasso =
    ap.option[Double]("lasso",
      metavar = "DOUBLE",
      must = be_>=(0.0),
      help = """For TADM: Do Lasso (L1) penalization, with specified penalty; if 0,
use the default (default: %default).""")

  var gaussian =
    ap.option[Double]("gaussian",
      metavar = "DOUBLE",
      must = be_>=(0.0),
      help = """For TADM: Do Gaussian (L2) penalization, with specified penalty; if 0,
use the default (default: %default).""")

  var aggressiveness =
    ap.option[Double]("a", "aggressiveness",
      metavar = "DOUBLE",
      default = 1.0,
      must = be_>(0.0),
      help = """For perceptron: aggressiveness factor > 0.0 (default: %default).""")

  var decay =
    ap.option[Double]("decay",
      metavar = "DOUBLE",
      default = 0.0,
      must = be_>=(0.0),
      help = """For perceptron: decay of aggressiveness factor each round (default: %default).""")

  var iterations =
    ap.option[Int]("i", "iterations",
      default = 10000,
      must = be_>(0),
      help = """For perceptron: maximum number of training iterations (default: %default).""")

  var debug =
    ap.option[String]("d", "debug", metavar = "FLAGS",
      help = """Output debug info of the given types.  Multiple debug
parameters can be specified, indicating different types of info to output.
Separate parameters by spaces, colons or semicolons.  Params can be boolean,
if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
list-valued; multiple values are specified by including the parameter
multiple times, or by separating values by a comma.
""")

  if (ap.parsedValues && debug != null)
    parse_debug_spec(debug)
}

/**
 * An application that reads in a standard format set of CSVs representing
 * instances, trains a Naive Bayes model, and then outputs the predictions
 * on a second set of instances (ignoring the labels).
 */
object Classify extends ExperimentApp("Classify") {

  type TParam = ClassifyParameters

  def create_param_object(ap: ArgParser) = new ClassifyParameters(ap)

  def error(msg: String) = {
    System.err.println(msg)
    System.exit(1)
    ???
  }

  def run_program(args: Array[String]) = {
    val trainSource = io.localfh.openr(params.trainSource)
    val predictSource = io.localfh.openr(params.predictSource)
    val label_specific = Seq("tadm-sparse-label-specific",
      "mlogit-dense-label-specific").contains(params.input_format)
    val sparse = Seq("simple-sparse", "tadm-sparse-label-specific"
      ).contains(params.input_format)

    // If the output file is given via the option, create and write to that
    // file; otherwise, use stdout.
    val output = {
      if (params.output == null)
        io.stdfh.openw("stdout")
      else
        io.localfh.openw(params.output)
    }

    if (params.ranker) {
      require(label_specific,
        "Need one of the label-specific input formats if '--ranker' is given")
    }
    if (params.binary) {
      require(sparse,
        "Need a sparse input format if '--binary' is given")
    }
    val attach_label = params.method == "tadm" && !params.ranker

    /**
     * Create the appropriate data instance factory for non-label-dependent
     * training data.
     */
    def create_simple_data_instance_factory() = {
      // Read in the data instances and create feature vectors
      params.input_format match {
        case "simple-dense" =>
          new SimpleDenseDataInstanceFactory(attach_label,
            params.split_re, params.label_column)
        case "simple-sparse" =>
          new SimpleSparseDataInstanceFactory(attach_label,
            params.split_re, is_binary = params.binary)
      }
    }

    /**
     * Convert the sorted scores, along with the true label, into a set
     * of column values for a single line that will later get formatted as
     * part of a table.
     */
    def get_table_line(mapper: FeatureLabelMapper,
        scores: Iterable[(LabelIndex, Double)],
        truelab: LabelIndex) = {
      val correct = scores.head._1 == truelab
      val conf = scores.head._2 - scores.tail.head._2
      // Map to labels
      val scorelabs = scores.flatMap {
        case (lab, pred) => Seq(
          mapper.label_to_string(lab), min_format_double(pred))
      }
      val corrstr = if (correct) "CORRECT" else "WRONG"
      val confstr = min_format_double(conf)
      val truelabstr = mapper.label_to_string(truelab)
      val line = Seq(corrstr, confstr, truelabstr) ++ scorelabs
      (correct, line)
    }

    /**
     * Output the classification results as a table to the correct
     * output file.
     *
     * @param num_labels Number of labels in classification problem.
     * @param insts_results_lines Iterable over line descriptions, each
     *    of which is a tuple of the original instance, whether the
     *    prediction was correct or not, and a sequence of column values.
     * @param fmt_inst Format an instance and a prefix string into a
     *    human-readable string. Used when --debug=features.
     */
    def output_results[Inst](num_labels: Int,
      insts_results_lines: Iterable[(Inst, (Boolean, Seq[String]))]
    )(fmt_inst: (Inst, String) => String) {
      // Run classifier on each instance to get the predictions, and output
      // them in reverse sorted order, tab separated
      val pred_column_headings = Seq("Corr?", "Conf", "Truelab") ++
        (1 to num_labels).toSeq.flatMap { num => Seq(s"Pred$num", s"Score$num") }

      val (insts, results_lines) = insts_results_lines.unzip
      val (results, lines) = results_lines.unzip
      val fmt = table_column_format(pred_column_headings +: lines.toSeq)

      output.println(fmt.format(pred_column_headings: _*))
      for (((inst, line), index) <- (insts zip lines).zipWithIndex) {
        if (debug("features"))
          output.println(fmt_inst(inst, s"#${index + 1}"))
        output.println(fmt.format(line: _*))
      }
      val accuracy =
        results.map { if (_) 1 else 0 }.sum.toDouble / results.size
      output.println("Accuracy: %.2f%%" format (accuracy * 100))
    }

    /** Handle "indiv classifiers", which allow individual test instances
     * to be efficiently evaluated one at a time.
     */
    def handle_indiv_classifier() = {
      val (classifier, test_instances, factory) =
        if (label_specific) {
          // Read in the data instances and create feature vectors
          val factory = params.input_format match {
            case "mlogit-dense-label-specific" =>
              new MLogitDenseLabelSpecificDataInstanceFactory(attach_label,
                params.split_re, params.instance_index_column,
                params.label_column, params.choice_column)
            case "tadm-sparse-label-specific" =>
              new TADMSparseLabelSpecificDataInstanceFactory(attach_label,
                params.split_re, is_binary = params.binary)
          }
          val training_instances =
            factory.import_instances(trainSource, is_training = true)
          val test_instances =
            factory.import_instances(predictSource, is_training = false)
          val numlabs = factory.mapper.number_of_labels
          if (numlabs < 2) {
            error(
              s"Found $numlabs different labels, when at least 2 are needed.")
          }

          // Train a classifer
          val trainer = params.method match {
            case "mlogit" => {
              errprint("Using mlogit() conditional logit")
              new MLogitConditionalLogitTrainer[FeatureVector](ArrayVector)
            }
            case "tadm" => {
              errprint("Using TADM trainer")
              new TADMTrainer[FeatureVector](ArrayVector,
                max_iterations = params.iterations,
                gaussian = params.gaussian,
                lasso = params.lasso)
            }
            case "pa-perceptron" => {
              errprint("Using passive-aggressive multi-label perceptron")
              new PassiveAggressiveNoCostSingleWeightMultiLabelPerceptronTrainer[
                FeatureVector
              ](ArrayVector, params.variant,
                params.aggressiveness,
                decay = params.decay,
                error_threshold = params.error_threshold,
                max_iterations = params.iterations)
            }
            case "perceptron"|"avg-perceptron" => {
              errprint("Using basic multi-label perceptron")
              new BasicSingleWeightMultiLabelPerceptronTrainer[
                FeatureVector
              ](ArrayVector, params.aggressiveness,
                decay = params.decay,
                averaged = params.method == "avg-perceptron",
                error_threshold = params.error_threshold,
                max_iterations = params.iterations)
            }
          }
          val classifier = trainer(TrainingData(training_instances))
          (classifier, test_instances, factory)
        } else {
          // Read in the data instances and create feature vectors
          val factory = create_simple_data_instance_factory
          val training_instances =
            factory.import_instances(trainSource, is_training = true)
          val test_instances =
            factory.import_instances(predictSource, is_training = false)
          val numlabs = factory.mapper.number_of_labels
          if (numlabs < 2) {
            error("Found %s different labels, when at least 2 are needed." format
              numlabs)
          }

          // Train a classifier
          val trainer = params.method match {
            case "mlogit" =>
              error("Currently `mlogit` only works when --label-specific")
            case "tadm" => {
              errprint("Using TADM trainer")
              new TADMTrainer[FeatureVector](ArrayVector,
                max_iterations = params.iterations,
                gaussian = params.gaussian,
                lasso = params.lasso)
            }
            case "pa-perceptron" if numlabs > 2 || debug("multilabel") => {
              errprint("Using passive-aggressive multi-label perceptron")
              new PassiveAggressiveNoCostMultiWeightMultiLabelPerceptronTrainer[
                FeatureVector
              ](ArrayVector, numlabs, params.variant,
                params.aggressiveness,
                decay = params.decay,
                error_threshold = params.error_threshold,
                max_iterations = params.iterations)
            }
            case "pa-perceptron" => {
              errprint("Using passive-aggressive binary perceptron")
              new PassiveAggressiveBinaryPerceptronTrainer(
                ArrayVector, params.variant, params.aggressiveness,
                decay = params.decay,
                error_threshold = params.error_threshold,
                max_iterations = params.iterations)
            }
            case "perceptron"|"avg-perceptron"
                if numlabs > 2 || debug("multilabel") => {
              errprint("Using basic multi-label perceptron")
              new BasicMultiWeightMultiLabelPerceptronTrainer[
                FeatureVector
              ](ArrayVector, numlabs, params.aggressiveness,
                decay = params.decay,
                averaged = params.method == "avg-perceptron",
                error_threshold = params.error_threshold,
                max_iterations = params.iterations)
            }
            case "perceptron"|"avg-perceptron" => {
              errprint("Using basic binary perceptron")
              new BasicBinaryPerceptronTrainer(
                ArrayVector, params.aggressiveness,
                decay = params.decay,
                averaged = params.method == "avg-perceptron",
                error_threshold = params.error_threshold,
                max_iterations = params.iterations)
            }
          }
          val classifier = trainer(TrainingData(training_instances,
            remove_non_choice_specific_columns = false))
          (classifier, test_instances, factory)
        }

      val insts_results_lines =
        for ((inst, truelab) <- test_instances) yield {
          // Scores in reverse sorted order
          val scores = classifier.sorted_scores(inst)
          (inst, get_table_line(factory.mapper, scores, truelab))
        }

      output_results(factory.mapper.number_of_labels, insts_results_lines) {
        (inst, prefix) => inst.pretty_format(prefix)
      }
    }

    /** Handle Vowpal Wabbit as either a "batch classifier", which requires that
     * batches of test instances be evaluated at a time for efficiency purposes,
     * or a "daemon classifier", which is able to evaluate test instances one
     * at a time. Basically, in the indiv classifiers above, the weights are
     * directly available so that we can do the classification ourselves, but
     * in the case of VW we need to run a program to do the classification. VW
     * can be run either in batch mode or daemon mode. In the latter case, it
     * stays resident in memory and communication with it is by means of a
     * network socket.
     *
     * Vowpal Wabbit features hashing and reduces multiclass classification
     * to a set of binary classifications (using either the "one against all"
     * or "error-correcting tournament" strategy), and for efficient
     * one-at-a-time evaluation we would need to use the --daemon IPC interface
     * onto the running VW executable.
     */
    def handle_vw_classifier(daemon: Boolean) = {
      val (training_instances, test_instances, factory, numlabs) =
        if (label_specific) {
          error("Can't handle label-specific input yet with Vowpal Wabbit")
        } else {
          // Read in the data instances and create feature vectors
          val factory = create_simple_data_instance_factory
          // By calling toIndexedSeq here we force all values to be
          // computed, which will correctly set the number of labels in
          // the mapper.
          val training_instances =
            factory.import_direct_instances(trainSource).toIndexedSeq
          val test_instances =
            factory.import_direct_instances(predictSource).toIndexedSeq
          val numlabs = factory.mapper.number_of_labels
          if (numlabs < 2) {
            error(
              s"Found $numlabs different labels, when at least 2 are needed.")
          }
          (training_instances, test_instances, factory, numlabs)
        }

      val insts_results_lines =
        if (daemon) {
          // Train a classifier
          errprint("Using Vowpal Wabbit daemon trainer")
          val trainer = new VowpalWabbitDaemonTrainer(
            gaussian = params.gaussian,
            lasso = params.lasso)

          val train_feats_filename =
            trainer.write_feature_file(training_instances.toIterator)
          val classifier = trainer(train_feats_filename, "",
            Seq("--oaa", s"$numlabs"))

          for ((inst, truelab) <- test_instances) yield {
            val extline = classifier.externalize_data_instance(inst, truelab)
            // Scores in reverse sorted order
            val scores = classifier(extline).head.sortWith(_._2 > _._2)
            (inst, get_table_line(factory.mapper, scores, truelab))
          }
        } else {
          // Train a classifier
          errprint("Using Vowpal Wabbit batch trainer")
          val trainer = new VowpalWabbitBatchTrainer(
            gaussian = params.gaussian,
            lasso = params.lasso)

          val train_feats_filename =
            trainer.write_feature_file(training_instances.toIterator)
          val classifier = trainer(train_feats_filename, "",
            Seq("--oaa", s"$numlabs"))

          // Return insts_results_lines
          val test_feats_filename =
            classifier.write_feature_file(test_instances.toIterator)
          val results = classifier(test_feats_filename)
          for ((result, (inst, truelab)) <-
               results zip test_instances) yield {
            val scores = result.sortWith(_._2 > _._2)
            (inst, get_table_line(factory.mapper, scores, truelab))
          }
        }
      output_results(factory.mapper.number_of_labels, insts_results_lines) {
        (inst, prefix) =>
          inst.map { case (ty, feat, value) =>
            s"  $prefix: $feat ($ty) = $value"
          }.mkString("\n")
      }
    }

    if (params.method == "vw-batch") handle_vw_classifier(daemon = false)
    else if (params.method == "vw-daemon") handle_vw_classifier(daemon = true)
    else handle_indiv_classifier()

    output.flush
    output.close
    0
  }
}
