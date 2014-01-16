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
         "tadm"),
       default = "perceptron",
       help = """Method to use for classification: 'perceptron'
(perceptron using the basic algorithm); 'avg-perceptron' (perceptron using
the basic algorithm, where the weights from the various iterations are averaged
-- this usually improves results if the weights oscillate around a certain
error rate, rather than steadily improving); 'pa-perceptron'
(passive-aggressive perceptron, which usually leads to steady but gradually
dropping-off error rate improvements with increased number of iterations);
'mlogit' (use a conditional logit model implemented by R's 'mlogit()'
function). Default %default.""")

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

  var label_specific =
    ap.flag("label-specific",
      help = """If true, features are label-specific, i.e. for a given
instance there are different features for each possible label. This
requires a different format for the training file.""")

  var input_format =
    ap.option[String]("input-format",
      choices = Seq("dense", "sparse"),
      default = "sparse",
      help = """If true, features are label-specific, i.e. for a given
instance there are different features for each possible label. This
requires a different format for the training file.""")

  var split_re =
    ap.option[String]("split-re", "sre", "sr",
      default = """[\s,]+""",
      help = """Regexp for splitting a line into column values. By default,
split on whitespace or commas.""")

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

    // If the output file is given via the option, create and write to that 
    // file; otherwise, use stdout.
    val output = {
      if (params.output == null)
        io.stdfh.openw("stdout")
      else
        io.localfh.openw(params.output)
    }

    val (classifier, test_instances, factory) =
      if (params.label_specific) {
        // Read in the data instances and create feature vectors
        val factory = new SparseAggregateInstanceFactory
        def read_data(source: Iterator[String], is_training: Boolean) = {
          if (params.input_format == "dense")
            factory.import_dense_labeled_instances(source,
              params.split_re,
              params.instance_index_column,
              params.label_column,
              params.choice_column,
              is_training = is_training)
          else
            factory.import_sparse_labeled_instances(source,
              params.split_re,
              is_binary = false,
              is_training = is_training)
        }
        val training_instances = read_data(trainSource, is_training = true)
        val test_instances = read_data(predictSource, is_training = false)
        val numlabs = factory.label_mapper.number_of_indices
        if (numlabs < 2) {
          error("Found %s different labels, when at least 2 are needed." format
            numlabs)
        }

        // Train a classifer
        val trainer = params.method match {
          case "mlogit" => {
            errprint("Using mlogit() conditional logit")
            new MLogitConditionalLogitTrainer[FeatureVector](ArrayVector)
          }
          case "tadm" => {
            errprint("Using TADM maxent ranker")
            new TADMRankingTrainer[FeatureVector](ArrayVector,
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
        val factory = new SparseSimpleInstanceFactory
        val training_instances =
          factory.import_labeled_instances(trainSource,
            params.split_re,
            params.label_column,
            is_training = true).
            toIndexedSeq
        val test_instances =
          factory.import_labeled_instances(predictSource,
            params.split_re,
            params.label_column,
            is_training = false).
            toIndexedSeq
        val numlabs = factory.label_mapper.number_of_indices
        if (numlabs < 2) {
          error("Found %s different labels, when at least 2 are needed." format
            numlabs)
        }

        // Train a classifer
        val trainer = params.method match {
          case "mlogit" =>
            error("Currently `mlogit` only works when --label-specific")
          case "tadm" =>
            error("Currently `tadm` only works when --label-specific")
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
        val classifier = trainer(TrainingData(training_instances))
        (classifier, test_instances, factory)
      }

    // Run classifier on each instance to get the predictions, and output
    // them in reverse sorted order, tab separated
    val pred_column_headings = Seq("Corr?", "Conf", "Truelab") ++
      (1 to factory.label_mapper.number_of_indices).toSeq.flatMap { num =>
        Seq(s"Pred$num", s"Score$num") }

    val results_insts_lines =
      for ((inst, truelab) <- test_instances) yield {
        // Scores in reverse sorted order
        val scores = classifier.sorted_scores(inst)
        val correct = scores(0)._1 == truelab
        val conf = scores(0)._2 - scores(1)._2
        // Map to labels
        val scorelabs = scores.flatMap {
          case (lab, pred) => Seq(
            factory.label_mapper.to_raw(lab), min_format_double(pred))
        }
        val corrstr = if (correct) "CORRECT" else "WRONG"
        val confstr = min_format_double(conf)
        val truelabstr = factory.label_mapper.to_raw(truelab)
        val line = Seq(corrstr, confstr, truelabstr) ++ scorelabs
        (correct, (inst, line))
      }

    val (results, insts_lines) = results_insts_lines.unzip
    val (_, lines) = insts_lines.unzip
    val fmt = table_column_format(pred_column_headings +: lines)

    output.println(fmt.format(pred_column_headings: _*))
    for (((inst, line), index) <- insts_lines.zipWithIndex) {
      if (debug("features"))
        output.println(inst.pretty_format(s"#${index + 1}"))
      output.println(fmt.format(line: _*))
    }
    val accuracy =
      results.map { if (_) 1 else 0 }.sum.toDouble / results.size
    output.println("Accuracy: %.2f%%" format (accuracy * 100))

    output.flush
    output.close
    0
  }
}
