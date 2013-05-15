//  ClassifyApp.scala
//
//  Copyright (C) 2012 Ben Wing, The University of Texas at Austin
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
import util.experiment._
import util.io
import util.print._

import perceptron._

import util.debug._

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
       choices = Seq("perceptron", "avg-perceptron", "pa-perceptron"),
       default = "perceptron",
       help = """Method to use for classification: 'perceptron'
(perceptron using the basic algorithm); 'avg-perceptron' (perceptron using
the basic algorithm, where the weights from the various rounds are averaged
-- this usually improves results if the weights oscillate around a certain
error rate, rather than steadily improving); 'pa-perceptron'
(passive-aggressive perceptron, which usually leads to steady but gradually
dropping-off error rate improvements with increased number of rounds).
Default %default.""")

  var trainSource =
    ap.option[String]("t", "train",
      metavar = "FILE",
      help = """Labeled file for training model.""")

  var predictSource =
    ap.option[String]("p", "predict",
      metavar = "FILE",
      help = """(Labeled) file to make predictions on.""")

  var output =
    ap.option[String]("o", "out",
      metavar = "FILE",
      help = """File to make output predictions to.""")

  var lambda =
    ap.option[Double]("l", "lambda",
      metavar = "DOUBLE",
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
      help = """For perceptron: Total error threshold below which training stops (default: %default).""")

  var aggressiveness =
    ap.option[Double]("a", "aggressiveness",
      metavar = "DOUBLE",
      default = 1.0,
      help = """For perceptron: aggressiveness factor > 0.0 (default: %default).""")

  var rounds =
    ap.option[Int]("r", "rounds",
      default = 10000,
      help = """For perceptron: maximum number of training rounds (default: %default).""")

  var debug =
    ap.option[String]("d", "debug", metavar = "FLAGS",
      help = """Output debug info of the given types.  Multiple debug
parameters can be specified, indicating different types of info to output.
Separate parameters by spaces, colons or semicolons.  Params can be boolean,
if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
list-valued; multiple values are specified by including the parameter
multiple times, or by separating values by a comma.
""")

}

/**
 * An application that reads in a standard format set of CSVs representing
 * instances, trains a Naive Bayes model, and then outputs the predictions
 * on a second set of instances (ignoring the labels).
 */
object ClassifyApp extends ExperimentApp("classify") {

  type TParam = ClassifyParameters

  def create_param_object(ap: ArgParser) = new ClassifyParameters(ap)

  def initialize_parameters() {
    val ap = arg_parser
    if (params.lambda < 0)
      ap.error("Lambda value should be greater than or equal to zero.")
    if (params.aggressiveness <= 0)
      ap.error("Aggressiveness value should be strictly greater than zero.")
    if (params.trainSource == null)
      ap.error("No training file provided.")
    if (params.predictSource == null)
      ap.error("No input file provided.")
    if (params.debug != null)
      parse_debug_spec(params.debug)
  }

  def run_program(args: Array[String]) = {
    val local = io.local_file_handler
    val std = io.std_file_handler
    val trainSource = local.openr(params.trainSource)
    val predictSource = local.openr(params.predictSource)

    // If the output file is given via the option, create and write to that 
    // file; otherwise, use stdout.
    val output = {
      if (params.output == null)
        std.openw("stdout")
      else
        local.openw(params.output)
    }

    // Train the classifier
    val factory = new SparseNominalInstanceFactory
    val training_instances =
      factory.get_csv_labeled_instances(trainSource, is_training = true).
        toIndexedSeq
    val test_instances =
      factory.get_csv_labeled_instances(predictSource, is_training = false).
        toIndexedSeq
    val numlabs = factory.number_of_labels
    if (numlabs < 2) {
      println("Found %d different labels, when at least 2 are needed." format
        numlabs)
      System.exit(0)
    }

    // Train a classifer
    val trainer = params.method match {
      case "pa-perceptron" if numlabs > 2 || debug("multilabel") => {
        errprint("Using passive-aggressive multi-label perceptron")
        new PassiveAggressiveNoCostMultiWeightMultiLabelPerceptronTrainer[
          FeatureVector
        ](ArrayVector, numlabs, params.variant,
          params.aggressiveness,
          error_threshold = params.error_threshold,
          max_iterations = params.rounds)
      }
      case "pa-perceptron" => {
        errprint("Using passive-aggressive binary perceptron")
        new PassiveAggressiveBinaryPerceptronTrainer(
          ArrayVector, params.variant, params.aggressiveness,
          error_threshold = params.error_threshold,
          max_iterations = params.rounds)
      }
      case _ if numlabs > 2 || debug("multilabel") => {
        errprint("Using basic multi-label perceptron")
        new BasicMultiWeightMultiLabelPerceptronTrainer[
          FeatureVector
        ](ArrayVector, numlabs, params.aggressiveness,
          averaged = params.method == "avg-perceptron",
          error_threshold = params.error_threshold,
          max_iterations = params.rounds)
      }
      case _ => {
        errprint("Using basic binary perceptron")
        new BasicBinaryPerceptronTrainer(
          ArrayVector, params.aggressiveness,
          averaged = params.method == "avg-perceptron",
          error_threshold = params.error_threshold,
          max_iterations = params.rounds)
      }
    }
    val classifier = trainer(training_instances)

    // Run classifier on each instance to get the predictions, and output
    // them in reverse sorted order, tab separated
    val predheader = ((1 to numlabs)
             map (num => "Pred%s\tScore%s" format (num, num))
             mkString "\t")
    output.println("Corr?\tConf\tTruelab\t%s" format predheader)
    val results =
      for ((inst, truelab) <- test_instances) yield {
        // Scores in reverse sorted order
        val scores = classifier.sorted_scores(inst)
        val correct = scores(0)._1 == truelab
        val corrstr = if (correct) "CORRECT" else "WRONG"
        val conf = scores(0)._2 - scores(1)._2
        // Map to labels
        val scorelabs = scores map {
          case (lab, pred) => (factory.index_to_label(lab), pred)
        }
        val preds = scorelabs.map { case (l, p) => l + " " + p }.
                    mkString("\t")
        val line = "%s\t%s\t%s\t%s" format (corrstr, conf, truelab, preds)
        (correct, conf, line)
      }
    val accuracy = results.map {
      case (correct, _, _) => if (correct) 1 else 0
    }.sum.toDouble / results.size
    for ((correct, conf, line) <- results) {
      output.println(line)
    }
    output.println("Accuracy: %.2f%%" format (accuracy * 100))

    output.flush
    output.close
    0
  }
}
