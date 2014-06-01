//  VWWeights.scala
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
package postprocess

import scala.util.control.Breaks._
import scala.collection.mutable

import util.argparser._
import util.collection._
import util.experiment._
import util.io._
import util.print._
import util.table.output_reverse_sorted_table

import util.debug._

class VWWeightsParameters(ap: ArgParser) {
  var readable_model = ap.option[String]("readable-model",
    "rm",
    metavar = "FILE",
    help="""File output using --readable_model argument in Vowpal Wabbit.""")

  var invert_hash = ap.option[String]("invert-hash",
    "ih",
    metavar = "FILE",
    help="""File output using --invert_hash argument in Vowpal Wabbit.""")

  var output_highest_weights = ap.option[String]("output-highest-weights",
    "ohw",
    metavar = "FILE",
    help="""File to output highest seen weight for each feature to.
There is a separate weight for each combination of feature and cell.
This outputs the highest weight seen for any cell.""")

  var output_weights = ap.option[String]("output-weights",
    "ow",
    metavar = "FILE",
    help="""File to output weights to.
There is a separate weight for each combination of feature and cell.""")

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
 * An application to combine the named features available from VW's
 * --invert_hash with the weights available from VW's --readable_model.
 * This is because VW's BFGS doesn't with --invert_hash, but the equivalent
 * can be gotten by running with the same parameters using SGD to get the
 * mapping between feature names and hashed bins, and combining this mapping
 * with the bin numbers and weights from --readable_model.
 */
object VWWeights extends ExperimentApp("VWWeights") {

  type TParam = VWWeightsParameters

  def create_param_object(ap: ArgParser) = new VWWeightsParameters(ap)

  def get_weights(file: String): Map[Int, Double] = {
    val fh = localfh.openr(file)
    breakable {
      while (true) {
        val line = fh.next
        if (line == ":0")
          break
      }
    }
    (for (line <- fh) yield {
      val Array(featnum, weight) = line.split(":")
      (featnum.toInt, weight.toDouble)
    }).toMap
  }

  // Mapping from feature ID to (feature-name, cell)
  def get_names(file: String): Map[Int, (String, Int)] = {
    val fh = localfh.openr(file)
    breakable {
      while (true) {
        val line = fh.next
        if (line == ":0")
          break
      }
    }
    val name_count = intmap[String]()
    (for (line <- fh) yield {
      val Array(feat, featnum, _) = line.split(":")
      // Features in the VW files normally begin with " ^"
      val name = if (feat.startsWith(" ^")) feat.drop(2) else feat
      name_count(name) += 1
      (featnum.toInt, (name, name_count(name)))
    }).toMap
  }

  def run_program(args: Array[String]) = {
    val names = get_names(params.invert_hash)
    val weights = get_weights(params.readable_model)
    val named_weights = weights map { case (featnum, weight) =>
      val (name, cell) = names.getOrElse(featnum, ("???", 0))
      val tagged_name = s"$name:cell"
      ((name, cell), weight)
    }
    val sorted_weights = named_weights.toSeq.sortWith(_._2 > _._2)
    if (params.output_highest_weights != null) {
      val highest_weight = doublemap[String]()
      sorted_weights.foreach { case ((name, cell), weight) =>
        // Under normal circumstances, the first weight we see will
        // be the highest, but not always because of negative weights
        if (weight.abs > highest_weight(name).abs)
          highest_weight(name) = weight
      }
      output_reverse_sorted_table(highest_weight,
        outfile = localfh.openw(params.output_highest_weights))
    }
    if (params.output_weights != null) {
      val weight_file = localfh.openw(params.output_weights)
      sorted_weights.foreach { case ((name, cell), weight) =>
        weight_file.println(s"$name:$cell = $weight")
      }
    }
    0
  }
}
