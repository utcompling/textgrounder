//  AnalyzeResults.scala
//
//  Copyright (C) 2013 Ben Wing, The University of Texas at Austin
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

import collection.mutable

import util.argparser._
import util.collection._
import util.experiment._
import util.io
import util.print._
import util.textdb._

import util.debug._

class AnalyzeResultsParameters(ap: ArgParser) {
  var pred_cell_distribution = ap.option[String]("pred-cell-distribution",
    "pred-cell-distrib", "pcd",
    metavar = "FILE",
    help="""Output Zipfian distribution of predicted cells,
to see the extent to which they are balanced or unbalanced.""")
  var true_cell_distribution = ap.option[String]("true-cell-distribution",
    "true-cell-distrib", "tcd",
    metavar = "FILE",
    help="""Output Zipfian distribution of true cells,
to see the extent to which they are balanced or unbalanced.""")
  var input = ap.positional[String]("input",
    help = "Results file to analyze.")

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
 * An application to analyze the results from a TextGrounder run,
 * as output using --results.
 */
object AnalyzeResults extends ExperimentApp("classify") {

  type TParam = AnalyzeResultsParameters

  def create_param_object(ap: ArgParser) = new AnalyzeResultsParameters(ap)

  def initialize_parameters() {
    if (params.debug != null)
      parse_debug_spec(params.debug)
  }

  def output_freq_of_freq(filehand: io.FileHandler, file: String,
      map: collection.Map[String, Int]) {
    val numcells = map.values.sum
    var sofar = 0
    val outf = filehand.openw(file)
    for (((cell, count), ind) <-
        map.toSeq.sortWith(_._2 > _._2).zipWithIndex) {
      sofar += count
      outf.println("%s  %s  %s  %.2f%%" format (
        ind + 1, cell, count, sofar.toDouble / numcells * 100))
    }
    outf.close()
  }

  def run_program() = {
    val true_cells = intmap[String]()
    val pred_cells = intmap[String]()

    val filehand = io.local_file_handler
    val input_file =
      if (params.input contains "/") params.input
      else "./" + params.input
    val (dir, base) = filehand.split_filename(input_file)
    val (schema, field_iter) =
      TextDB.read_textdb_with_schema(filehand, dir, prefix = base)
    for (fieldvals <- field_iter.flatten) {
      val true_cell = schema.get_field(fieldvals, "true-cell")
      val pred_cell = schema.get_field(fieldvals, "pred-cell")
      true_cells(true_cell) += 1
      pred_cells(pred_cell) += 1
    }
    if (params.pred_cell_distribution != null)
      output_freq_of_freq(filehand, params.pred_cell_distribution, pred_cells)
    if (params.true_cell_distribution != null)
      output_freq_of_freq(filehand, params.true_cell_distribution, true_cells)
    0
  }
}
