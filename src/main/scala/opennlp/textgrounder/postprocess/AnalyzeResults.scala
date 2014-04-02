//  AnalyzeResults.scala
//
//  Copyright (C) 2013-2014 Ben Wing, The University of Texas at Austin
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
import util.spherical._
import util.experiment._
import util.io
import util.math._
import util.print._
import util.serialize.TextSerializer
import util.table.format_table_with_header
import util.textdb._

import util.debug._

class AnalyzeResultsParameters(ap: ArgParser) {
  var pred_cell_distribution = ap.option[String]("pred-cell-distribution",
    "pred-cell-distrib", "pcd",
    metavar = "FILE",
    help="""Output Zipfian distribution of predicted cells,
to see the extent to which they are balanced or unbalanced.""")

  var correct_cell_distribution = ap.option[String]("correct-cell-distribution",
    "correct-cell-distrib", "ccd",
    metavar = "FILE",
    help="""Output Zipfian distribution of correct cells,
to see the extent to which they are balanced or unbalanced.""")

  var input = ap.positional[String]("input",
    must = be_specified,
    help = """Results file to analyze, a textdb database. The value can be
  any of the following: Either the data or schema file of the database;
  the common prefix of the two; or the directory containing them, provided
  there is only one textdb in the directory.""")

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
 * An application to analyze the results from a TextGrounder run,
 * as output using --results.
 */
object AnalyzeResults extends ExperimentApp("AnalyzeResults") {

  type TParam = AnalyzeResultsParameters

  def create_param_object(ap: ArgParser) = new AnalyzeResultsParameters(ap)

  def fmt_sphere_coord(coord: String) = SphereCoord.deserialize(coord).format

  def output_freq_of_freq(filehand: io.FileHandler, file: String,
      map: collection.Map[String, Int],
      cell_stats: collection.Map[String, CellStats]) {
    val headings = Seq("rank", "cell", "central-pt", "count", "numdocs", "cum")
    val numcells = map.values.sum
    var sofar = 0
    val outf = filehand.openw(file)
    val results =
      for (((cell, count), ind) <-
          map.toSeq.sortWith(_._2 > _._2).zipWithIndex) yield {
        sofar += count
        val stats = cell_stats(cell)
        val Array(cell_sw_str,cell_ne_str) = cell.split(":")
        val cell_sw = fmt_sphere_coord(cell_sw_str)
        val cell_ne = fmt_sphere_coord(cell_ne_str)
        val central_pt = fmt_sphere_coord(stats.central_point)
        Seq(s"${ind + 1}", s"$cell_sw:$cell_ne", central_pt, s"$count",
          s"${stats.numdocs}",
          "%.2f%%" format (sofar.toDouble / numcells * 100))
    }
    outf.println(format_table_with_header(headings, results))
    //val fmt = table_column_format(headings +: results)
    //fmt.format(headings: _*))
    //outf.println("-" * 70)
    //results.map { line =>
    //  outf.println(fmt.format(line: _*))
    //}
    outf.close()
  }

  def print_stats(prefix: String, units: String, nums: IndexedSeq[Double]) {
    def pr(fmt: String, args: Any*) {
      outprint("%s: %s", prefix, fmt format (args: _*))
    }
    pr("Mean: %.2f%s +/- %.2f%s", mean(nums), units, stddev(nums), units)
    pr("Median: %.2f%s", median(nums), units)
    pr("Mode: %.2f%s", mode(nums), units)
    pr("Range: [%.2f%s to %.2f%s]", nums.min, units, nums.max, units)
  }

  case class CellStats(numdocs: Int, central_point: String)

  def run_program(args: Array[String]) = {
    val cell_stats = mutable.Map[String, CellStats]()
    val correct_cells = intmap[String]()
    val pred_cells = intmap[String]()
    var numtokens = Vector[Double]()
    var numtypes = Vector[Int]()
    var numcorrect = 0
    var numseen = 0
    var oracle_dist_true_center = Vector[Double]()
    var oracle_dist_centroid = Vector[Double]()
    var oracle_dist_central_point = Vector[Double]()
    var error_dist_true_center = Vector[Double]()
    var error_dist_centroid = Vector[Double]()
    var error_dist_central_point = Vector[Double]()

    val filehand = io.localfh
    for (row <- TextDB.read_textdb(filehand, params.input)) {
      correct_cells(row.gets("correct-cell")) += 1
      pred_cells(row.gets("pred-cell")) += 1
      cell_stats(row.gets("correct-cell")) =
        CellStats(row.get[Int]("correct-cell-numdocs"),
          row.gets("correct-cell-central-point"))
      cell_stats(row.gets("pred-cell")) =
        CellStats(row.get[Int]("pred-cell-numdocs"),
          row.gets("pred-cell-central-point"))
      val correct_coord = row.get[SphereCoord]("correct-coord")
      def dist_to(field: String) =
        spheredist(correct_coord, row.get[SphereCoord](field))
      oracle_dist_true_center :+= dist_to("correct-cell-true-center")
      oracle_dist_centroid :+= dist_to("correct-cell-centroid")
      oracle_dist_central_point :+= dist_to("correct-cell-central-point")
      error_dist_true_center :+= dist_to("pred-cell-true-center")
      error_dist_centroid :+= dist_to("pred-cell-centroid")
      error_dist_central_point :+= dist_to("pred-cell-central-point")
      numtypes :+= row.get[Int]("numtypes")
      numtokens :+= row.get[Double]("numtokens")
      numseen += 1
      if (row.get[Int]("correct-rank") == 1)
        numcorrect += 1
    }
    outprint("Accuracy: %.4f (%s/%s)" format
      ((numcorrect.toDouble / numseen), numcorrect, numseen))
    print_stats("Oracle distance to central point", " km", oracle_dist_central_point)
    print_stats("Oracle distance to centroid", " km", oracle_dist_centroid)
    print_stats("Oracle distance to true center", " km", oracle_dist_true_center)
    print_stats("Error distance to central point", " km", error_dist_central_point)
    print_stats("Error distance to centroid", " km", error_dist_centroid)
    print_stats("Error distance to true center", " km", error_dist_true_center)
    print_stats("Word types per document", "", numtypes map { _.toDouble })
    print_stats("Word tokens per document", "", numtokens map { _.toDouble })
    if (params.pred_cell_distribution != null)
      output_freq_of_freq(filehand, params.pred_cell_distribution, pred_cells,
        cell_stats)
    if (params.correct_cell_distribution != null)
      output_freq_of_freq(filehand, params.correct_cell_distribution,
        correct_cells, cell_stats)
    0
  }
}
