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
import util.io.localfh
import util.math._
import util.print._
import util.serialize.TextSerializer
import util.table._
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

  var detailed = ap.flag("detailed", "d",
    help="""Output in a multi-line format, with more information for each
file. Currently the results are always output file-by-file in the order
the files are specified on the command line, ignoring any '--sort-*' and
'--omit-*' parameters.""")

  var sort_average = ap.flag("sort-average", "sort", "sort-avg",
    help="""Sort by the average of mean and median error distance.""")

  var sort_mean = ap.flag("sort-mean",
    help="""Sort by the mean error distance.""")

  var sort_median = ap.flag("sort-median",
    help="""Sort by the median error distance.""")

  var sort_accuracy = ap.flag("sort-accuracy", "sort-acc",
    help="""Sort by accuracy.""")

  var sort_acc161 = ap.flag("sort-acc161",
    help="""Sort by accuracy within 161 km (about 100 miles).""")

  var sort_numeval = ap.flag("sort-numeval",
    help="""Sort by number of evaluation instances.""")

  var sort_file = ap.flag("sort-file", "sort-name",
    help="""Sort by file name.""")

  var no_sort = ap.flag("no-sort",
    help="""Don't sort; output according the order of files given on the
command line.""")

  var omit = ap.option[String]("omit",
    default = "",
    help="""Fields to omit, separated by commas. Possible values:
'average' or 'avg', 'mean', 'median', 'accuracy' or 'acc', 'numeval',
'file'""")

  var omitted_fields = if (omit == "") Array[String]() else omit.split(",")
  var possible_fields = Seq("average", "avg", "mean", "median", "accuracy",
    "acc", "numeval", "file")
  for (field <- omitted_fields) {
    if (!possible_fields.contains(field))
      ap.error(s"Unrecognized field: $field")
  }

  var input = ap.multiPositional[String]("input",
    must = be_specified,
    help = """Results file to analyze, a textdb database. The value can be
  any of the following: Either the data or schema file of the database;
  the common prefix of the two; or the directory containing them, provided
  there is only one textdb in the directory.""")

  var debug =
    ap.option[String]("debug", metavar = "FLAGS",
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

  def output_freq_of_freq(file: String,
      map: collection.Map[String, Int],
      cell_stats: collection.Map[String, CellStats]) {
    val headings = Seq("rank", "cell", "central-pt", "count", "numdocs", "cum")
    val numcells = map.values.sum
    var sofar = 0
    val outf = localfh.openw(file)
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

  def print_stats(prefix: String, nums: IndexedSeq[Double],
      km: Boolean = false) {
    def pr(fmt: String, args: Any*) {
      outprint("%s: %s", prefix, fmt format (args: _*))
    }
    val units = if (km) " km" else ""
    if (km) {
      val num_within_161 = nums.count(_ <= 161)
      val num_total = nums.size
      pr("Acc@161: %.2f%% (%s/%s)", num_within_161.toDouble / num_total * 100,
        num_within_161, num_total)
    }
    pr("Mean: %.2f%s +/- %.2f%s", mean(nums), units, stddev(nums), units)
    pr("Median: %.2f%s", median(nums), units)
    pr("Mode: %.2f%s", mode(nums), units)
    pr("Range: [%.2f%s to %.2f%s]", nums.min, units, nums.max, units)
  }

  case class CellStats(numdocs: Int, central_point: String)

  case class FileStats(file: String, numinst: Int, accuracy: Double,
    acc161: Double, mean: Double, median: Double, average: Double)

  def run_program(args: Array[String]) = {
    val file_stats = mutable.Buffer[FileStats]()
    for (infile <- params.input) {
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
      for (row <- TextDB.read_textdb(localfh, infile)) {
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
      if (!params.detailed) {
        val num_within_161 = error_dist_central_point.count(_ <= 161)
        val acc161 = num_within_161.toDouble / numseen * 100
        val accuracy = numcorrect.toDouble / numseen * 100
        val meanval = mean(error_dist_central_point)
        val medianval = median(error_dist_central_point)
        val average = (meanval + medianval) / 2.0
        file_stats += FileStats(infile, numinst = numseen,
          accuracy = accuracy, acc161 = acc161,
          mean = meanval, median = medianval,
          average = average)
      } else {
        outprint(s"File: $infile")
        outprint("Accuracy: %.2f%% (%s/%s)" format
          (numcorrect.toDouble / numseen * 100, numcorrect, numseen))
        print_stats("Oracle distance to central point", oracle_dist_central_point, km = true)
        print_stats("Oracle distance to centroid", oracle_dist_centroid, km = true)
        print_stats("Oracle distance to true center", oracle_dist_true_center, km = true)
        print_stats("Error distance to central point", error_dist_central_point, km = true)
        print_stats("Error distance to centroid", error_dist_centroid, km = true)
        print_stats("Error distance to true center", error_dist_true_center, km = true)
        print_stats("Word types per document", numtypes map { _.toDouble })
        print_stats("Word tokens per document", numtokens map { _.toDouble })
      }
      if (params.pred_cell_distribution != null)
        output_freq_of_freq(params.pred_cell_distribution, pred_cells,
          cell_stats)
      if (params.correct_cell_distribution != null)
        output_freq_of_freq(params.correct_cell_distribution,
          correct_cells, cell_stats)
    }
    if (!params.detailed) {
      val sorted_stats =
        if (params.no_sort) file_stats
        else if (params.sort_mean) file_stats.sortBy(_.mean)
        else if (params.sort_median) file_stats.sortBy(_.median)
        // Highest to lowest for these next three
        else if (params.sort_accuracy) file_stats.sortBy(-_.accuracy)
        else if (params.sort_acc161) file_stats.sortBy(-_.acc161)
        else if (params.sort_numeval) file_stats.sortBy(-_.numinst)
        else if (params.sort_file) file_stats.sortBy(_.file)
        else file_stats.sortBy(_.average)

      val headers = mutable.Buffer[String]()
      val of = params.omitted_fields
      val rows = sorted_stats.map { s =>
        headers.clear()
        val row = mutable.Buffer[String]()
        if (!of.contains("numeval")) {
          headers += "#Eval"
          row += s"${s.numinst}"
        }
        if (!of.contains("accuracy") && !of.contains("acc")) {
          headers += "%Acc."
          row += "%.2f" format s.accuracy
        }
        if (!of.contains("acc161")) {
          headers += "Acc@161"
          row += "%.2f" format s.acc161
        }
        if (!of.contains("mean")) {
          headers += "Mean"
          row += "%.2f" format s.mean
        }
        if (!of.contains("median")) {
          headers += "Median"
          row += "%.2f" format s.median
        }
        if (!of.contains("average") && !of.contains("avg")) {
          headers += "Average"
          row += "%.2f" format s.average
        }
        if (!of.contains("file") && !of.contains("name")) {
          headers += "File"
          row += s.file.replaceAll("""^results\.""", "").
              replaceAll("""\.(data|schema)\.txt$""", "")
        }
        row
      }
//      val headers = Seq("#Eval", "%Acc.", "Acc@161", "Mean", "Median",
//        "Average", "File")
//      val rows = sorted_stats.map { s =>
//        Seq(s"${s.numinst}", "%.2f" format s.accuracy, "%.2f" format s.acc161,
//          "%.2f" format s.mean, "%.2f" format s.median,
//          "%.2f" format s.average,
//          s.file.replaceAll("""^results\.""", "").
//            replaceAll("""\.(data|schema)\.txt$""", ""))
//      }
      outprint(format_table(headers +: rows))
    }
    0
  }
}
