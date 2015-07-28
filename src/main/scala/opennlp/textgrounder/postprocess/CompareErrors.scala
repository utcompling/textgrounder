//  CompareErrors.scala
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
import util.experiment._
import util.io.localfh
import util.math._
import util.print._
import util.table._
import util.textdb._

import util.debug._

/**
 * See description under `CompareErrors`.
 */
class CompareErrorsParameters(ap: ArgParser) {
  var input1 = ap.option[String]("input1", metavar = "FILE",
    must = be_specified,
    help = """First results file to analyze, a textdb database. The value
  can be any of the following: Either the data or schema file of the database;
  the common prefix of the two; or the directory containing them, provided
  there is only one textdb in the directory.""")

  var input2 = ap.option[String]("input2", metavar = "FILE",
    help = """Second results file to analyze, a textdb database. The value
  can be any of the following: Either the data or schema file of the database;
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
 * If two different results files are given, compare the error distances
 * from the results output from two different TextGrounder runs. Output a
 * table sorted by absolute difference between the error distances, from
 * highest to lowest.
 *
 * If only one results file is given, output a table of erro distances,
 * from highest to lowest.
 */
object CompareErrors extends ExperimentApp("CompareErrors") {

  type TParam = CompareErrorsParameters

  def create_param_object(ap: ArgParser) = new CompareErrorsParameters(ap)

  def run_program(args: Array[String]) = {
    if (params.input2 != null) {
      val error_dist_1 = mutable.Map[String, Double]()
      val error_dist_2 = mutable.Map[String, Double]()
      for (row <- TextDB.read_textdb(localfh, params.input1))
        error_dist_1(row.gets("title")) = row.get[Double]("error-dist")
      for (row <- TextDB.read_textdb(localfh, params.input2))
        error_dist_2(row.gets("title")) = row.get[Double]("error-dist")
      val error_dist_diff =
        (for ((title, errdist1) <- error_dist_1;
             if error_dist_2 contains title;
             errdist2 = error_dist_2(title))
          yield (title, errdist2 - errdist1)).toMap
      val rows =
        for ((title, diff) <-
             error_dist_diff.toSeq.sortWith(_._2.abs > _._2.abs)) yield
          Seq(title, "%.2f" format error_dist_1(title),
            "%.2f" format error_dist_2(title), "%.2f" format diff)
      val headers = Seq("title", "errdist1", "errdist2", "errdiff")
      outprint(format_table(headers +: rows))
    } else {
      val error_dist_1 = mutable.Map[Row, Double]()
      for (row <- TextDB.read_textdb(localfh, params.input1))
        error_dist_1(row) = row.get[Double]("error-dist")
      val rows =
        for ((row, error) <-
             error_dist_1.toSeq.sortWith(_._2 > _._2)) yield
          Seq(row.gets("title"), "%.2f" format error,
            row.gets("correct-coord"), row.gets("pred-coord"))
      val headers = Seq("title", "errdist", "correct-coord", "pred-coord")
      outprint(format_table(headers +: rows))
    }
    0
  }
}
