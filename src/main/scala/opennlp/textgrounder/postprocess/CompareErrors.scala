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
  var input1 = ap.positional[String]("input1",
    must = be_specified,
    help = """First results file to analyze, a textdb database. The value
  can be any of the following: Either the data or schema file of the database;
  the common prefix of the two; or the directory containing them, provided
  there is only one textdb in the directory.""")

  var input2 = ap.positional[String]("input2",
    must = be_specified,
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
 * Compare the error distances from the results output from two different
 * TextGrounder runs. Output a table sorted by absolute difference between
 * the error distances, from highest to lowest.
 */
object CompareErrors extends ExperimentApp("CompareErrors") {

  type TParam = CompareErrorsParameters

  def create_param_object(ap: ArgParser) = new CompareErrorsParameters(ap)

  def run_program(args: Array[String]) = {
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
    0
  }
}
