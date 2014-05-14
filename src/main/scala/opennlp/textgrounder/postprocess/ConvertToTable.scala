//  ConvertToTable.scala
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

import collection.mutable

import util.argparser._
import util.experiment._
import util.io.stdfh
import util.numeric.pretty_double
import util.print._
import util.table._

import util.debug._

class ConvertToTableParameters(ap: ArgParser) {
  var x_field = ap.option[String]("x-field",
    metavar = "FIELD",
    must = be_specified,
    help="""Field name to use for data along the X axis.""")

  var y_field = ap.option[String]("y-field",
    metavar = "FIELD",
    must = be_specified,
    help="""Field name to use for data along the Y axis.""")

  var x_regexp = ap.option[String]("x-regexp",
    metavar = "REGEXP",
    default = "(.*)",
    help="""Regexp to pick out X-axis value from the field. Should
have one group in it to pick out the value.""")

  var y_regexp = ap.option[String]("y-regexp",
    metavar = "REGEXP",
    default = "(.*)",
    help="""Regexp to pick out Y-axis value from the field. Should
have one group in it to pick out the value.""")

  var data_field = ap.option[String]("data-field",
    metavar = "FIELD",
    must = be_specified,
    help="""Field name to use for data in the cells.""")

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
object ConvertToTable extends ExperimentApp("ConvertToTable") {

  type TParam = ConvertToTableParameters

  def create_param_object(ap: ArgParser) = new ConvertToTableParameters(ap)

  def find_field_index(fields: Array[String], field: String): Int = {
    for ((f, index) <- fields.zipWithIndex) {
      if (f == field)
        return index
    }
    throw new IllegalArgumentException(s"Unable to find field '$field'")
  }

  def run_program(args: Array[String]) = {
    val infile = stdfh.openr("stdin")
    val headers = infile.next.split("""\s+""")
    val x_field = find_field_index(headers, params.x_field)
    val y_field = find_field_index(headers, params.y_field)
    val data_field = find_field_index(headers, params.data_field)
    val values = mutable.Map[(Double, Double), String]()
    val x_regexp = params.x_regexp.r
    val y_regexp = params.y_regexp.r
    for (line <- infile) {
      val fields = line.trim.split("""\s+""")
      val x = fields(x_field) match {
        case x_regexp(d) => d.toDouble
      }
      val y = fields(y_field) match {
        case y_regexp(d) => d.toDouble
      }
      val z = fields(data_field)
      values((x, y)) = z
    }
    val x_values = values.keys.map(_._1).toSet.toSeq.sorted
    val y_values = values.keys.map(_._2).toSet.toSeq.sorted
    val table =
      for (y <- y_values) yield {
        for (x <- x_values) yield {
          values.getOrElse((x, y), "NA")
        }
      }
    val labeled_table =
      ("x/y" +: x_values.map(pretty_double(_))) +: {
        for ((y, row) <- y_values zip table) yield pretty_double(y) +: row
      }

    outprint(format_table(labeled_table))
    0
  }
}
