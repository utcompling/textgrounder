//  AnalyzeProperty.scala
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

import math.Ordering.Implicits._

import util.argparser._
import util.experiment._
import util.io
import util.print._
import util.textdb._

import util.debug._

class AnalyzePropertyParameters(ap: ArgParser) {
  var field = ap.option[String]("field",
    metavar = "FIELD",
    must = be_specified,
    help = """Field to analyze.""")
  var field_type = ap.option[String]("field-type",
    metavar = "TYPE",
    choices = Seq("integer", "real", "string"),
    default = "string",
    help = """Type of field: 'integer', 'real', 'string'. Default %default.""")
  var input = ap.positional[String]("input",
    must = be_specified,
    help = """Textdb database to analyze. The value can be
any of the following: Either the data or schema file of the database;
the common prefix of the two; or the directory containing them, provided
there is only one textdb in the directory.""")
}

/**
 * An application to analyze the different values of some field seen in
 * a textdb corpus, displaying minimum, maximum, quantiles, etc.
 */
object AnalyzeProperty extends ExperimentApp("AnalyzeProperty") {

  type TParam = AnalyzePropertyParameters

  def create_param_object(ap: ArgParser) = new AnalyzePropertyParameters(ap)

  def output_quantiles[T : Ordering](origvals: IndexedSeq[T]) {
    val vals = origvals.sorted
    val size = vals.size
    if (size > 0) {
      errprint("Total number non-blank: %s", size)
      errprint("Minimum: %s", vals.head)
      errprint("Maximum: %s", vals.last)
      val numquants = 20 min size
      // Figure out how many spaces needed to hold displayed index into vals
      val posneeded = math.log10(size).toInt + 1
      val fmtstr = ("Quantile %2s/%2s (%4.1f%%, %" + posneeded +
        "s - %" + posneeded + "s): %s - %s")

      for (x <- 0 until numquants) {
        val start = size * x / numquants
        val nextstart = size * (x + 1) / numquants
        val end = nextstart - 1
        assert (end >= start)
        errprint(fmtstr,
          x, numquants, x.toDouble*100/numquants, start, end, vals(start),
          vals(end))
      }
    }
  }

  def run_program(args: Array[String]) = {
    val filehand = io.localfh
    val fields = TextDB.read_textdb(filehand, params.input).map { row =>
      row.gets(params.field)
    }.toIndexedSeq
    errprint("For field %s:", params.field)
    errprint("Total number: %s", fields.size)
    val nonblank = fields.filter(_ != "")
    errprint("Total number blank: %s", fields.size - nonblank.size)
    params.field_type match {
      case "integer" => {
        val vals = nonblank.map(_.toLong)
        output_quantiles(vals)
      }
      case "real" => {
        val vals = nonblank.map(_.toDouble)
        output_quantiles(vals)
      }
      case "string" => {
        output_quantiles(nonblank)
      }
    }
    0
  }
}
