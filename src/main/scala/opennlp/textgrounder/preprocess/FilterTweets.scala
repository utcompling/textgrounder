//  FilterTweets.scala
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
package preprocess

import scala.collection.mutable

import net.liftweb

import util.argparser._
import util.experiment._
import util.io.localfh
import util.print._

class FilterTweetsParameters(ap: ArgParser) {
  var id_file = ap.option[String]("id-file",
    must = be_specified,
    help = """File containing tweet ID's. There should be one ID per line.""")
  var output = ap.option[String]("output",
    must = be_specified,
    help = """Prefix of files to which filtered tweets are written to.
There is a separate output file for each input file.""")
  var input = ap.multiPositional[String]("input",
    must = be_specified,
    help = """Tweet file(s) to analyze.""")
}

/**
 * An application to filter JSON-format tweets in a series of files
 * by a list of tweet ID's.
 */
object FilterTweets extends ExperimentApp("FilterTweets") {

  type TParam = FilterTweetsParameters

  def create_param_object(ap: ArgParser) = new FilterTweetsParameters(ap)

  /**
   * Retrieve a string along a path, checking to make sure the path
   * exists.
   */
  def force_string(value: liftweb.json.JValue, fields: String*): String = {
    var fieldval = value
    var path = List[String]()
    for (field <- fields) {
      path :+= field
      fieldval \= field
      if (fieldval == liftweb.json.JNothing) {
        val fieldpath = path mkString "."
        // errprint("Can't find field path %s in tweet", fieldpath)
        return ""
      }
    }
    val values = fieldval.values
    if (values == null) {
      // errprint("Null value from path %s", fields mkString ".")
      ""
    } else
      fieldval.values.toString
  }

  def run_program(args: Array[String]) = {

    def parse_line(line: String) = {
      val parsed = liftweb.json.parse(line)
      val id = force_string(parsed, "id_str")
      if (id != "")
        Some(id.toLong)
      else
        None
    }

    // Set of tweet ID's to filter on
    errprint("Loading tweet ID's from %s...", params.id_file)
    val ids =
      (for (line <- localfh.openr(params.id_file)) yield line.toLong).toSet

    for (file <- params.input) {
      errprint("Processing %s...", file)
      val (lines, comptype, uncompname) =
        localfh.openr_with_compression_info(file)
      val outfile = localfh.openw("%s%s" format (params.output, uncompname),
        compression = comptype)
      for (line <- lines) {
        parse_line(line) match {
          case Some(tweet_id) => {
            if (ids contains tweet_id)
              outfile.println(line)
          }
          case None => ()
        }
      }
      outfile.close()
    }
    0
  }
}
