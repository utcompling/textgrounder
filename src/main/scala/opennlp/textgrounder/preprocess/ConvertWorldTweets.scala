//  ConvertWorldTweets.scala
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

import net.liftweb

import util.argparser._
import util.debug._
import util.experiment._
import util.io.{localfh,stdfh}
import util.print._
import util.textdb._

class ConvertWorldTweetsParameters(ap: ArgParser) {
  var output =
    ap.option[String]("output", metavar = "FILE",
      must = be_specified,
      help = """Base of output to write data to. Data is written in
TextDB format.""")

  var split =
    ap.option[String]("split",
      must = be_specified,
      help = """Name of split (training, dev, test).""")

  var corpus_name =
    ap.option[String]("corpus-name",
      default = "han-twitter-world",
      help = """Corpus name to put into the schema file.""")

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
 * An application to convert the JSON-format World Twitter corpus sent to me by
 * Bo Han into the format used by TextGrounder.
 */
object ConvertWorldTweets extends ExperimentApp("ConvertWorldTweets") {

  type TParam = ConvertWorldTweetsParameters

  def create_param_object(ap: ArgParser) = new ConvertWorldTweetsParameters(ap)

  def run_program(args: Array[String]) = {
    val infile = stdfh.openr("stdin")
    val rows = for (line <- infile) yield {
      val parsed = liftweb.json.parse(line)
      val lat = (parsed \ "lat" values).asInstanceOf[Number].doubleValue
      val long = (parsed \ "lon" values).asInstanceOf[Number].doubleValue
      val uid = (parsed \ "uid" values).asInstanceOf[String]
      val city = (parsed \ "city" values).asInstanceOf[String]
      val features = (parsed \ "fea" values).asInstanceOf[Map[String, Number]].
        map { case (name, value) => (name, value.intValue) }
      Seq(
        "user" -> Encoder.string(uid),
        "coord" -> s"$lat,$long",
        "city" -> Encoder.string(city),
        "unigram-counts" -> Encoder.count_map(features)
      )
    }
    TextDB.write_constructed_textdb(localfh, params.output, rows,
      Map(
        "split" -> params.split,
        "corpus-type" -> "twitter-user",
        "corpus-name" -> params.corpus_name
      )
    )
    0
  }
}
