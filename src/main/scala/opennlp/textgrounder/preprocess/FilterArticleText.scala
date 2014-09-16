//  FilterArticleText.scala
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

import util.argparser._
import util.experiment._
import util.io.localfh
import util.print._

class FilterArticleTextParameters(ap: ArgParser) {
  var title_file = ap.option[String]("title-file",
    must = be_specified,
    help = """File containing article titles to be filtered out.""")
  var invert = ap.flag("invert",
    help = """Invert the filter, preserving only article titles not
specified.""")
  var input = ap.option[String]("input",
    must = be_specified,
    help = """Article text file to filter.""")
}

/**
 * An application to filter an article text file in the TextGrounder format
 * with one word per line, either selecting only or all but the articles
 * with titles in --title-file (one title per line).
 */
object FilterArticleText extends ExperimentApp("FilterArticleText") {

  type TParam = FilterArticleTextParameters

  def create_param_object(ap: ArgParser) = new FilterArticleTextParameters(ap)

  def run_program(args: Array[String]) = {

    val titles = localfh.openr(params.title_file).toSet

    var output = params.invert
    val title_re = "^Article title: (.*)$".r
    for (line <- localfh.openr(params.input)) {
      line match {
        case title_re(title) => {
          output = titles contains title
          if (params.invert)
            output = !output
        }
        case _ => {}
      }
      if (output)
        outprint("%s", line)
    }
    0
  }
}
