//  SplitArticleText.scala
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

import java.io._

import util.argparser._
import util.experiment._
import util.io.localfh
import util.print._

class SplitArticleTextParameters(ap: ArgParser) {
  var input = ap.option[String]("input",
    must = be_specified,
    help = """Article text file to filter.""")
}

/**
 * An application to split an article text file in the TextGrounder format
 * into individual .txt files, with the corresponding article titles and ID's
 * going into a .dat file. We skip files not in the main namespace.
 */
object SplitArticleText extends ExperimentApp("SplitArticleText") {

  type TParam = SplitArticleTextParameters

  def create_param_object(ap: ArgParser) = new SplitArticleTextParameters(ap)

  def run_program(args: Array[String]) = {
    var output_txt: PrintStream = null
    var output_dat: PrintStream = null
    val title_re = "^Article title: (.*)$".r
    val id_re = "^Article ID: (.*)$".r
    for (line <- localfh.openr(params.input)) {
      line match {
        case title_re(title) => {
          if (output_txt != null) {
            output_txt.close()
            output_txt = null
          }
          if (output_dat != null) {
            output_dat.close()
            output_dat = null
          }
          if (!title.contains(':')) {
            output_txt = localfh.openw(title.replace("/", "_") + ".txt")
            output_dat = localfh.openw(title.replace("/", "_") + ".dat")
            output_dat.println(line)
          }
        }
        case id_re(id) => {
          if (output_dat != null)
            output_dat.println(line)
        }
        case _ => {
          if (output_txt != null)
            output_txt.println(line)
        }
      }
    }
    if (output_txt != null)
      output_txt.close()
    if (output_dat != null)
      output_dat.close()
    0
  }
}
