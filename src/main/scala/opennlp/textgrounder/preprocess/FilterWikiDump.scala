//  FilterWikiDump.scala
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

class FilterWikiDumpParameters(ap: ArgParser) {
  var title_file = ap.option[String]("title-file",
    must = be_specified,
    help = """File containing article titles to be filtered out.""")
  var invert = ap.flag("invert",
    help = """Invert the filter, preserving only article titles not
specified.""")
  var input = ap.option[String]("input",
    must = be_specified,
    help = """Wikipedia dump file to filter.""")
}

/**
 * An application to filter a Wikipedia dump file, either selecting only or
 * all but the articles with titles in --title-file (one title per line).
 */
object FilterWikiDump extends ExperimentApp("FilterWikiDump") {

  type TParam = FilterWikiDumpParameters

  def create_param_object(ap: ArgParser) = new FilterWikiDumpParameters(ap)

  def run_program(args: Array[String]) = {
    var inpage = false
    var page_matches = false
    var curpage: StringBuilder = null

    val titles = localfh.openr(params.title_file).toSet

    val page_re = "^.*<page>.*$".r
    val page_end_re = "^.*</page>.*$".r
    val title_re = "^.*<title>(.*)</title>.*$".r

    for (line <- localfh.openr(params.input)) {
      line match {
        case page_re() => {
          // Beginning of article -- reset variables, accumulate line
          inpage = true
          page_matches = false
          curpage = new StringBuilder()
          curpage ++= line + "\n"
        }
        case page_end_re() => {
          // End of article -- print if matching, or if not matching and
          // --invert
          inpage = false
          curpage ++= line + "\n"
          if (page_matches == !params.invert) {
            print(curpage.toString)
          }
        }
        case title_re(title) if inpage => {
          // In article, found title -- check whether title matches
          if (titles contains title)
            page_matches = true
          curpage ++= line + "\n"
        }
        case _ if inpage => {
          // Some other line within article -- accumulate
          curpage ++= line + "\n"
        }
        case _ => {
          // Some other line outside article -- print immediately
          println(line)
        }
      }
    }
    0
  }
}
