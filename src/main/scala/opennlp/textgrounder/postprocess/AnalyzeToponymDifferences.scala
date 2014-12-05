//  AnalyzeToponymDifferences.scala
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
import util.collection._
import util.experiment._
import util.io.localfh
import util.math._
import util.print._
import util.table._
import util.textdb._

import util.debug._

/**
 * See description under `AnalyzeToponymDifferences`.
 */
class AnalyzeToponymDifferencesParameters(ap: ArgParser) {
  var input1 = ap.positional[String]("input1",
    must = be_specified,
    help = """First toponym prediction file to analyze, a textdb database.
The value can be any of the following: Either the data or schema file of the
database; the common prefix of the two; or the directory containing them,
provided there is only one textdb in the directory.""")

  var input2 = ap.positional[String]("input2",
    must = be_specified,
    help = """Second toponym prediction file to analyze.""")

  //var ambiguity = ap.positional[String]("ambiguity",
  //  help = """File listing toponym ambiguities.""")
}

/**
 * Compare the toponym predictions of two different runs for the same corpus.
 */
object AnalyzeToponymDifferences extends ExperimentApp("AnalyzeToponymDifferences") {

  type TParam = AnalyzeToponymDifferencesParameters

  def create_param_object(ap: ArgParser) = new AnalyzeToponymDifferencesParameters(ap)

  def process_file(file: String) = {
    val predmap = mutable.Map[(String, Int, Int), (String, Int)]()
    val toplocs = intmapmap[String, String]()
    //for (line <- localfh.openr(file)) {
    //  val Array(doc, sentind, topind, toponym, selected, location, coords) =
    //    line.split("\t")
    //  predmap((doc, sentind.toInt, topind.toInt)) = (toponym, selected.toInt)
    //  toplocs(toponym)(location) += 1
    //}
    for (row <- TextDB.read_textdb(localfh, file)) {
      val toponym = row.gets("toponym")
      predmap((row.gets("docid"), row.gets("sentind").toInt, row.gets("topind").toInt)) =
        (toponym, row.gets("selectedind").toInt)
      toplocs(toponym)(row.gets("predloc")) += 1
    }
    val most_common_top = toplocs.map { case (toponym, locs) =>
      (toponym, locs.toSeq.maxBy(_._2))
    }.toMap
    (predmap, most_common_top)
  }

  def run_program(args: Array[String]) = {
    //val ambigmap = mutable.Map[String, Int]()
    //if (params.ambiguity != null)
    //  for (line <- localfh.openr(params.ambiguity)) {
    //    val Array(doc, sentind, topind, toponym, ambiguity) = line.split("\t")
    //    ambigmap(toponym) = ambiguity
    //  }

    val (pred1, most_common_top1) = process_file(params.input1)
    val (pred2, most_common_top2) = process_file(params.input2)
    val diffmap = intmap[String]()
    val countmap = intmap[String]()
    for ((key, value1) <- pred1) {
      val value2 = pred2(key)
      val (top1, sel1) = value1
      val (top2, sel2) = value2
      assert(top1 == top2)
      countmap(top1) += 1
      if (sel1 != sel2)
        diffmap(top1) += 1
    }
    val topdiffs =
      (for ((top, numdiff) <- diffmap.toSeq) yield {
        (top, numdiff.toDouble / countmap(top))
      }).sortWith(_._2 > _._2)
    val rows =
      for ((toponym, difffrac) <- topdiffs) yield {
        val (loc1, count1) = most_common_top1(toponym)
        val (loc2, count2) = most_common_top2(toponym)
        val topcount = countmap(toponym)
        Seq(toponym, topcount.toString, "%.2f" format (difffrac * 100), 
          loc1, "%.2f" format (count1.toDouble / topcount * 100),
          loc2, "%.2f" format (count2.toDouble / topcount * 100))
      }
    val headers = Seq("toponym", "count", "diffpct", "mostpop1", "pop1frac",
      "mostpop2", "pop2frac")
    outprint(format_table(headers +: rows))
    0
  }
}
