///////////////////////////////////////////////////////////////////////////////
//  FilterWords.scala
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
package geolocate

import math.log

import util.argparser._
import util.collection.combine_maps
import util.experiment._
import util.print.errprint
import util.textdb._

import gridlocate._

import langmodel._

class FilterWordsParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
  var output =
    ap.option[String]("o", "output",
      metavar = "PREFIX",
      must = be_specified,
      help = """File prefix of written-out counts.  Counts are stored as
textdb corpora, i.e. for each word, two files will be written, named
`PREFIX.data.txt` and `PREFIX.schema.txt`, with the former storing the data
as tab-separated fields and the latter naming the fields.""")

  var filter_below_count =
    ap.option[Int]("filter-below-count", "fbc",
      metavar = "INT",
      default = 10,
      must = be_>(0),
      help = """Filter words whose total count is less than the specified
amount. Default %default.""")

  var filter_non_alpha =
    ap.flag("filter-non-alpha",
      help = """Filter out words containing non-alphabetic characters.""")

  var sort =
    ap.option[String]("sort",
      choices = Seq("word", "wordcount", "cellcount"),
      default = "wordcount",
      help = """Sort the entries by the given field. Allowed are %choices.
Default %default.""")
}

class FilterWordsDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = FilterWordsParameters
  type TRunRes = Unit

  /**
   * Do the actual computation.
   */

  def run() {
    val grid = initialize_grid
    errprint("Computing set of words seen ...")
    val cells = grid.iter_nonempty_cells

    val all_alpha = """^[A-Za-z]+$""".r
    // Compute the total word count across all cells for each word.
    // Filter words with too small word count, and maybe filter
    // non-alphabetic words.
    val words_counts_1 = cells.map { cell =>
      val lm = cell.grid_lm
      lm.iter_grams.toMap
    }.reduce[Map[Gram,GramCount]](combine_maps _)
    errprint(s"Total number of words before filtering: ${words_counts_1.size}")
    val words_counts_2 =
      words_counts_1.filter { _._2 >= params.filter_below_count }
    errprint(s"Total number of words after filtering by count: ${words_counts_2.size}")
    val words_counts = if (!params.filter_non_alpha) words_counts_1 else
      words_counts_1.filter {
        _._1 match {
          case all_alpha() => true
          case _ => false
        }
      }
    errprint(s"Total number of words after filtering by alpha: ${words_counts.size}")
    errprint(s"Total number of non-empty cells: ${cells.size}")
    // Compute the number of cells each word occurs in.
    val words_cellcounts = cells.map { cell =>
      val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
      lm.iter_grams.map {
        case (word, count) => (word, 1)
      }.toMap
    }.reduce[Map[Gram,Int]](combine_maps _)

    // For each word, compute information gain and entropy. Return a tuple of
    // (word, wordcount, cellcount, inf-gain, gain-ratio, entropy)
    // // and entropy normed various ways).
    val words_counts_cellcounts =
      (for ((word, count) <- words_counts) yield {
        val cellcount = words_cellcounts(word)
        (word, count, cellcount)
      }).toSeq
    val sorted_words_counts_cellcounts = params.sort match {
      case "word" => words_counts_cellcounts sortBy { _._1 }
      case "wordcount" => words_counts_cellcounts sortBy { -_._2 }
      case "cellcount" => words_counts_cellcounts sortBy { -_._3 }
    }
    val props = sorted_words_counts_cellcounts map {
      case (word, wordcount, cellcount) => Seq(
        "word" -> Unigram.to_raw(word),
        "wordcount" -> wordcount,
        "cellcount" -> cellcount
        )
    }
    note_result("textdb-type", "wordcount")
    write_constructed_textdb_with_results(util.io.localfh, params.output,
      props.iterator)
  }
}

object FilterWords extends GeolocateApp("FilterWords") {
  type TDriver = FilterWordsDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

