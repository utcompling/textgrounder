///////////////////////////////////////////////////////////////////////////////
//  ComputeEntropy.scala
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
package geolocate

import util.argparser._
import util.collection.{combine_double_maps, combine_int_maps}
import util.experiment._
import util.print.errprint
import util.textdb._
 
import gridlocate._

import langmodel._
import LangModel._

class ComputeEntropyParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
  var output =
    ap.option[String]("o", "output",
      metavar = "PREFIX",
      help = """File prefix of written-out distributions.  Distributions are
stored as textdb corpora, i.e. for each word, two files will be written, named
`PREFIX.data.txt` and `PREFIX.schema.txt`, with the former storing the data
as tab-separated fields and the latter naming the fields.""")

  var entropy_minimum_word_count =
    ap.option[Int]("entropy-minimum-word-count", "emwc",
      metavar = "INT",
      default = 10,
      help = """Minimum total count of a word in order for it to be included
in the list of words for which the entropy is computed.""")

  var include_normed_entropy =
    ap.flag("include-normed-entropy", "ine",
      help = """Include fields containing the entropy normed by the total
number of word tokens and total number of cells containing the word.""")

  var verbose =
    ap.flag("verbose",
      help = """Output word counts and such as we compute the entropy.""")
}

class ComputeEntropyDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = ComputeEntropyParameters
  type TRunRes = Unit

  override def handle_parameters() {
    super.handle_parameters()
    need(params.output, "output")
  }

  /**
   * Do the actual computation.
   */

  def run() {
    val grid = initialize_grid
    errprint("Computing set of words seen ...")
    val cells = grid.iter_nonempty_cells
    val words_counts = cells.map { cell =>
      val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
      lm.model.iter_items.toMap
    }.reduce[Map[Word,Double]](combine_double_maps _).
    filter { _._2 >= params.entropy_minimum_word_count }
    val words_cellcounts = cells.map { cell =>
      val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
      lm.model.iter_items.map {
        case (word, count) => (word, 1)
      }.toMap
    }.reduce[Map[Word,Int]](combine_int_maps _)
    if (params.verbose) {
      for ((word, count) <- words_counts)
        errprint("Word: %s (%s) = %s / %s", word, memoizer.unmemoize(word), count,
          words_cellcounts(word))
    }
    errprint("Computing set of words seen ... done.")
    errprint("Computing entropies ...")
    val entropies = for ((word, count) <- words_counts) yield {
      val probs = cells.map { cell =>
        val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
        // lm.lookup_word(word)
        lm.mle_word_prob(word)
      }.filter(_ != 0.0)
      val totalprob = probs.sum
      val normprob = probs.map { _ / totalprob }
      val entropy = normprob.map { prob =>
        - prob * math.log(prob)
      }.sum
      val cellcount = words_cellcounts(word)
      (word, count, cellcount, entropy, entropy/count, entropy/cellcount)
    }
    val sorted_entropies = entropies.toSeq sortBy { _._4 }
    val props = sorted_entropies map {
      case (word, count, cellcount, entropy, normed_wordcount,
          normed_cellcount) => Seq(
        "word" -> memoizer.unmemoize(word),
        "wordcount" -> count,
        "cellcount" -> cellcount,
        "entropy" -> entropy
        ) ++ (if (params.include_normed_entropy) Seq(
        "normed-entropy-by-wordcount" -> normed_wordcount,
        "normed-entropy-by-cellcount" -> normed_cellcount
        ) else Seq[(String, Any)]()
      )
    }
    errprint("Computing entropies ... done.")
    note_result("textdb-type", "word-entropies")
    write_textdb_values_with_results(util.io.localfh, params.output,
      props.iterator)
  }
}

object ComputeEntropy extends GeolocateApp("ComputeEntropy") {
  type TDriver = ComputeEntropyDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

