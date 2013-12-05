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
in the list of words for which the entropy is computed.  Default %default.""")

  var smoothed =
    ap.flag("smoothed",
      help = """Use smoothed probabilities when computing entropy. Default is
to use unsmoothed (maximum-likelihood) probabilities.""")

  var omit_normed_entropy =
    ap.flag("omit-normed-entropy", "one",
      help = """Do not include fields containing the entropy normed by the
total number of word tokens and total number of cells containing the word
and the logarithms of these two values.""")

  var sort =
    ap.option[String]("sort",
      choices = Seq("entropy", "normed-log-wordcount", "normed-log-cellcount",
        "normed-wordcount", "normed-cellcount"),
      default = "entropy",
      help = """Sort the entries by the given field, from lowest to highest.
Allowed are %choices. Default %default.""")

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

    // Compute the total word count across all cells for each word.
    // Filter words with too small word count.
    val words_counts = cells.map { cell =>
      val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
      lm.model.iter_grams.toMap
    }.reduce[Map[Gram,Double]](combine_double_maps _).
    filter { _._2 >= params.entropy_minimum_word_count }

    // Compute the number of cells each word occurs in.
    val words_cellcounts = cells.map { cell =>
      val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
      lm.model.iter_grams.map {
        case (word, count) => (word, 1)
      }.toMap
    }.reduce[Map[Gram,Int]](combine_int_maps _)

    // Maybe print word and cell counts.
    if (params.verbose) {
      for ((word, count) <- words_counts)
        errprint("Gram: %s (%s) = %s / %s", word, Unigram.unmemoize(word),
          count, words_cellcounts(word))
    }
    errprint("Computing set of words seen ... done.")

    errprint("Computing entropies ...")

    // For each word, compute entropy. Return a tuple of
    // (word, wordcount, cellcount, entropy, and entropy normed various ways).
    val entropies = for ((word, wordcount) <- words_counts) yield {
      // Compute p(word|cell) for each cell. Filter out 0's as they will
      // cause problems otherwise when we take the log. (By definition, such
      // 0's are ignored when computing entropy.)
      val probs = cells.map { cell =>
        val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
        if (params.smoothed)
          lm.gram_prob(word)
        else
          lm.mle_gram_prob(word)
      }.filter(_ != 0.0)
      val totalprob = probs.sum
      // Normalize probabilities to get a distribution p(cell|word).
      // NOTE NOTE NOTE: This is only mathematically correct if we assume that
      // all cells have the same prior probability.
      val normprob = probs.map { _ / totalprob }

      // Compute entropy by standard formula.
      val entropy = normprob.map { prob =>
        - prob * math.log(prob)
      }.sum

      // Compute various kinds of "normed entropy". As the number of word
      // tokens increases, it's observed that the entropy will tend to
      // increase as well. This makes sense since the words will tend to be
      // distributed over a larger number of cells. For a discrete distribution
      // the maximal-entropy distribution is a uniform distribution, and for a
      // uniform distribution with N choices, its entropy is log N. If we want
      // to factor out the effect of number of word tokens, it seems we want
      // to figure out the expected number of non-zero cells in a random
      // distribution containing the given number of word tokens, and divide
      // by the log of that number. Perhaps log(wordcount) is a good enough
      // proxy?
      val cellcount = words_cellcounts(word)
      assert(cellcount > 0)
      assert(wordcount > 0)
      val e_normed_log_wordcount =
        if (wordcount == 1) 0.0
        else entropy / math.log(wordcount)
      val e_normed_log_cellcount =
        if (cellcount == 1) 0.0
        else entropy / math.log(cellcount)
      (word, wordcount, cellcount, entropy, e_normed_log_wordcount,
        e_normed_log_cellcount, entropy/wordcount, entropy/cellcount)
    }
    val seqent = entropies.toSeq
    val sorted_entropies = params.sort match {
      case "entropy" => seqent sortBy { _._4 }
      case "normed-log-wordcount" => seqent sortBy { _._5 }
      case "normed-log-cellcount" => seqent sortBy { _._6 }
      case "normed-wordcount" => seqent sortBy { _._7 }
      case "normed-cellcount" => seqent sortBy { _._8 }
    }
    val props = sorted_entropies map {
      case (word, wordcount, cellcount, entropy,
          normed_log_wordcount, normed_log_cellcount,
          normed_wordcount, normed_cellcount
        ) => Seq(
        "word" -> Unigram.unmemoize(word),
        "wordcount" -> wordcount,
        "cellcount" -> cellcount,
        "entropy" -> entropy
        ) ++ (if (!params.omit_normed_entropy) Seq(
        "normed-entropy-by-log-wordcount" -> normed_log_wordcount,
        "normed-entropy-by-log-cellcount" -> normed_log_cellcount,
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

