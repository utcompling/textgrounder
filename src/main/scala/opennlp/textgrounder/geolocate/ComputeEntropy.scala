///////////////////////////////////////////////////////////////////////////////
//  ComputeEntropy.scala
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

class ComputeEntropyParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
  var output =
    ap.option[String]("o", "output",
      metavar = "PREFIX",
      must = be_specified,
      help = """File prefix of written-out distributions.  Distributions are
stored as textdb corpora, i.e. for each word, two files will be written, named
`PREFIX.data.txt` and `PREFIX.schema.txt`, with the former storing the data
as tab-separated fields and the latter naming the fields.""")

  var entropy_minimum_word_count =
    ap.option[Int]("entropy-minimum-word-count", "emwc",
      metavar = "INT",
      default = 10,
      must = be_>(0),
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
      choices = Seq("inf-gain", "gain-ratio", "entropy"
        //, "normed-log-wordcount", "normed-log-cellcount",
        //"normed-wordcount", "normed-cellcount"
      ),
      default = "entropy",
      help = """Sort the entries by the given field, from lowest to highest.
Allowed are %choices. Default %default.""")

  var output_counts =
    ap.flag("output-counts",
      help = """Output word counts and such as we compute the entropy.""")
}

class ComputeEntropyDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = ComputeEntropyParameters
  type TRunRes = Unit

  /**
   * Do the actual computation.
   */

  def run() {
    val grid = initialize_grid
    errprint("Computing set of words seen ...")
    val cells = grid.iter_nonempty_cells

    // How to compute p(cell|word). If true, use counts directly, just as
    // p(word|cell) is computed. If false, compute form p(word|cell) by
    // Bayes' Law.
    val pcell_word_by_count = true

    // Compute the total word count across all cells for each word.
    // Filter words with too small word count.
    val words_counts = cells.map { cell =>
      val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
      lm.iter_grams.toMap
    }.reduce[Map[Gram,Double]](combine_maps _).
    filter { _._2 >= params.entropy_minimum_word_count }
    val total_wordcount = words_counts.values.sum

    // Compute the number of cells each word occurs in.
    val words_cellcounts = cells.map { cell =>
      val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
      lm.iter_grams.map {
        case (word, count) => (word, 1)
      }.toMap
    }.reduce[Map[Gram,Int]](combine_maps _)

    // Maybe print word and cell counts.
    if (params.output_counts) {
      for ((word, count) <- words_counts)
        errprint("Gram: %s (%s) = %s / %s", word, Unigram.to_raw(word),
          count, words_cellcounts(word))
    }
    errprint("Computing set of words seen ... done.")

    errprint("Computing entropies ...")

    // For each word, compute information gain and entropy. Return a tuple of
    // (word, wordcount, cellcount, inf-gain, gain-ratio, entropy)
    // // and entropy normed various ways).
    val entropies = for ((word, count) <- words_counts) yield {
      val cellcount = words_cellcounts(word)
      assert(cellcount > 0)
      assert(count > 0)

      // Compute unnormalized version of p(cell|word) for each cell.
      // If pcell_word_by_count = true, compute this by normalizing the
      // count of occurrences of word in cell, as we would for p(word|cell).
      // If false, use Bayes' Law:
      //   p(cell|word) = p(word|cell) * p(cell) / p(word), i.e.
      //   p(cell|word) \propto p(word|cell) * p(cell)
      // Assuming uniform prior distribution of p(cell), we have
      //   p(cell|word) \propto p(word|cell)
      val raw_pcell_word = cells.map { cell =>
        val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
        if (pcell_word_by_count)
          lm.get_gram(word).toDouble
        else if (params.smoothed)
          lm.gram_prob(word)
        else
          lm.mle_gram_prob(word)
      }
      val sum_raw_pcell_word = raw_pcell_word.sum
      // Normalize probabilities to get a distribution p(cell|word).
      val pcell_word = raw_pcell_word.map { _ / sum_raw_pcell_word }

      // Repeat to compute distribution p(cell|word-bar) for all words in
      // a cell other than the given one.
      val raw_pcell_word_bar = cells.map { cell =>
        val lm = Unigram.check_unigram_lang_model(cell.grid_lm)
        if (pcell_word_by_count)
          lm.num_tokens.toDouble - lm.get_gram(word)
        else if (params.smoothed)
          1.0 - lm.gram_prob(word)
        else
          1.0 - lm.mle_gram_prob(word)
      }
      val sum_raw_pcell_word_bar = raw_pcell_word_bar.sum
      val pcell_word_bar = raw_pcell_word_bar.map { _ / sum_raw_pcell_word_bar }

      // Compute overall p(word) across all cells, and p(word-bar) for
      // all remaining words.
      val pword = count.toDouble / total_wordcount
      val pword_bar = 1.0 - pword

      // Compute parts of the information-gain formula
      // (see Han Cook Baldwin 2014).
      val igw = pcell_word.filter(_ != 0.0).map { p => p * log(p) }.sum
      val igwbar = pcell_word_bar.filter(_ != 0.0).map { p => p * log(p) }.sum
      // Compute conditional entropy H(c|w) across all cities -- the portion
      // of information gain that changes from one word to the next.
      val ig = pword * igw + pword_bar * igwbar
      // Compute intrinsic entropy
      val iv = - pword * log(pword) - pword_bar * log(pword_bar)
      // Compute information gain ratio
      val igr = ig / iv

      val entropy = iv
      // Compute entropy by standard formula??? This is how I used to compute
      // entropy, but not the same as HCB's formula. It's rather the entropy
      // of p(cell|word), I think.
      // val entropy = -igw

      //// Compute various kinds of "normed entropy". As the number of word
      //// tokens increases, it's observed that the entropy will tend to
      //// increase as well. This makes sense since the words will tend to be
      //// distributed over a larger number of cells. For a discrete distribution
      //// the maximal-entropy distribution is a uniform distribution, and for a
      //// uniform distribution with N choices, its entropy is log N. If we want
      //// to factor out the effect of number of word tokens, it seems we want
      //// to figure out the expected number of non-zero cells in a random
      //// distribution containing the given number of word tokens, and divide
      //// by the log of that number. Perhaps log(wordcount) is a good enough
      //// proxy?
      //val e_normed_log_wordcount =
      //  if (wordcount == 1) 0.0
      //  else entropy / log(wordcount)
      //val e_normed_log_cellcount =
      //  if (cellcount == 1) 0.0
      //  else entropy / log(cellcount)
      (word, count, cellcount, ig, igr, entropy
        // e_normed_log_wordcount, e_normed_log_cellcount,
        // entropy/wordcount, entropy/cellcount
      )
    }
    val seqent = entropies.toSeq
    val sorted_entropies = params.sort match {
      case "inf-gain" => seqent sortBy { _._4 }
      case "gain-ratio" => seqent sortBy { _._5 }
      case "entropy" => seqent sortBy { _._6 }
//      case "normed-log-wordcount" => seqent sortBy { _._7 }
//      case "normed-log-cellcount" => seqent sortBy { _._8 }
//      case "normed-wordcount" => seqent sortBy { _._9 }
//      case "normed-cellcount" => seqent sortBy { _._10 }
    }
    val props = sorted_entropies map {
      case (word, wordcount, cellcount, inf_gain, gain_ratio, entropy
          // , normed_log_wordcount, normed_log_cellcount,
          // normed_wordcount, normed_cellcount
        ) => Seq(
        "word" -> Unigram.to_raw(word),
        "wordcount" -> wordcount,
        "cellcount" -> cellcount,
        "inf-gain" -> inf_gain,
        "gain-ratio" -> gain_ratio,
        "entropy" -> entropy
        )
//        ++ (if (!params.omit_normed_entropy) Seq(
//        "normed-entropy-by-log-wordcount" -> normed_log_wordcount,
//        "normed-entropy-by-log-cellcount" -> normed_log_cellcount,
//        "normed-entropy-by-wordcount" -> normed_wordcount,
//        "normed-entropy-by-cellcount" -> normed_cellcount
//        ) else Seq[(String, Any)]()
//        )
    }
    errprint("Computing entropies ... done.")
    note_result("textdb-type", "word-entropies")
    write_constructed_textdb_with_results(util.io.localfh, params.output,
      props.iterator)
  }
}

object ComputeEntropy extends GeolocateApp("ComputeEntropy") {
  type TDriver = ComputeEntropyDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

