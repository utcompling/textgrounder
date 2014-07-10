///////////////////////////////////////////////////////////////////////////////
//  ComputeWordProps.scala
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
import util.debug._
import util.error._
import util.experiment._
import util.print.errprint
import util.textdb._

import gridlocate._

import langmodel._

class ComputeWordPropsParameters(
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

  var filter_below_count =
    ap.option[Int]("filter-below-count", "fbc",
      metavar = "INT",
      default = 10,
      must = be_>(0),
      help = """Filter words whose total count is less than the specified
amount. Default %default.""")

  var filter_below_length =
    ap.option[Int]("filter-below-length", "fbl",
      metavar = "INT",
      default = 3,
      must = be_>(0),
      help = """Filter words whose total length is less than the specified
amount. Default %default.""")

  var filter_non_alpha =
    ap.flag("filter-non-alpha",
      help = """Filter out words containing non-alphabetic characters.""")

  var smoothed =
    ap.flag("smoothed",
      help = """Use smoothed probabilities when computing entropy. Default is
to use unsmoothed (maximum-likelihood) probabilities.""")

  var entropy =
    ap.flag("entropy",
      help = """Compute and include entropy, information gain and information
gain ratio.""")

  var normed_entropy =
    ap.flag("normed-entropy", "ne",
      help = """Compute and include fields containing the entropy normed by the
total number of word tokens and total number of cells containing the word
and the logarithms of these two values.""")

  var sort =
    ap.option[String]("sort",
      choices = Seq("word", "wordcount", "cellcount",
        "inf-gain", "gain-ratio", "entropy"
        //, "normed-log-wordcount", "normed-log-cellcount",
        //"normed-wordcount", "normed-cellcount"
      ),
      default = "word",
      help = """Sort the entries by the given field, from lowest to highest.
Allowed are %choices. Default %default.""")

  var output_counts =
    ap.flag("output-counts",
      help = """Output word counts and such as we compute the entropy.""")
}

case class WordProps(word: String, wordcount: GramCount, cellcount: Int,
  inf_gain: Double = 0.0)

class ComputeWordPropsDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = ComputeWordPropsParameters
  type TRunRes = Unit

  /**
   * Do the actual computation.
   */

  def run() {
    val grid = initialize_grid
    errprint("Computing set of words seen ...")
    val cells = grid.iter_nonempty_cells
    val first_lm = cells.head.grid_lm

    // How to compute p(cell|word). If true, use counts directly, just as
    // p(word|cell) is computed. If false, compute form p(word|cell) by
    // Bayes' Law.
    val pcell_word_by_count = true

    val all_alpha = """^[A-Za-z]+$""".r
    // Compute the total word count across all cells for each word.
    // Filter words with too small word count, and maybe filter
    // non-alphabetic words.
    //
    // NOTE: This seems to be buggy in some circumstances.
    // It seems to fail when run on Cophir with K-d trees at bucket sizes
    // <= 100. Unclear why no failure in other circumstances.
    val words_counts_1 = cells.map { cell =>
      cell.grid_lm.iter_grams.toMap
    }.reduce[Map[Gram,GramCount]](combine_maps _)
    errprint(s"Total number of non-empty cells: ${cells.size}")
    errprint(s"Total number of words (types) before filtering: ${words_counts_1.size}")
    val total_wordcount_before_filtering = words_counts_1.values.sum
    errprint(s"Total count of all words (tokens) before filtering: $total_wordcount_before_filtering")
    val words_counts_2 =
      words_counts_1
      .filter { _._2 >= params.filter_below_count }
      .filter { case (word, count) =>
        first_lm.gram_to_string(word).size >= params.filter_below_length }
    errprint(s"Total number of words (types) after filtering by count and length: ${words_counts_2.size}")
    val words_counts = if (!params.filter_non_alpha) words_counts_2 else
      words_counts_2.filter { case (word, count) =>
        first_lm.gram_to_string(word) match {
          case all_alpha() => true
          case _ => false
        }
      }
    errprint(s"Total number of words (types) after filtering by alpha: ${words_counts.size}")
    val total_wordcount = words_counts.values.sum
    errprint(s"Total count of all words (tokens) after filtering: $total_wordcount")
    val word_set = words_counts.keys.toSet

    if (debug("compute-word-props")) {
      val parallel_cells = cells.par
      val words_counts_redone =
        word_set.toSeq.map { word =>
          val count = parallel_cells.map { cell => cell.grid_lm.get_gram(word) }.sum
          if (words_counts(word) != count)
            errprint("For word %s (%s), %s should == %s",
              word, first_lm.gram_to_string(word), words_counts(word), count)
          (word, count)
        }.toMap
      errprint(s"${words_counts_redone.size} should == ${words_counts.size}")
      errprint(s"${words_counts_redone.values.sum} should == ${words_counts.values.sum}")
      errprint(s"${words_counts_redone.map(_._2).sum} should == ${words_counts.values.sum}")
    }

    // Compute the total cell entropy.

    // 1. Compute the count of all tokens in each cell.
    val cells_counts = cells.map { cell =>
      val total =
        cell.grid_lm.iter_grams.filter(word_set contains _._1).map(_._2).sum
      (cell, total)
    }.toMap
    val total_wordcount_2 = cells_counts.map(_._2).sum
    assert_==(total_wordcount, total_wordcount_2)
    // 2. Normalize to get a distribution over cells.
    val cells_probs = cells_counts.map { case (cell, count) =>
      (cell, count.toDouble / total_wordcount)
    }
    // 3. Compute cell entropy.
    val cell_entropy = - (cells_probs.map(_._2).filter(_ != 0.0).
      map { p => p * log(p) }.sum)

    // Compute the number of cells each word occurs in.
    val words_cellcounts = cells.map { cell =>
      cell.grid_lm.iter_grams.map {
        case (word, count) => (word, 1)
      }.toMap
    }.reduce[Map[Gram,Int]](combine_maps _)
    errprint("Computing set of words seen ... done.")

    // Maybe print word and cell counts.
    if (params.output_counts) {
      for ((word, count) <- words_counts)
        errprint("Gram: %s (%s) = %s / %s", word, first_lm.gram_to_string(word),
          count, words_cellcounts(word))
    }

    if (params.entropy)
      errprint("Computing entropies ...")

    // For each word, compute information gain and entropy. Return a tuple of
    // (word, wordcount, cellcount, entropy-props) where entropy-props is an
    // IndexedSeq of inf-gain, gain-ratio, entropy, and entropy normed
    // various ways.
    val props = words_counts.par.map { case (word, wordcount) =>
      val cellcount = words_cellcounts(word)
      assert_>(cellcount, 0)
      assert_>(wordcount, 0.0)
      val entropy_props = if (params.entropy) {
        // Compute unnormalized version of p(cell|word) for each cell.
        // If pcell_word_by_count = true, compute this by normalizing the
        // count of occurrences of word in cell, as we would for p(word|cell).
        // If false, use Bayes' Law:
        //   p(cell|word) = p(word|cell) * p(cell) / p(word), i.e.
        //   p(cell|word) \propto p(word|cell) * p(cell)
        // Assuming uniform prior distribution of p(cell), we have
        //   p(cell|word) \propto p(word|cell)
        val raw_pcell_word = cells.map { cell =>
          val lm = cell.grid_lm
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
          val lm = cell.grid_lm
          if (pcell_word_by_count)
            cells_counts(cell) - lm.get_gram(word)
          else if (params.smoothed)
            1.0 - lm.gram_prob(word)
          else
            1.0 - lm.mle_gram_prob(word)
        }
        val sum_raw_pcell_word_bar = raw_pcell_word_bar.sum
        val pcell_word_bar = raw_pcell_word_bar.map { _ / sum_raw_pcell_word_bar }

        // Compute overall p(word) across all cells, and p(word-bar) for
        // all remaining words.
        val pword = wordcount.toDouble / total_wordcount
        val pword_bar = 1.0 - pword

        // Compute parts of the information-gain formula
        // (see Han Cook Baldwin 2014).
        val igw = pcell_word.filter(_ != 0.0).map { p => p * log(p) }.sum
        val igwbar = pcell_word_bar.filter(_ != 0.0).map { p => p * log(p) }.sum
        // Compute information gain from IG = H(c) - H(c|w) for
        // H(c) = cell entropy
        // H(c|w) = conditional cell entropy given a word
        val conditional_cell_entropy = - pword * igw - pword_bar * igwbar
        val ig = cell_entropy - conditional_cell_entropy
        //errprint("For word %s: H(c) = %s, H(c|w) = %s, IG = %s",
        //  first_lm.gram_to_string(word), cell_entropy,
        //  conditional_cell_entropy, ig)
        // Compute intrinsic entropy
        val iv = - pword * log(pword) - pword_bar * log(pword_bar)
        // Compute information gain ratio
        val igr = ig / iv

        val entropy = iv
        // Compute entropy by standard formula??? This is how I used to compute
        // entropy, but not the same as HCB's formula. It's rather the entropy
        // of p(cell|word), I think.
        // val entropy = -igw

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
        val e_normed_log_wordcount =
          if (wordcount == 1) 0.0
          else entropy / log(wordcount)
        val e_normed_log_cellcount =
          if (cellcount == 1) 0.0
          else entropy / log(cellcount)
        IndexedSeq(ig, igr, entropy,
          e_normed_log_wordcount, e_normed_log_cellcount,
          entropy/wordcount, entropy/cellcount)
      } else IndexedSeq()
      (first_lm.gram_to_string(word), wordcount, cellcount, entropy_props)
    }
    if (params.entropy)
      errprint("Computing entropies ... done.")
    val seqent = props.seq.toSeq
    val sorted_props = params.sort match {
      case "word" => seqent sortBy { _._1 }
      case "wordcount" => seqent sortBy { -_._2 }
      case "cellcount" => seqent sortBy { -_._3 }
      case "inf-gain" => seqent sortBy { -_._4(0) }
      case "gain-ratio" => seqent sortBy { -_._4(1) }
      case "entropy" => seqent sortBy { -_._4(2) }
      case "normed-log-wordcount" => seqent sortBy { -_._4(3) }
      case "normed-log-cellcount" => seqent sortBy { -_._4(4) }
      case "normed-wordcount" => seqent sortBy { -_._4(5) }
      case "normed-cellcount" => seqent sortBy { -_._4(6) }
    }
    val row_props = sorted_props map {
      case (word, wordcount, cellcount, entropy_props) =>
        val basic_props = Seq(
          "word" -> word,
          "wordcount" -> wordcount,
          "cellcount" -> cellcount
        )
        val row_entropy_props = if (params.entropy) {
          val IndexedSeq(inf_gain, gain_ratio, entropy,
            normed_log_wordcount, normed_log_cellcount,
            normed_wordcount, normed_cellcount) =
              entropy_props
          Seq(
            "inf-gain" -> inf_gain,
            "gain-ratio" -> gain_ratio,
            "entropy" -> entropy
          ) ++ (if (params.normed_entropy) Seq(
            "normed-entropy-by-log-wordcount" -> normed_log_wordcount,
            "normed-entropy-by-log-cellcount" -> normed_log_cellcount,
            "normed-entropy-by-wordcount" -> normed_wordcount,
            "normed-entropy-by-cellcount" -> normed_cellcount
            ) else Seq()
          )
        } else Seq()
        basic_props ++ row_entropy_props
    }
    note_result("textdb-type", "word-entropies")
    write_constructed_textdb_with_results(util.io.localfh, params.output,
      row_props.iterator)
  }
}

object ComputeWordProps extends GeolocateApp("ComputeWordProps") {
  type TDriver = ComputeWordPropsDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

