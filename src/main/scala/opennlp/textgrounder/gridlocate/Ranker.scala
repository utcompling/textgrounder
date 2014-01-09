///////////////////////////////////////////////////////////////////////////////
//  Inference.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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
package gridlocate

import scala.util.Random
import math._

import util.print.errprint
import util.debug._

import langmodel._
import learning._

/*

This file implements the various rankers used for inference of the
location of a document in a grid -- i.e. returning a ranking of the
suitability of the cells of the grid for a given document.

*/

/**
 * A ranker for ranking cells in a grid as possible matches for a given
 * document (aka "grid-locating a document").
 *
 * @tparam Co Type of document's identifying coordinate (e.g. a lat/long tuple,
 *   a year, etc.), which tends to determine the grid structure.
 * @param ranker_name Name of the ranker, for output purposes
 * @param grid Grid containing the cells over which this ranker operates
 */
abstract class GridRanker[Co](
  val ranker_name: String,
  val grid: Grid[Co]
) extends Ranker[GridDoc[Co], GridCell[Co]] {
}

/**
 * A grid ranker that does not use reranking.
 *
 * @tparam Co Type of document's identifying coordinate (e.g. a lat/long tuple,
 *   a year, etc.), which tends to determine the grid structure.
 * @param ranker_name Name of the ranker, for output purposes
 * @param grid Grid containing the cells over which this ranker operates
 */
abstract class SimpleGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends GridRanker[Co](ranker_name, grid) {
  /**
   * For a given language model (describing a test document), return
   * an Iterable of tuples, each listing a particular cell on the Earth
   * and a score of some sort.  The cells given in `include` must be
   * included in the list.  Higher scores are better.  The results should
   * be in sorted order, with better cells earlier.
   */
  def return_ranked_cells(lang_model: LangModel, correct: GridCell[Co],
      include_correct: Boolean):
    Iterable[(GridCell[Co], Double)]

  def imp_evaluate(item: GridDoc[Co], correct: GridCell[Co],
      include_correct: Boolean) =
    return_ranked_cells(item.grid_lm, correct, include_correct)
}

/**
 * Object encapsulating a GridLocate data instance to be used by the
 * classifier that underlies the ranker. This corresponds to a document
 * in the training corpus and serves as the main part of an RTI (rerank
 * training instance, see `PointwiseClassifyingRerankerTrainer`).
 */
case class GridRankerInst[Co](
  doc: GridDoc[Co],
  candidates: IndexedSeq[GridCell[Co]],
  agg: AggregateFeatureVector
) extends DataInstance {
  final def feature_vector = agg
  def pretty_print_labeled(prefix: String, correct: LabelIndex) {
    errprint(s"For instance $prefix with query doc $doc:")
    for (((cand, fv), index) <- (candidates.view zip agg.fv).zipWithIndex) {
      errprint(s"  $prefix-${index + 1}: %s: $cand: $fv",
        if (index == correct) "CORRECT" else "WRONG")
    }
  }
}

/**
 * Class that implements a very simple baseline ranker -- pick a random
 * cell.
 */

class RandomGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends SimpleGridRanker[Co](ranker_name, grid) {
  def return_ranked_cells(lang_model: LangModel, correct: GridCell[Co],
      include_correct: Boolean) = {
    val cells = grid.iter_nonempty_cells_including(correct, include_correct)
    val shuffled = (new Random()).shuffle(cells)
    (for (cell <- shuffled) yield (cell, 0.0))
  }
}

/**
 * Class that implements a simple baseline ranker -- pick the "most
 * popular" cell (the one either with the largest number of documents, or
 * the highest salience, if `salience` is true).
 */

class MostPopularGridRanker[Co] (
  ranker_name: String,
  grid: Grid[Co],
  salience: Boolean
) extends SimpleGridRanker[Co](ranker_name, grid) {
  def return_ranked_cells(lang_model: LangModel, correct: GridCell[Co],
      include_correct: Boolean) = {
    val cells = grid.iter_nonempty_cells_including(correct, include_correct)
    (for (cell <- cells) yield {
      val rank = if (salience) cell.salience else cell.num_docs
      (cell, rank.toDouble)
    }).toIndexedSeq sortWith (_._2 > _._2)
  }
}

/**
 * Abstract class that implements a ranker for grid location that
 * involves directly comparing the document language model against each cell
 * in turn and computing a score.
 */
abstract class PointwiseScoreGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends SimpleGridRanker[Co](ranker_name, grid) {
  /**
   * Function to return the score of a document language model against a
   * cell.
   */
  def score_cell(lang_model: LangModel, cell: GridCell[Co]): Double

  /**
   * Compare a language model (for a document, typically) against all
   * cells. Return a sequence of tuples (cell, score) where 'cell'
   * indicates the cell and 'score' the score.
   */
  def return_ranked_cells_serially(lang_model: LangModel, correct: GridCell[Co],
      include_correct: Boolean) = {
    for (cell <- grid.iter_nonempty_cells_including(correct, include_correct))
        yield {
      if (debug("ranking")) {
        errprint(
          "Nonempty cell at indices %s = location %s, num_documents = %s",
          cell.format_indices, cell.format_location,
          cell.num_docs)
      }
      (cell, score_cell(lang_model, cell))
    }
  }

  /**
   * Compare a language model (for a document, typically) against all
   * cells. Return a sequence of tuples (cell, score) where 'cell'
   * indicates the cell and 'score' the score.
   */
  def return_ranked_cells_parallel(lang_model: LangModel, correct: GridCell[Co],
      include_correct: Boolean) = {
    val cells = grid.iter_nonempty_cells_including(correct, include_correct)
    cells.par.map(c => (c, score_cell(lang_model, c)))
  }

  def return_ranked_cells(lang_model: LangModel, correct: GridCell[Co],
      include_correct: Boolean) = {
    val parallel = !grid.driver.params.no_parallel
    val cell_buf = {
      if (parallel)
        return_ranked_cells_parallel(lang_model, correct, include_correct)
      else
        return_ranked_cells_serially(lang_model, correct, include_correct)
    }

    val retval = cell_buf.toIndexedSeq sortWith (_._2 > _._2)

    /* If doing things parallel, this code applies for debugging
       (serial has the debugging code embedded into it). */
    if (parallel && debug("ranking")) {
      for ((cell, score) <- retval)
        errprint("Nonempty cell at indices %s = location %s, num_documents = %s, score = %s",
          cell.format_indices, cell.format_location,
          cell.num_docs, score)
    }
    retval
  }
}

/**
 * Class that implements a ranker for document geolocation by computing
 * the KL-divergence between document and cell (approximately, how much
 * the language models differ).  Note that the KL-divergence as currently
 * implemented uses the smoothed language models.
 *
 * @param partial If true (the default), only do "partial" KL-divergence.
 * This only computes the divergence involving words in the document
 * language model, rather than considering all words in the vocabulary.
 * @param symmetric If true, do a symmetric KL-divergence by computing
 * the divergence in both directions and averaging the two values.
 * (Not by default; the comparison is fundamentally asymmetric in
 * any case since it's comparing documents against cells.)
 */
class KLDivergenceGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co],
  partial: Boolean = true,
  symmetric: Boolean = false
) extends PointwiseScoreGridRanker[Co](ranker_name, grid) {

  var self_kl_cache: KLDivergenceCache = null
  val slow = false

  def call_kl_divergence(self: LangModel, other: LangModel) =
    self.kl_divergence(other, partial = partial, cache = self_kl_cache)

  def score_cell(lang_model: LangModel, cell: GridCell[Co]) = {
    val cell_lang_model = cell.grid_lm
    var kldiv = call_kl_divergence(lang_model, cell_lang_model)
    if (symmetric) {
      val kldiv2 = cell_lang_model.kl_divergence(lang_model,
        partial = partial)
      kldiv = (kldiv + kldiv2) / 2.0
    }
    // Negate so that higher scores are better
    -kldiv
  }

  override def return_ranked_cells(lang_model: LangModel,
      correct: GridCell[Co], include_correct: Boolean) = {
    // This will be used by `score_cell` above.
    self_kl_cache = lang_model.get_kl_divergence_cache()

    val cells = super.return_ranked_cells(lang_model, correct, include_correct)

    if (debug("kldiv") && lang_model.isInstanceOf[FastSlowKLDivergence]) {
      val fast_slow_dist = lang_model.asInstanceOf[FastSlowKLDivergence]
      // Print out the words that contribute most to the KL divergence, for
      // the top-ranked cells
      errprint("")
      errprint("KL-divergence debugging info:")
      for (((cell, _), i) <- cells.take(
           GridLocateConstants.kldiv_num_contrib_cells) zipWithIndex) {
        val (_, contribs) =
          fast_slow_dist.slow_kl_divergence_debug(
            cell.grid_lm, partial = partial,
            return_contributing_words = true)
        errprint("  At rank #%s, cell %s:", i + 1, cell)
        errprint("    %30s  %s", "Word", "KL-div contribution")
        errprint("    %s", "-" * 50)
        // sort by absolute value of second element of tuple, in reverse order
        val grams =
          (contribs.toIndexedSeq sortWith ((x, y) => abs(x._2) > abs(y._2))).
            take(GridLocateConstants.kldiv_num_contrib_words)
        for ((word, contribval) <- grams)
          errprint("    %30s  %s", word, contribval)
        errprint("")
      }
    }

    cells
  }
}

/**
 * Class that implements a ranker for document geolocation by computing
 * the cosine similarity between the language models of document and cell.
 * FIXME: We really should transform the language model counts by TF/IDF
 * before doing this.
 *
 * @param smoothed If true, use the smoothed language models. (By default,
 * use unsmoothed language models.)
 * @param partial If true, only do "partial" cosine similarity.
 * This only computes the similarity involving words in the document
 * language model, rather than considering all words in the vocabulary.
 */
class CosineSimilarityGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co],
  smoothed: Boolean = false,
  partial: Boolean = true
) extends PointwiseScoreGridRanker[Co](ranker_name, grid) {

  def score_cell(lang_model: LangModel, cell: GridCell[Co]) = {
    val cossim =
      lang_model.cosine_similarity(cell.grid_lm,
        partial = partial, smoothed = smoothed)
    assert(cossim >= 0.0)
    // Just in case of round-off problems
    assert(cossim <= 1.002)
    cossim
  }
}

/** Use a Naive Bayes ranker for comparing document and cell. */
class NaiveBayesGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co],
  use_baseline: Boolean = true
) extends PointwiseScoreGridRanker[Co](ranker_name, grid) {

  def score_cell(lang_model: LangModel, cell: GridCell[Co]) = {
    val params = grid.driver.params
    // Determine respective weightings
    val (word_weight, baseline_weight) = (
      if (use_baseline) {
        if (params.naive_bayes_weighting == "equal") (1.0, 1.0)
        else {
          val bw = params.naive_bayes_baseline_weight.toDouble
          ((1.0 - bw) / lang_model.num_tokens, bw)
        }
      } else (1.0, 0.0))

    val gram_logprob =
      cell.grid_lm.model_logprob(lang_model)
    val baseline_logprob =
      log(cell.num_docs.toDouble /
          grid.total_num_docs)
    val logprob = (word_weight * gram_logprob +
      baseline_weight * baseline_logprob)
    logprob
  }
}

class AverageCellProbabilityGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends SimpleGridRanker[Co](ranker_name, grid) {
  def create_cell_dist_factory = new CellDistFactory[Co]

  val cdist_factory = create_cell_dist_factory

  def return_ranked_cells(lang_model: LangModel, correct: GridCell[Co],
      include_correct: Boolean) = {
    val celldist =
      cdist_factory.get_cell_dist_for_lang_model(grid, lang_model)
    celldist.get_ranked_cells(correct, include_correct)
  }
}

/////////////////////////////////////////////////////////////////////////////
//                                Segmentation                             //
/////////////////////////////////////////////////////////////////////////////

// General idea: Keep track of best possible segmentations up to a maximum
// number of segments.  Either do it using a maximum number of segmentations
// (e.g. 100 or 1000) or all within a given factor of the best score (the
// "beam width", e.g. 10^-4).  Then given the existing best segmentations,
// we search for new segmentations with more segments by looking at all
// possible ways of segmenting each of the existing best segments, and
// finding the best score for each of these.  This is a slow process -- for
// each segmentation, we have to iterate over all segments, and for each
// segment we have to look at all possible ways of splitting it, and for
// each split we have to look at all assignments of cells to the two
// new segments.  It also seems that we're likely to consider the same
// segmentation multiple times.
//
// In the case of per-word cell dists, we can maybe speed things up by
// computing the non-normalized distributions over each paragraph and then
// summing them up as necessary.
