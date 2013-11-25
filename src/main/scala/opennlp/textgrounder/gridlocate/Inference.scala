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

/*

This file implements the various rankers used for inference of the
location of a document in a grid -- i.e. returning a ranking of the
suitability of the cells of the grid for a given document.

*/

/**
 * Class that implements a very simple baseline ranker -- pick a random
 * cell.
 */

class RandomGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends GridRanker[Co](ranker_name, grid) {
  def return_ranked_cells(lang_model: LangModel,
      include: Iterable[GridCell[Co]]) = {
    val cells = grid.iter_nonempty_cells_including(include)
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
) extends GridRanker[Co](ranker_name, grid) {
  def return_ranked_cells(lang_model: LangModel, include: Iterable[GridCell[Co]]) = {
    (for (cell <-
        grid.iter_nonempty_cells_including(include))
      yield (cell,
        (if (salience)
           cell.salience
         else
           cell.num_docs).toDouble)).
    toIndexedSeq sortWith (_._2 > _._2)
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
) extends GridRanker[Co](ranker_name, grid) {
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
  def return_ranked_cells_serially(lang_model: LangModel,
    include: Iterable[GridCell[Co]]) = {
      for (cell <- grid.iter_nonempty_cells_including(include)) yield {
        if (debug("lots")) {
          errprint("Nonempty cell at indices %s = location %s, num_documents = %s",
            cell.describe_indices, cell.describe_location,
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
  def return_ranked_cells_parallel(lang_model: LangModel,
    include: Iterable[GridCell[Co]]) = {
    val cells = grid.iter_nonempty_cells_including(include)
    cells.par.map(c => (c, score_cell(lang_model, c)))
  }

  def return_ranked_cells(lang_model: LangModel, include: Iterable[GridCell[Co]]) = {
    val parallel = !grid.driver.params.no_parallel
    val cell_buf = {
      if (parallel)
        return_ranked_cells_parallel(lang_model, include)
      else
        return_ranked_cells_serially(lang_model, include)
    }

    val retval = cell_buf.toIndexedSeq sortWith (_._2 > _._2)

    /* If doing things parallel, this code applies for debugging
       (serial has the debugging code embedded into it). */
    if (parallel && debug("lots")) {
      for ((cell, score) <- retval)
        errprint("Nonempty cell at indices %s = location %s, num_documents = %s, score = %s",
          cell.describe_indices, cell.describe_location,
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
    self.kl_divergence(self_kl_cache, other, partial = partial)

  def score_cell(lang_model: LangModel, cell: GridCell[Co]) = {
    val cell_lang_model = cell.grid_lm
    var kldiv = call_kl_divergence(lang_model, cell_lang_model)
    if (symmetric) {
      val kldiv2 = cell_lang_model.kl_divergence(null, lang_model,
        partial = partial)
      kldiv = (kldiv + kldiv2) / 2.0
    }
    // Negate so that higher scores are better
    -kldiv
  }

  override def return_ranked_cells(lang_model: LangModel,
      include: Iterable[GridCell[Co]]) = {
    // This will be used by `score_cell` above.
    self_kl_cache = lang_model.get_kl_divergence_cache()

    val cells = super.return_ranked_cells(lang_model, include)

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
        val items =
          (contribs.toIndexedSeq sortWith ((x, y) => abs(x._2) > abs(y._2))).
            take(GridLocateConstants.kldiv_num_contrib_words)
        for ((word, contribval) <- items)
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
  partial: Boolean = false
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
          ((1.0 - bw) / lang_model.model.num_tokens, bw)
        }
      } else (1.0, 0.0))

    val word_logprob =
      cell.grid_lm.model_logprob(lang_model)
    val baseline_logprob =
      log(cell.num_docs.toDouble /
          grid.total_num_docs)
    val logprob = (word_weight * word_logprob +
      baseline_weight * baseline_logprob)
    logprob
  }
}

class AverageCellProbabilityGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends GridRanker[Co](ranker_name, grid) {
  def create_cell_dist_factory(lru_cache_size: Int) =
    new CellDistFactory[Co](lru_cache_size)

  val cdist_factory =
    create_cell_dist_factory(grid.driver.params.lru_cache_size)

  def return_ranked_cells(lang_model: LangModel, include: Iterable[GridCell[Co]]) = {
    val celldist =
      cdist_factory.get_cell_dist_for_lang_model(grid, lang_model)
    celldist.get_ranked_cells(include)
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
