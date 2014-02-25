///////////////////////////////////////////////////////////////////////////////
//  GridRanker.scala
//
//  Copyright (C) 2010-2014 Ben Wing, The University of Texas at Austin
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
import collection.mutable
import math._

import util.print.errprint
import util.debug._
import util.textdb.Row

import langmodel._
import learning._
import learning.vowpalwabbit._

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
  /** Optional initialization stage passing one or more times over the
   * test data. */
  def initialize(
    get_docstats: () => Iterator[DocStatus[(Row, GridDoc[Co])]]
  ) { }
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
   * For a given test document, return an Iterable of tuples, each listing
   * a particular cell on the Earth and a score of some sort.  The correct
   * cell is as given, and if `include_correct` is specified, must be
   * included in the list.  Higher scores are better.  The results should
   * be in sorted order, with better cells earlier.
   */
  def return_ranked_cells(doc: GridDoc[Co], correct: Option[GridCell[Co]],
      include_correct: Boolean):
    Iterable[(GridCell[Co], Double)]

  def imp_evaluate(item: GridDoc[Co], correct: Option[GridCell[Co]],
      include_correct: Boolean) =
    return_ranked_cells(item, correct, include_correct)
}

/**
 * Object encapsulating a GridLocate data instance to be used by the
 * classifier that is either used for ranking directly or underlies the
 * reranker. This corresponds to a document in the training corpus.
 * This is used in place of just using an aggregate feature vector directly
 * because the cost perceptron cost function needs to retrieve the document
 * and correct cell while training in order to compute the distance between
 * them, which is used to compute the cost.
 */
abstract class GridRankerInst[Co] extends DataInstance {
  def doc: GridDoc[Co]
  def agg: AggregateFeatureVector
  final def feature_vector = agg
  /**
   * Return the candidate cell at the given label index.
   */
  def get_cell(index: LabelIndex): GridCell[Co]

  def pretty_print_labeled(prefix: String, correct: LabelIndex) {
    errprint(s"For instance $prefix with query doc $doc:")
    for ((fv, index) <- agg.fv.zipWithIndex) {
      val cell = get_cell(index)
      errprint(s"  $prefix-${index + 1}: %s: $cell: $fv",
        if (index == correct) "CORRECT" else "WRONG")
    }
  }
}

/**
 * Object encapsulating a GridLocate data instance to be used by the
 * classifier that is used directly for ranking the cells.
 */
case class GridRankingClassifierInst[Co](
  doc: GridDoc[Co],
  agg: AggregateFeatureVector,
  featvec_factory: CandidateFeatVecFactory[Co]
) extends GridRankerInst[Co] {
  def get_cell(index: LabelIndex) = featvec_factory.index_to_cell(index)
}

/**
 * Class that implements a very simple baseline ranker -- pick a random
 * cell.
 */

class RandomGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends SimpleGridRanker[Co](ranker_name, grid) {
  def return_ranked_cells(doc: GridDoc[Co], correct: Option[GridCell[Co]],
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
  def return_ranked_cells(doc: GridDoc[Co], correct: Option[GridCell[Co]],
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
  def score_cell(doc: GridDoc[Co], cell: GridCell[Co]): Double

  def get_candidates(correct: Option[GridCell[Co]], include_correct: Boolean) =
    grid.iter_nonempty_cells_including(correct, include_correct)

  /**
   * Compare a language model (for a document, typically) against all
   * cells. Return a sequence of tuples (cell, score) where 'cell'
   * indicates the cell and 'score' the score.
   */
  def return_ranked_cells_serially(doc: GridDoc[Co],
      correct: Option[GridCell[Co]], include_correct: Boolean) = {
    for (cell <- get_candidates(correct, include_correct)) yield {
      if (debug("ranking")) {
        errprint(
          "Nonempty cell at indices %s = location %s, num_documents = %s",
          cell.format_indices, cell.format_location,
          cell.num_docs)
      }
      (cell, score_cell(doc, cell))
    }
  }

  /**
   * Compare a language model (for a document, typically) against all
   * cells. Return a sequence of tuples (cell, score) where 'cell'
   * indicates the cell and 'score' the score.
   */
  def return_ranked_cells_parallel(doc: GridDoc[Co],
      correct: Option[GridCell[Co]], include_correct: Boolean) = {
    val cells = get_candidates(correct, include_correct)
    cells.par.map(c => (c, score_cell(doc, c)))
  }

  def return_ranked_cells(doc: GridDoc[Co], correct: Option[GridCell[Co]],
      include_correct: Boolean) = {
    val parallel = !grid.driver.params.no_parallel
    val cell_buf = {
      if (parallel)
        return_ranked_cells_parallel(doc, correct, include_correct)
      else
        return_ranked_cells_serially(doc, correct, include_correct)
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

  def score_cell(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val lang_model = doc.grid_lm
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

  override def return_ranked_cells(doc: GridDoc[Co],
      correct: Option[GridCell[Co]], include_correct: Boolean) = {
    val lang_model = doc.grid_lm
    // This will be used by `score_cell` above.
    self_kl_cache = lang_model.get_kl_divergence_cache()

    val cells = super.return_ranked_cells(doc, correct, include_correct)

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

  def score_cell(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val cossim =
      doc.grid_lm.cosine_similarity(cell.grid_lm,
        partial = partial, smoothed = smoothed)
    assert(cossim >= 0.0)
    // Just in case of round-off problems
    assert(cossim <= 1.002)
    cossim
  }
}

/**
 * Class that implements a ranker for document geolocation that sums the
 * unsmoothed probability (or frequency) values for the words in the
 * document. Generally only useful when '--tf-idf' or similar is invoked.
 */
class SumFrequencyGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends PointwiseScoreGridRanker[Co](ranker_name, grid) {

  def score_cell(doc: GridDoc[Co], cell: GridCell[Co]) = {
    doc.grid_lm.sum_frequency(cell.grid_lm)
  }
}

trait NaiveBayesFeature[Co]
{
  /** Possible initialization step at beginning to do a pass over test data.
   * Needed for NaiveBayesRoughRankerFeature when the wrapped ranker uses
   * Vowpal Wabbit. */
  def initialize(
      get_docstats: () => Iterator[DocStatus[(Row, GridDoc[Co])]]) {
  }

  def get_logprob(doc: GridDoc[Co], cell: GridCell[Co]): Double
}

class NaiveBayesTermsFeature[Co] extends NaiveBayesFeature[Co]
{
  def get_logprob(doc: GridDoc[Co], cell: GridCell[Co]) =
    cell.grid_lm.model_logprob(doc.grid_lm)
}

class NaiveBayesRoughRankerFeature[Co](
  rough_ranker: PointwiseScoreGridRanker[Co]
) extends NaiveBayesFeature[Co]
{
  // Needed for Vowpal Wabbit.
  override def initialize(
      get_docstats: () => Iterator[DocStatus[(Row, GridDoc[Co])]]) {
    rough_ranker.initialize(get_docstats)
  }

  def get_logprob(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val central = cell.get_central_point
    val rough_cell = rough_ranker.grid.find_best_cell_for_coord(central,
      create_non_recorded = true).get
    // We don't need to take the log here. If the score is from Naive Bayes,
    // we've already taken the log of the probability. If from maxent, we'd
    // convert it to a probability by exponentiating and renormalizing, and
    // the normalization factor will be the same for all cells so it shouldn't
    // affect the ranking and can be ignored. Taking the log would then just
    // cancel out the exponentiation, so neither needs to be done.
    rough_ranker.score_cell(doc, rough_cell)
  }
}

/** Use a Naive Bayes ranker for comparing document and cell. */
class NaiveBayesGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co],
  features: Iterable[NaiveBayesFeature[Co]]
) extends PointwiseScoreGridRanker[Co](ranker_name, grid) {

  override def initialize(
      get_docstats: () => Iterator[DocStatus[(Row, GridDoc[Co])]]) {
    for (f <- features)
      f.initialize(get_docstats)
  }

  def score_cell(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val params = grid.driver.params
    // Determine respective weightings
    val (word_weight, prior_weight) = {
      val bw = params.naive_bayes_prior_weight
      (1.0 - bw, bw)
    }

    val features_logprob = features.map(_.get_logprob(doc, cell)).sum
    // FIXME: Is the normalization necessary?
    val prior_logprob = log(cell.prior_weighting / grid.total_prior_weighting)
    val logprob = (word_weight * features_logprob + prior_weight * prior_logprob)
    logprob
  }
}

/** Use a classifier (normally maxent) for comparing document and cell. */
abstract class ClassifierGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co],
  featvec_factory: CandidateFeatVecFactory[Co]
) extends PointwiseScoreGridRanker[Co](ranker_name, grid) {

  // Only include the cells corresponding to those labels that the classifier
  // knows, possibly plus the correct cell if required.
  override def get_candidates(correct: Option[GridCell[Co]],
      include_correct: Boolean) = {
    val cands =
      (0 until featvec_factory.featvec_factory.mapper.number_of_labels
      ).map { label => featvec_factory.index_to_cell(label) }
    if (!include_correct || cands.find(_ == correct.get) != None)
      cands
    else
      correct.get +: cands
  }

  /**
   * Score a document by directly invoking the classifier, rather than
   * by looking up a cache of scores, if such a cache exists.
   */
  def score_doc_directly(doc: GridDoc[Co]): Iterable[(GridCell[Co], Double)]
}

/**
 * A classifier where we can use the normal LinearClassifier mechanism,
 * i.e. where we have the weights directly available and can cheaply score
 * an individual cell of an individual document. */
class IndivClassifierGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co],
  classifier: LinearClassifier,
  featvec_factory: CandidateFeatVecFactory[Co]
) extends ClassifierGridRanker[Co](ranker_name, grid, featvec_factory) {

  def score_cell(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val fv = featvec_factory(doc, cell, 0, 0, is_training = false)
    // We might be asked about a cell outside of the set of candidates,
    // especially when we have a limited set of possible candidates.
    // See comment in VowpalWabbitGridRanker for more info.
    featvec_factory.lookup_cell_if(cell) match {
      case Some(label) => classifier.score_label(fv, label)
      case None => Double.NegativeInfinity
    }
  }

  def score_doc_directly(doc: GridDoc[Co]) =
    return_ranked_cells(doc, None, include_correct = false)
}

/** Use a Vowpal Wabbit classifier for comparing document and cell. */
class VowpalWabbitGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co],
  classifier: VowpalWabbitClassifier,
  featvec_factory: DocFeatVecFactory[Co],
  cost_sensitive: Boolean
) extends ClassifierGridRanker[Co](ranker_name, grid, featvec_factory) {

  var doc_scores: Map[String, Array[Double]] = _

  override def initialize(
    get_docstats: () => Iterator[DocStatus[(Row, GridDoc[Co])]]
  ) {
    val docs = grid.docfact.document_statuses_to_documents(get_docstats())
    doc_scores = score_test_docs(docs).toMap
  }

  def score_test_docs(docs: Iterator[GridDoc[Co]]
      ): Iterable[(String, Array[Double])] = {
    val titles = mutable.Buffer[String]()

    val feature_file =
      if (cost_sensitive) {
        val labels_costs =
          (0 until featvec_factory.featvec_factory.mapper.number_of_labels
            ).map { label => (label, 0.0) }

        val training_data =
          docs.map/*Metered(task)*/ { doc =>
          val feats = featvec_factory.get_features(doc)
          titles += doc.title
          // It doesn't matter what we give as the correct costs here
          (feats, labels_costs)
        }

        classifier.write_cost_sensitive_feature_file(training_data)
      } else {
        val training_data =
          docs.map/*Metered(task)*/ { doc =>
          val feats = featvec_factory.get_features(doc)
          titles += doc.title
          // It doesn't matter what we give as the correct cell here
          (feats, 0)
        }

        classifier.write_feature_file(training_data)
      }
    val list_of_scores =
      classifier(feature_file).map { raw_label_scores =>
        val label_scores =
          if (cost_sensitive) {
            // Raw scores for cost-sensitive appear to be costs, i.e.
            // lower is better, so negate.
            raw_label_scores.map { case (label, score) => (label, -score) }
          } else {
            // Convert to proper log-probs.
            val indiv_probs = raw_label_scores map { case (label, score) =>
              (label, 1/(1 + math.exp(-score)))
            }
            val norm_sum = indiv_probs.map(_._2).sum
            indiv_probs map { case (label, prob) =>
              (label, math.log(prob/norm_sum))
            }
          }
        val scores = label_scores.sortWith(_._1 < _._1).map(_._2)
        assert(scores.size ==
          featvec_factory.featvec_factory.mapper.number_of_labels)
        scores
      }
    assert(titles.size == list_of_scores.size)
    titles zip list_of_scores
  }

  def score_cell(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val scores = doc_scores(doc.title)
    // We might be asked about a cell outside of the set of candidates,
    // especially when we have a limited set of possible candidates.
    // Note that the set of labels that the classifier knows about may
    // be less than the number in the 'candidates' param passed to
    // create_classifier_ranker(), particularly in the non-cost-sensitive
    // case, where the classifier only knows about labels corresponding
    // to candidates with a non-zero number of training documents in them.
    // (In the cost-sensitive case we have to supply costs for all labels
    // for each training document, and we go ahead and convert all
    // candidates to labels.)
    featvec_factory.lookup_cell_if(cell) match {
      case Some(label) => scores(label)
      case None => Double.NegativeInfinity
    }
  }

  /**
   * Score a document by directly invoking the classifier (which requires
   * spawning the VW app), rather than by looking up a cache of scores.
   */
  def score_doc_directly(doc: GridDoc[Co]) = {
    val scores = score_test_docs(Iterator(doc)).head._2
    val cands = get_candidates(None, include_correct = false)
    assert(scores.size == cands.size)
    cands zip scores
  }
}

/** Use a hierarchical classifier for comparing document and
  * cell. We work as follows:
  *
  * 1. Classify at the coarsest grid level, over all the cells in that grid.
  *    Take the top N for some beam size.
  * 2. For each grid cell, classify among the subdividing cells at the next
  *    finer grid. This computes e.g. p(C2|C1) for cell C1 at the coarsest
  *    level and cell C2 at the next finer level. Compute e.g. p(C1,C2) =
  *    p(C1) * p(C2|C1); again take the top N.
  * 3. Repeat till we reach the finest level.
  *
  * We need one classifier over all the non-empty cells at the coarsest level,
  * then for each subsequent level except the finest, as many classifiers
  * as there are non-empty cells on that level.
  *
  * @param ranker_name Identifying string, usually "classifier".
  * @param grids List of successive hierarchical grids, from coarse to fine.
  * @param coarse_ranker Ranker at coarsest level for all cells at
  *   that level.
  * @param finer_rankers Sequence of maps, one per grid other than at the
  *   coarsest level, mapping a grid cell at a coarser level to a ranker
  *   over the subdivided cells of that cell at the next finer level.
  * @param beam_size Number of top-ranked cells we keep from one level to
  *   the next.
  * @param cost_sensitive Whether we are doing cost-sensitive classification.
  */
class HierarchicalClassifierGridRanker[Co](
  ranker_name: String,
  grids: Iterable[Grid[Co]],
  coarse_ranker: ClassifierGridRanker[Co],
  finer_rankers: Iterable[Map[GridCell[Co], ClassifierGridRanker[Co]]],
  training_docs_cells: Iterable[(GridDoc[Co], GridCell[Co])],
  beam_size: Int
) extends SimpleGridRanker[Co](ranker_name, grids.last) {

  val coarsest_grid = grids.head
  // val finest_grid = grids.last

  override def initialize(
    get_docstats: () => Iterator[DocStatus[(Row, GridDoc[Co])]]
  ) {
    coarse_ranker.initialize(get_docstats)
  }

  def return_ranked_cells(doc: GridDoc[Co], correct: Option[GridCell[Co]],
      include_correct: Boolean): Iterable[(GridCell[Co], Double)] = {
    val raw_prev_scores =
      for (cell <- coarsest_grid.iter_nonempty_cells) yield {
        val score = coarse_ranker.score_cell(doc, cell)
        (cell, score)
      }
    var prev_scores =
      raw_prev_scores.toIndexedSeq.sortWith(_._2 > _._2)
    for ((finer, rankers) <- grids.tail zip finer_rankers) {
      // Cells at previous level that will be propagated to new level
      val beamed_prev_scores = prev_scores.take(beam_size)
      val new_scores = for ((old_cell, old_score) <- beamed_prev_scores) yield {
        val ranker = rankers(old_cell)
        val doc_ranked_scores =
          ranker.score_doc_directly(doc).toIndexedSeq.sortWith(_._2 > _._2)
        val (top_cell, top_score) = doc_ranked_scores.head
        (top_cell, old_score + top_score)
      }
      prev_scores = new_scores.sortWith(_._2 > _._2)
      /*
      The other way of doing hierarchical classification, constructing new
      classifiers on the fly.

      // Expand each cell to one or more cells at new level. Convert to set
      // to remove duplicates, then back to indexed sequence.
      val new_cells =
        prev_cells.flatMap { finer.get_subdivided_cells(_) }.toSet.toIndexedSeq
      val new_ranker = finer.driver.create_classifier_ranker(
        ranker_name, finer, new_cells, training_docs_cells)
      val new_scores =
        if (finer == finest_grid)
          new_ranker.evaluate(doc, correct, include_correct)
        else
          new_ranker.evaluate(doc, None, false)
      prev_scores = new_scores.toIndexedSeq
      */
    }
    prev_scores
  }
}

class AverageCellProbabilityGridRanker[Co](
  ranker_name: String,
  grid: Grid[Co]
) extends SimpleGridRanker[Co](ranker_name, grid) {
  def create_cell_dist_factory = new CellDistFactory[Co]

  val cdist_factory = create_cell_dist_factory

  def return_ranked_cells(doc: GridDoc[Co], correct: Option[GridCell[Co]],
      include_correct: Boolean) = {
    val celldist =
      cdist_factory.get_cell_dist_for_lang_model(grid, doc.grid_lm)
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
