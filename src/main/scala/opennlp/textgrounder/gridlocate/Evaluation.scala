///////////////////////////////////////////////////////////////////////////////
//  Evaluation.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
//  Copyright (C) 2011 Stephen Roller, The University of Texas at Austin
//  Copyright (C) 2012 Mike Speriosu, The University of Texas at Austin
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

import scala.util.control.Breaks._
import scala.collection.mutable

import learning.{Ranker, Reranker}
import util.collection._
import util.experiment._
import util.io.FileHandler
import util.math._
import util.os.{curtimehuman, output_resource_usage}
import util.print.errprint
import util.error.{warning, internal_error}
import util.textdb.Row
import util.debug._

/////////////////////////////////////////////////////////////////////////////
//                 General statistics on evaluation results                //
/////////////////////////////////////////////////////////////////////////////

// incorrect_reasons is a map from ID's for reasons to strings describing
// them.
class EvalStats(
  val driver_stats: ExperimentDriverStats,
  prefix: String = "",
  incorrect_reasons: Map[String, String] = Map()
) {
  def construct_counter_name(name: String) = {
    if (prefix == "") name
    else prefix + "." + name
  }

  def increment_counter(name: String) {
    driver_stats.increment_local_counter(construct_counter_name(name))
  }

  def get_counter(name: String) = {
    driver_stats.get_local_counter(construct_counter_name(name))
  }

  def list_counters(group: String, recursive: Boolean,
      fully_qualified: Boolean = true) =
    driver_stats.list_counters(construct_counter_name(group), recursive,
      fully_qualified)

  def record_result(correct: Boolean, reason: String = null) {
    if (reason != null)
      assert(incorrect_reasons.keySet contains reason)
    increment_counter("instances.total")
    if (correct)
      increment_counter("instances.correct")
    else {
      increment_counter("instances.incorrect")
      if (reason != null)
        increment_counter("instances.incorrect." + reason)
    }
  }

  def total_instances = get_counter("instances.total")
  def correct_instances = get_counter("instances.correct")
  def incorrect_instances = get_counter("instances.incorrect")

  def output_fraction(header: String, amount: Long, total: Long) {
    if (amount > total) {
      warning("Something wrong: Fractional quantity %s greater than total %s",
        amount, total)
    }
    var percent =
      if (total == 0) "indeterminate percent"
      else "%5.2f%%" format (100 * amount.toDouble / total)
    errprint("%s = %s/%s = %s", header, amount, total, percent)
  }

  def output_correct_results() {
    output_fraction("Percent correct", correct_instances, total_instances)
  }

  def output_incorrect_results() {
    output_fraction("Percent incorrect", incorrect_instances, total_instances)
    for ((reason, descr) <- incorrect_reasons) {
      output_fraction("  %s" format descr,
        get_counter("instances.incorrect." + reason), total_instances)
    }
  }

  def output_other_stats() {
    for (field <- driver_stats.list_local_counters("", recursive = true)) {
      val count = driver_stats.get_local_counter(field)
      driver_stats.note_result(field, count)
      errprint("%s = %s", field, count)
    }
  }

  def output_result_header() {
    if (total_instances == 0) {
      warning("Strange, no instances found at all; perhaps --eval-format is incorrect?")
      return
    }
    errprint("Number of instances = %s", total_instances)
  }

  def output_results() {
    output_result_header()
    output_correct_results()
    output_incorrect_results()
    output_other_stats()
  }
}

//////// Statistics for locating documents

/**
 * General class for the result of evaluating a document.  Specifies a
 * document, cell grid, and the predicted coordinate for the document.
 * The reason that a cell grid needs to be given is that we need to
 * retrieve the cell that the document belongs to in order to get the
 * "central point" (center or centroid of the cell), and in general we
 * may be operating with multiple cell grids (e.g. in the combination of
 * uniform and k-D tree grids). (FIXME: I don't know if this is actually
 * true.)
 *
 * FIXME: Perhaps we should redo the results in terms of pseudo-documents
 * instead of cells.
 *
 * @tparam Co type of a coordinate
 *
 * @param document document whose coordinate is predicted
 * @param grid cell grid against which error comparison should be done
 * @param pred_coord predicted coordinate of the document
 */
class DocEvalResult[Co](
  val document: GridDoc[Co],
  val grid: Grid[Co],
  val pred_coord: Co
) {
  /**
   * Correct cell in the cell grid in which the document belongs
   */
  val correct_cell = grid.find_best_cell_for_document(document,
    create_non_recorded = true).get
  /**
   * Number of documents in the correct cell
   */
  val num_docs_in_correct_cell = correct_cell.num_docs
  /**
   * Central point of the correct cell
   */
  val correct_central_point = correct_cell.get_central_point
  /**
   * "True distance" (rather than e.g. degree distance) between document's
   * coordinate and central point of correct cell
   */
  val correct_truedist = document.distance_to_coord(correct_central_point)
  /**
   * "True distance" (rather than e.g. degree distance) between document's
   * coordinate and predicted coordinate
   */
  val pred_truedist = document.distance_to_coord(pred_coord)

  /**
   * Print out the evaluation result, possibly along with some of the
   * top-ranked cells.
   *
   * @param doctag A short string identifying the document (e.g. '#25'),
   *   to be printed out at the beginning of diagnostic lines describing
   *   the document and its evaluation results.
   */
  def print_result(doctag: String, driver: GridLocateDriver[Co]) {
    errprint("%s:Document %s:", doctag, document)
    // errprint("%s:Document language model: %s", doctag, document.grid_lm)
    errprint("%s:  %s types, %f tokens",
      doctag, document.grid_lm.num_types,
      document.grid_lm.num_tokens)

    errprint("%s:  Distance %s to correct cell central point at %s",
      doctag, document.output_distance(correct_truedist), correct_central_point)
    errprint("%s:  Distance %s to predicted cell central point at %s",
      doctag, document.output_distance(pred_truedist), pred_coord)

    errprint("%s:  correct cell: %s", doctag, correct_cell)
  }

  def to_row = Seq(
    "title" -> document.title,
    "correct-coord" -> document.coord,
    "numtypes" -> document.grid_lm.num_types,
    "numtokens" -> document.grid_lm.num_tokens,
    "rerank-lm-numtypes" -> document.rerank_lm.num_types,
    "rerank-lm-numtokens" -> document.rerank_lm.num_tokens,
    "correct-cell" -> correct_cell.format_location,
    "correct-cell-true-center" -> correct_cell.get_true_center,
    "correct-cell-centroid" -> correct_cell.get_centroid,
    "correct-cell-central-point" -> correct_cell.get_central_point,
    "correct-cell-numdocs" -> correct_cell.num_docs,
    "pred-coord" -> pred_coord,
    "oracle-dist" -> correct_truedist,
    "error-dist" -> pred_truedist
  )

  /**
   * Return a "public" version of this result to be returned to callers.
   * May include less information than what is required temporarily for
   * print_result().
   */
  def get_public_result = this
}

/**
 * A basic class for accumulating statistics from multiple evaluation
 * results.
 */
trait DocEvalStats[Co] extends EvalStats {
  // "True dist" means actual distance in km's or whatever.
  // Error distance for actual predicted top cell.
  val true_dists = mutable.Buffer[Double]()
  // Error distance if we always picked the correct cell.
  val oracle_true_dists = mutable.Buffer[Double]()

  val output_result_with_units: Double => String

  def record_result(res: DocEvalResult[Co]) {
    true_dists += res.pred_truedist
    oracle_true_dists += res.correct_truedist
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
    val results = Seq(
      ("mean-true-error-distance",
       "Mean true error distance",
       mean(true_dists)),
      ("median-true-error-distance",
       "Median true error distance",
       median(true_dists)),
      ("mean-oracle-true-error-distance",
       "Mean oracle true error distance",
       mean(oracle_true_dists)),
      ("median-oracle-true-error-distance",
       "Median oracle true error distance",
       median(oracle_true_dists))
    )
    for ((field, desc, value) <- results) {
      errprint("  %s = %s", desc, output_result_with_units(value))
      driver_stats.note_result(field, value, desc)
    }
  }
}

/**
 * Class for accumulating statistics from multiple document evaluation results,
 * with separate sets of statistics for different intervals of error distances
 * and number of documents in correct cell. ("Grouped" in the sense that we may be
 * computing not only results for the documents as a whole but also for various
 * subgroups.)
 *
 * @tparam Co type of a coordinate
 *
 * @param driver_stats Object (possibly a trait) through which global-level
 *   program statistics can be accumulated (in a Hadoop context, this maps
 *   to counters).
 * @param grid Cell grid against which results were derived.
 */
class GroupedDocEvalStats[Co](
  driver_stats: ExperimentDriverStats,
  grid: Grid[Co],
  create_stats: (ExperimentDriverStats, String) => DocEvalStats[Co]
) extends EvalStats(driver_stats) with DocEvalStats[Co] {

  def create_stats_for_range[T](prefix: String, range: T) =
    create_stats(driver_stats, prefix + ".byrange." + range)

  val all_document = create_stats(driver_stats, "")

  // naitr = "num documents in correct cell"
  val docs_by_naitr = new IntTableByRange(Seq(1, 10, 25, 100),
    create_stats_for_range("num_documents_in_correct_cell", _))

  // Results for documents where the location is at a certain distance
  // from the center of the correct statistical cell.  The key is measured in
  // fractions of a tiling cell (determined by 'dist_fraction_increment',
  // e.g. if dist_fraction_increment = 0.25 then values in the range of
  // [0.25, 0.5) go in one bin, [0.5, 0.75) go in another, etc.).  We measure
  // distance is two ways: true distance (in km or whatever) and "degree
  // distance", as if degrees were a constant length both latitudinally
  // and longitudinally.
  val dist_fraction_increment = 0.25
  def docmap(prefix: String) =
    new SettingDefaultHashMap[Double, DocEvalStats[Co]](
      create_stats_for_range(prefix, _))
  val docs_by_true_dist_to_correct_central_point =
    docmap("true_dist_to_correct_central_point")

  // Similar, but distance between location and center of top predicted
  // cell.
  val dist_fractions_for_error_dist = Seq(
    0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6, 8,
    12, 16, 24, 32, 48, 64, 96, 128, 192, 256,
    // We're never going to see these
    384, 512, 768, 1024, 1536, 2048)
  val docs_by_true_dist_to_pred_central_point =
    new DoubleTableByRange(dist_fractions_for_error_dist,
      create_stats_for_range("true_dist_to_pred_central_point", _))

  override def record_result(res: DocEvalResult[Co]) {
    all_document.record_result(res)
    // Stephen says recording so many counters leads to crashes (at the 51st
    // counter or something), so don't do it unless called for.
    if (true) // results_by_range
      record_result_by_range(res)
  }

  def record_result_by_range(res: DocEvalResult[Co]) {
    val naitr = docs_by_naitr.get_collector(res.num_docs_in_correct_cell)
    naitr.record_result(res)
  }

  override def increment_counter(name: String) {
    all_document.increment_counter(name)
  }

  override def output_results() {
    errprint("")
    errprint("Results for all documents:")
    all_document.output_results()
    /* FIXME: This code specific to MultiRegularGrid is kind of ugly.
       Perhaps it should go elsewhere.

       FIXME: Also note that we don't actually do anything here, because of
       the 'if (false)'.  See above.
     */
    //if (all_results)
    if (true) // results_by_range
      output_results_by_range()
    // FIXME: Output median and mean of true and degree error dists; also
    // maybe move this info info EvalByRank so that we can output the values
    // for each category
    errprint("")
    output_resource_usage()
  }

  def output_results_by_range() { 
    errprint("")
    for ((lower, upper, obj) <- docs_by_naitr.iter_ranges()) {
      errprint("")
      errprint("Results for documents where number of documents")
      errprint("  in correct cell is in the range [%s,%s]:",
        lower, upper - 1)
      obj.output_results()
    }
  }

  val output_result_with_units: Double => String =
    (kmdist: Double) => internal_error("should not be called")
}

/////////////////////////////////////////////////////////////////////////////
//                             Main evaluation code                        //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for evaluating a corpus of test documents.
 * Uses the command-line parameters to determine which documents
 * should be skipped.
 *
 * @param ranker_name Name of the ranker used for performing evaluation.
 *   This is output in various status messages.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such, in particular command-line parameters that allow a subset of the
 *   total set of documents to be evaluated.
 */
abstract class CorpusEvaluator(
  ranker_name: String,
  val driver: GridLocateDriver[_]
) {
  /** Type of document to evaluate. */
  type TEvalDoc
  /** Type of result of evaluating a document. */
  type TEvalRes
  var documents_processed = 0
  var skip_initial = driver.params.skip_initial_test_docs
  var skip_n = 0

  /**
   * Determine whether we should skip the next document due to parameters
   * calling for certain documents in a certain sequence to be skipped.
   * Return `(skip, reason)` where `skip` is true if document would be skipped,
   * false if processed and evaluated; and `reason` is the reason for
   * skipping.
   */
  def would_skip_by_parameters() = {
    if (skip_initial != 0) {
      skip_initial -= 1
      (true, "--skip-initial-test-docs setting")
    } else if (skip_n != 0) {
      skip_n -= 1
      (true, "--every-nth-test-doc setting")
    } else {
      skip_n = driver.params.every_nth_test_doc - 1
      (false, "")
    }
  }
        
  /**
   * Return `(skip, reason)` where `skip` is true if document would be skipped,
   * false if processed and evaluated; and `reason` is the reason for
   * skipping.
   */
  def would_skip_document(doc: TEvalDoc) = (false, "")

  /**
   * Evaluate a document.  Return an object describing the results of the
   * evaluation.
   */
  def evaluate_document(doc: TEvalDoc): TEvalRes

  /**
   * Output results so far.  If 'isfinal', this is the last call, so
   * output more results.
   */
  def output_results(isfinal: Boolean = false): Unit

  val task = driver.show_progress("evaluating", "document",
    maxtime = driver.params.max_time_per_stage,
    maxitems = driver.params.num_test_docs)
  var last_elapsed = 0.0
  var last_processed = 0

  def process_document_statuses(docstats: Iterator[DocStatus[TEvalRes]]
  ) = {
    new DocCounterTrackerFactory(driver).process_statuses(docstats)
  }

  /**
   * Evaluate the documents, or some subset of them.  This may skip
   * some of the documents (e.g. based on the parameter
   * `--every-nth-test-doc`) and may stop early (e.g. based on
   * `--num-test-docs`).
   *
   * @return Iterator over evaluation results.
   */
  def evaluate_documents(docstats: Iterator[DocStatus[(Row, TEvalDoc)]]) = {
    var statnum = 0
    val result_stats =
      for (stat <- docstats) yield {
        // errprint("Processing document: %s", stat)
        statnum += 1
        stat.map_result { case (row, doc) =>
          val doctag = "#%s" format statnum
          val (skip, reason) = would_skip_document(doc)
          if (skip)
            (None, "skipped", reason, doctag)
          else {
            val (skip, reason) = would_skip_by_parameters()
            if (skip)
              (None, "skipped", reason, doctag)
            else {
              val res = evaluate_document(doc)
              (Some(res), "processed", "", doctag)
            }
          }
        }
      }

    task.iterate(process_document_statuses(result_stats)).map { res =>
      val new_elapsed = task.elapsed_time
      val new_processed = task.num_processed
      // If enough time and documents have gone by, print out results
      if ((new_elapsed - last_elapsed >=
          GridLocateConstants.time_between_status &&
        new_processed - last_processed >=
          GridLocateConstants.docs_between_status)) {
        errprint("Results after %s documents (ranker %s):",
          task.num_processed, ranker_name)
        output_results(isfinal = false)
        errprint("End of results after %s documents (ranker %s):",
          task.num_processed, ranker_name)
        last_elapsed = new_elapsed
        last_processed = new_processed
      }
      res
    } ++ new SideEffectIterator( {
      errprint("")
      errprint("Final results for ranker %s: All %s documents processed:",
        ranker_name, task.num_processed)
      errprint("Ending operation at %s", curtimehuman)
      output_results(isfinal = true)
      errprint("Ending final results for ranker %s", ranker_name)
      output_resource_usage()
    } )
  }
}

/**
 * Abstract class for evaluating a test document by comparing it against each
 * of the cells in a cell grid, where each cell has an associated
 * pseudo-document created by amalgamating all of the training documents
 * in the cell.
 *
 * Abstract class for for evaluating a test document where a collection of
 * documents has been divided into "training" and "test" sets, and the
 * training set used to construct a cell grid in which the training
 * documents in a particular cell are amalgamated to form a pseudo-document
 * and evaluation of a test document proceeds by comparing it against each
 * pseudo-document in turn.
 *
 * This is the highest-level evaluation class that includes the concept of a
 * coordinate that is associated with training and test documents, so that
 * computation of error distances possible.
 *
 * @tparam Co Type of the coordinate assigned to a document
 *
 * @param ranker Object describing how to rank a given document.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such.
 */
abstract class GridEvaluator[Co](
  val ranker: GridRanker[Co],
  override val driver: GridLocateDriver[Co],
  evalstats: DocEvalStats[Co]
) extends CorpusEvaluator(ranker.ranker_name, driver) {
  type TEvalDoc = GridDoc[Co]
  override type TEvalRes = DocEvalResult[Co]

  class GridDocCounterTrackerFactory extends
      DocCounterTrackerFactory[TEvalRes](driver) {
    override def create_tracker(shortname: String) =
      new DocCounterTracker[TEvalRes](shortname, driver) {
        val want_indiv_results = !driver.params.oracle_results &&
          driver.params.print_results
        override def print_status(status: DocStatus[TEvalRes]) {
          (status.status, status.maybedoc) match {
            case ("processed", Some(res)) => {
              if (want_indiv_results)
                res.print_result(status.docdesc, driver)
            }
            case _ => super.print_status(status)
          }
        }

        override def handle_status(status: DocStatus[TEvalRes]):
            Option[TEvalRes] = {
          super.handle_status(status) match {
            case None => None
            case Some(res) => Some(res.get_public_result)
          }
        }
      }
  }

  def output_results(isfinal: Boolean = false) {
    evalstats.output_results() // all_results = isfinal)
  }

  def get_correct_rank(candidates: Iterable[(GridCell[Co], Double)],
      correct_cell: GridCell[Co]) = {
    candidates.zipWithIndex.find {
      case ((cell, score), index) => cell == correct_cell
    } match {
      case Some(((cell, score), index)) => index + 1
      case None => 1000000000
    }
  }

  /**
   * Compare the document to the pseudo-documents associated with each cell,
   * using the ranker for this evaluator.  Return a tuple
   * (pred_cells, correct_rank), where:
   *
   *  pred_cells = List of predicted cells, from best to worst; each list
   *     entry is actually a tuple of (cell, score) where higher scores
   *     are better
   *  correct_rank = Rank of correct cell among predicted cells
   *
   * @param document Document to evaluate.
   * @param correct_cell Cell in the cell grid which contains the document.
   */
  def return_ranked_cells(document: GridDoc[Co], correct_cell: GridCell[Co]) = {
    if (driver.params.oracle_results)
      (Iterable((correct_cell, 0.0)), 1)
    else {
      ranker match {
        case reranker: Reranker[GridDoc[Co], GridCell[Co]] if debug("reranker") => {
          val (initial_ranking, reranking) =
            reranker.evaluate_with_initial_ranking(document, correct_cell,
              include_correct = false)
          errprint("Correct cell initial rank: %s",
            get_correct_rank(initial_ranking, correct_cell))
          val correct_rank = get_correct_rank(reranking, correct_cell)
          errprint("Correct cell new rank: %s", correct_rank)
          (reranking, correct_rank)
        }
        case _ => {
          val cells = ranker.evaluate(document, correct_cell,
            include_correct = false)
          (cells, get_correct_rank(cells, correct_cell))
        }
      }
    }
  }

  /**
   * Actual implementation of code to evaluate a document.  Optionally
   * Return an object describing the results of the evaluation, and
   * optionally print out information on these results.
   *
   * @param document Document to evaluate.
   * @param correct_cell Cell in the cell grid which contains the document.
   */
  def imp_evaluate_document(document: GridDoc[Co], correct_cell: GridCell[Co]
  ): DocEvalResult[Co]

  override def process_document_statuses(
    docstats: Iterator[DocStatus[TEvalRes]]
  ) = {
    (new GridDocCounterTrackerFactory).process_statuses(docstats)
  }

  /**
   * Evaluate a document, record statistics about it, etc.  Calls
   * `imp_evaluate_document` to do the document evaluation and records
   * the results in `evalstats`.  Printing of information about the
   * evaluation happens when the document status is processed, in
   * GridDocCounterTrackerFactory.
   *
   * Return an object describing the results of the evaluation.
   *
   * @param document Document to evaluate.
   */
  def evaluate_document(document: GridDoc[Co]) = {
    val (skip, reason) = would_skip_document(document)
    assert(!skip)
    assert(document.grid_lm.finished)
    assert(document.rerank_lm.finished)
    val maybe_correct_cell =
      ranker.grid.find_best_cell_for_document(document,
        create_non_recorded = true)
    assert(maybe_correct_cell != None)
    val correct_cell = maybe_correct_cell.get
    if (debug("ranking") || debug("commontop")) {
      val naitr = correct_cell.num_docs
      errprint("Evaluating document %s with %s documents in correct cell",
        document, naitr)
    }
    val result = imp_evaluate_document(document, correct_cell)
    evalstats.record_result(result)
    if (result.num_docs_in_correct_cell == 0) {
      evalstats.increment_counter("documents.no_training_documents_in_cell")
    }
    result
  }
}

/**
 * An implementation of `GridEvaluator` that compares the test
 * document against each pseudo-document in the cell grid, ranks them by
 * score and computes the document's location by the central point of the
 * top-ranked cell.
 *
 * @tparam Co Type of the coordinate assigned to a document
 *
 * @param ranker Object describing how to rank a given document.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such.
 */
class RankedGridEvaluator[Co](
  ranker: GridRanker[Co],
  driver: GridLocateDriver[Co],
  evalstats: DocEvalStats[Co]
) extends GridEvaluator[Co] (
  ranker, driver, evalstats
) {
  def imp_evaluate_document(document: GridDoc[Co],
      correct_cell: GridCell[Co]) = {
    ranker match {
      case reranker: Reranker[GridDoc[Co], GridCell[Co]] => {
        val (initial_ranking, reranking) =
          reranker.evaluate_with_initial_ranking(document, correct_cell,
            include_correct = false)
        val correct_rank = get_correct_rank(reranking, correct_cell)
        val initial_correct_rank =
          get_correct_rank(initial_ranking, correct_cell)
        new FullRerankedDocEvalResult[Co](document, reranking, correct_rank,
          initial_ranking, initial_correct_rank)
      }
      case _ => {
        val (pred_cells, correct_rank) =
          return_ranked_cells(document, correct_cell)

        new FullRankedDocEvalResult[Co](document, pred_cells,
          correct_rank)
      }
    }
  }
}

/**
 *
 * Subclass of `DocEvalResult` where the predicted coordinate
 * is specifically the central point of one of the grid cells.  Here we use
 * an algorithm that does cell-by-cell comparison and computes a ranking of
 * all the cells.
 *
 * @tparam Co type of a coordinate
 *
 * @param document document whose coordinate is predicted
 * @param pred_cell top-ranked predicted cell in which the document should
 *        belong
 * @param correct_rank rank of the document's correct cell among all of the
 *        predicted cell
 */
class RankedDocEvalResult[Co](
  document: GridDoc[Co],
  val pred_cell: GridCell[Co],
  val correct_rank: Int
) extends DocEvalResult[Co](
  document, pred_cell.grid,
  pred_cell.get_central_point
) {
  /**
   * "True distance" (rather than e.g. degree distance) between document's
   * coordinate and the coordinate that would be predicted if we picked a given
   * cell as the correct one.
   */
  def pred_truedist_for_cell(cell: GridCell[Co]) =
    document.distance_to_coord(cell.get_central_point)

  override def print_result(doctag: String, driver: GridLocateDriver[Co]) {
    super.print_result(doctag, driver)
    errprint("%s:  correct cell at rank: %s", doctag, correct_rank)
  }

  override def to_row = super.to_row ++ Seq(
    "pred-cell" -> pred_cell.format_location,
    "pred-cell-true-center" -> pred_cell.get_true_center,
    "pred-cell-centroid" -> pred_cell.get_centroid,
    "pred-cell-central-point" -> pred_cell.get_central_point,
    "pred-cell-numdocs" -> pred_cell.num_docs,
    "correct-rank" -> correct_rank
  )
}

class RerankedDocEvalResult[Co](
  document: GridDoc[Co],
  pred_cell: GridCell[Co],
  correct_rank: Int,
  val initial_pred_cell: GridCell[Co],
  val initial_correct_rank: Int
) extends RankedDocEvalResult[Co](
  document, pred_cell, correct_rank
) {
  override def print_result(doctag: String, driver: GridLocateDriver[Co]) {
    super.print_result(doctag, driver)
    errprint("%s:  correct cell at initial rank: %s (vs. new %s)", doctag,
      initial_correct_rank, correct_rank)
  }

  override def to_row = super.to_row ++ Seq(
    "initial-pred-cell" -> initial_pred_cell.format_location,
    "initial-pred-cell-true-center" -> initial_pred_cell.get_true_center,
    "initial-pred-cell-central-point" -> initial_pred_cell.get_central_point,
    "initial-pred-cell-numdocs" -> initial_pred_cell.num_docs,
    "initial-correct-rank" -> initial_correct_rank
  )
}

/**
 *
 * Subclass of `RankedDocEvalResult` listing all of the predicted
 * cells in order, with their scores.  We don't use this more than temporarily
 * to avoid excess memory usage when a large number of cells are present.
 *
 * @tparam Co type of a coordinate
 *
 * @param document document whose coordinate is predicted
 * @param pred_cells list of predicted cells with scores
 * @param correct_rank rank of the document's correct cell among all of the
 *        predicted cell
 */
class FullRankedDocEvalResult[Co](
  document: GridDoc[Co],
  val pred_cells: Iterable[(GridCell[Co], Double)],
  correct_rank: Int
) extends RankedDocEvalResult[Co](
  document, pred_cells.head._1, correct_rank
) {
  override def print_result(doctag: String, driver: GridLocateDriver[Co]) {
    if (debug("all-scores")) {
      for (((cell, score), index) <- pred_cells.zipWithIndex) {
        errprint("%s: %6s: Cell at %s: score = %g", doctag, index + 1,
          cell.format_indices, score)
      }
    }
    super.print_result(doctag, driver)
    val num_cells_to_output =
      if (driver.params.num_top_cells_to_output >= 0)
         math.min(driver.params.num_top_cells_to_output, pred_cells.size)
      else pred_cells.size
    for (((cell, score), i) <- pred_cells.take(num_cells_to_output).zipWithIndex) {
      errprint("%s:  Predicted cell (at rank %s, kl-div %s): %s",
        // FIXME: This assumes KL-divergence or similar scores, which have
        // been negated to make larger scores better.
        doctag, i + 1, -score, cell)
    }

    val num_nearest_neighbors = driver.params.num_nearest_neighbors
    val kNN = pred_cells.take(num_nearest_neighbors).map {
      case (cell, score) => cell }
    val kNNranks = pred_cells.take(num_nearest_neighbors).zipWithIndex.map {
      case ((cell, score), i) => (cell, i + 1) }.toMap
    val closest_half_with_dists =
      kNN.map(n => (n, document.distance_to_coord(n.get_central_point))).
        toIndexedSeq.sortWith(_._2 < _._2).take(num_nearest_neighbors/2)

    closest_half_with_dists.foreach {
      case (cell, dist) =>
        errprint("%s:  #%s close neighbor: %s; error distance: %s",
          doctag, kNNranks(cell), cell.get_central_point,
          document.output_distance(dist))
    }

    val avg_dist_of_neighbors = mean(closest_half_with_dists.map(_._2))
    errprint("%s:  Average distance from correct cell center to %s closest cells' centers from %s best matches: %s",
      doctag, (num_nearest_neighbors/2), num_nearest_neighbors,
      document.output_distance(avg_dist_of_neighbors))

    if (avg_dist_of_neighbors < pred_truedist)
      driver.increment_local_counter("instances.num_where_avg_dist_of_neighbors_beats_pred_truedist.%s" format num_nearest_neighbors)

    if (debug("relcontribgrams")) {
      def output_relcontribgrams(celltag: String, cell: GridCell[Co],
          othertag: String, others: Iterable[GridCell[Co]]) {
        val compared_string =
          if (others.isEmpty) ""
          else " compared to %s" format othertag
        errprint("%s: Words contributing most to %s %s%s", doctag, celltag,
          cell, compared_string)
        val grams =
          document.grid_lm.get_most_contributing_grams(
            cell.grid_lm, others.map { _.grid_lm })
        for ((gram, count) <-
             grams.take(GridLocateConstants.relcontribgrams_to_print))
          errprint("%s: %s = %s", doctag,
            document.grid_lm.gram_to_string(gram), count)
      }

      output_relcontribgrams("predicted cell", pred_cell, "", Iterable())
      output_relcontribgrams("correct cell", correct_cell, "", Iterable())
      output_relcontribgrams("predicted cell", pred_cell,
        "correct cell %s" format correct_cell, Iterable(correct_cell))
      output_relcontribgrams("predicted cell", pred_cell,
        "all others", pred_cells.map(_._1).tail)
      output_relcontribgrams("correct cell", correct_cell,
        "all others", pred_cells.map(_._1).filter(_ != correct_cell))
    }
  }

  override def get_public_result =
    new RankedDocEvalResult(document, pred_cells.head._1, correct_rank)
}

/**
 *
 * Subclass of `FullRankedDocEvalResult` for debugging or
 * investigating performance issues with the reranker.
 *
 * @tparam Co type of a coordinate
 *
 * @param document document whose coordinate is predicted
 * @param pred_cells list of predicted cells with scores
 * @param correct_rank rank of the document's correct cell among all of the
 *        predicted cell
 */
class FullRerankedDocEvalResult[Co](
  document: GridDoc[Co],
  pred_cells: Iterable[(GridCell[Co], Double)],
  correct_rank: Int,
  initial_pred_cells: Iterable[(GridCell[Co], Double)],
  initial_correct_rank: Int
) extends FullRankedDocEvalResult[Co](
  document, pred_cells, correct_rank
) {
  override def print_result(doctag: String, driver: GridLocateDriver[Co]) {
    super.print_result(doctag, driver)
    val num_cells_to_output = 5
    for (((cell, score), i) <-
        initial_pred_cells.take(num_cells_to_output).zipWithIndex) {
      errprint("%s:  Initial predicted cell (at rank %s, kl-div %s): %s",
        // FIXME: This assumes KL-divergence or similar scores, which have
        // been negated to make larger scores better.
        doctag, i + 1, -score, cell)
    }

    val initial_pred_cell = initial_pred_cells.head._1
    val initial_pred_coord = initial_pred_cell.get_central_point
    val initial_pred_truedist = document.distance_to_coord(initial_pred_coord)

    errprint("%s:  Distance %s to initial predicted cell center at %s",
      doctag, document.output_distance(initial_pred_truedist),
      initial_pred_coord)
    errprint("%s:  Error distance change by reranking = %s - %s = %s",
      doctag, document.output_distance(pred_truedist),
      document.output_distance(initial_pred_truedist),
      document.output_distance(pred_truedist - initial_pred_truedist)
    )
  }

  override def get_public_result =
    new RerankedDocEvalResult(document, pred_cells.head._1, correct_rank,
      initial_pred_cells.head._1, initial_correct_rank)
}

/**
 * A class for accumulating statistics from multiple evaluation results,
 * including statistics on the rank of the correct cell.
 */
class RankedDocEvalStats[Co](
  driver_stats: ExperimentDriverStats,
  prefix: String,
  val output_result_with_units: Double => String
) extends EvalStats(driver_stats, prefix) with DocEvalStats[Co] {
  val top_n_for_oracle_dists =
    GridLocateConstants.top_n_for_oracle_dists
  val max_rank_for_exact_incorrect =
    GridLocateConstants.max_rank_for_exact_incorrect
  val max_top_n = top_n_for_oracle_dists.max
  val incorrect_by_exact_rank = intmap[Int]()
  val correct_by_up_to_rank = intmap[Int]()
  var incorrect_past_max_rank = 0
  var total_credit = 0

  // Error distance if we always picked the best possible cell within the
  // top N items after the initial ranking. This indicates the best we
  // could do if our reranker functioned as an oracle within the a given
  // value of --rerank-top-n.
  val oracle_true_dists_at = bufmap[Int, Double]()
  val oracle_saw_correct_at = bufmap[Int, Double]()

  override def record_result(res: DocEvalResult[Co]) {
    super.record_result(res)
    val rankres = res.asInstanceOf[FullRankedDocEvalResult[Co]]

    // Code formerly in EvalStatsWithRank.
    {
      val corrank = rankres.correct_rank
      assert(corrank >= 1)
      val correct = corrank == 1
      super.record_result(correct, reason = null)
      var within_max_rank = false
      if (corrank <= max_top_n) {
        total_credit += max_top_n + 1 - corrank
        for (n <- top_n_for_oracle_dists if corrank <= n)
          correct_by_up_to_rank(n) += 1
      }
      if (corrank <= max_rank_for_exact_incorrect)
        incorrect_by_exact_rank(corrank) += 1
      else
        incorrect_past_max_rank += 1
    }

    // Compute best possible error distance among top N items. First we
    // compute a list of best error distance at all values of N up to the
    // largest one we're considering. This is monotonically decreasing
    // so to compute the new best we only need compare the most recently
    // computed best with the error distance for the cell at the next
    // rank.
    val min_errors =
      rankres.pred_cells.take(max_top_n).foldLeft(Vector(Double.MaxValue)) {
        (minerrors, cell_score_pair) =>
          val (cell_at_rank, score) = cell_score_pair
          val newbest =
            minerrors.last min rankres.pred_truedist_for_cell(cell_at_rank)
          minerrors :+ newbest
      }
    for (rank <- top_n_for_oracle_dists)
      oracle_true_dists_at(rank) :+= min_errors(rank)
    true_dists += res.pred_truedist
    oracle_true_dists += res.correct_truedist
  }

  override def output_correct_results() {
    // This just prints the percent correct, but we incorporate it below.
    // super.output_correct_results()
    for (i <- top_n_for_oracle_dists) {
      output_fraction("  Percent correct at rank <= %s" format i,
        correct_by_up_to_rank(i), total_instances)
    }
    val possible_credit = max_top_n * total_instances
    output_fraction("Percent correct with partial credit (rank <= %s)"
                    format max_top_n,
      total_credit, possible_credit)
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
     errprint("                                           %10s %10s",
       "mean", "median")
    for (i <- top_n_for_oracle_dists) {
       errprint("Oracle true error distance at rank <= %-4s %10s %10s",
         "%s:" format i,
         output_result_with_units(mean(oracle_true_dists_at(i))),
         output_result_with_units(median(oracle_true_dists_at(i))))
    }
    for (i <- 2 to max_rank_for_exact_incorrect) {
      output_fraction("  Incorrect, with correct at rank %s" format i,
        incorrect_by_exact_rank(i),
        total_instances)
    }
    output_fraction("  Incorrect, with correct not in top %s" format
      max_rank_for_exact_incorrect,
      incorrect_past_max_rank, total_instances)
  }
}

/**
 * A general implementation of `GridEvaluator` that returns a single
 * best point for a given test document.
 *
 * @tparam Co Type of the coordinate assigned to a document
 *
 * @param ranker Object describing how to rank a given document.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such.
 */
abstract class CoordGridEvaluator[Co](
  ranker: GridRanker[Co],
  driver: GridLocateDriver[Co],
  evalstats: DocEvalStats[Co]
) extends GridEvaluator[Co](
  ranker, driver, evalstats
) {
  def find_best_point(document: GridDoc[Co], correct_cell: GridCell[Co]): Co

  def imp_evaluate_document(document: GridDoc[Co], correct_cell: GridCell[Co]) = {
    val pred_coord = find_best_point(document, correct_cell)
    new CoordDocEvalResult[Co](document, ranker.grid, pred_coord)
  }
}

/**
 * Subclass of `DocEvalResult` where the predicted coordinate
 * is a point, not necessarily the central point of one of the grid cells.
 *
 * @tparam Co type of a coordinate
 *
 * @param document document whose coordinate is predicted
 * @param grid cell grid against which error comparison should be done
 * @param pred_coord predicted coordinate of the document
 */
class CoordDocEvalResult[Co](
  document: GridDoc[Co],
  grid: Grid[Co],
  pred_coord: Co
) extends DocEvalResult[Co](
  document, grid, pred_coord
) {
}

/**
 * A class for accumulating statistics from multiple evaluation results,
 * where the results directly specify a coordinate (rather than e.g. a cell).
 */
class CoordDocEvalStats[Co](
  driver_stats: ExperimentDriverStats,
  prefix: String,
  val output_result_with_units: Double => String
) extends EvalStats(driver_stats, prefix) with DocEvalStats[Co] {
  override def record_result(res: DocEvalResult[Co]) {
    super.record_result(res)
    // It doesn't really make sense to record a result as "correct" or
    // "incorrect" but we need to record something; just do "false"
    // FIXME: Fix the incorrect assumption here that "correct" or
    // "incorrect" always exists.
    record_result(correct = false)
  }
}

/**
 * An implementation of `GridEvaluator` that compares the test
 * document against each pseudo-document in the cell grid, selects the
 * top N ranked pseudo-documents for some N, and uses the mean-shift
 * algorithm to determine a single point that is hopefully in the middle
 * of the strongest cluster of points among the central points of the
 * pseudo-documents.
 *
 * @tparam Co Type of the coordinate assigned to a document
 *
 * @param ranker Object describing how to rank a given document.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such.
 */
class MeanShiftGridEvaluator[Co](
  ranker: GridRanker[Co],
  driver: GridLocateDriver[Co],
  evalstats: DocEvalStats[Co],
  k_best: Int,
  mean_shift_obj: MeanShift[Co]
) extends CoordGridEvaluator[Co](
  ranker, driver, evalstats
) {
  def find_best_point(document: GridDoc[Co], correct_cell: GridCell[Co]) = {
    val (pred_cells, correct_rank) = return_ranked_cells(document, correct_cell)
    val top_k = pred_cells.take(k_best).map(_._1.get_central_point).toIndexedSeq
    val shifted_values = mean_shift_obj.mean_shift(top_k)
    mean_shift_obj.vec_mean(shifted_values)
  }
}
