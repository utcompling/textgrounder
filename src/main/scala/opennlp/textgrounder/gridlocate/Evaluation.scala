///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010, 2011 Ben Wing, The University of Texas at Austin
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

////////
//////// Evaluation.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.gridlocate

import util.control.Breaks._
import collection.mutable

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.experiment.ExperimentDriverStats
import opennlp.textgrounder.util.mathutil._
import opennlp.textgrounder.util.ioutil.{FileHandler, FileProcessor}
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.osutil.{curtimehuman, output_resource_usage}
import opennlp.textgrounder.util.printutil.{errprint, warning}

import GridLocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                 General statistics on evaluation results                //
/////////////////////////////////////////////////////////////////////////////

// incorrect_reasons is a map from ID's for reasons to strings describing
// them.
class EvalStats(
  driver_stats: ExperimentDriverStats,
  prefix: String,
  incorrect_reasons: Map[String, String]
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
    for (ty <- driver_stats.list_local_counters("", true)) {
      val count = driver_stats.get_local_counter(ty)
      errprint("%s = %s", ty, count)
    }
  }

  def output_results() {
    if (total_instances == 0) {
      warning("Strange, no instances found at all; perhaps --eval-format is incorrect?")
      return
    }
    errprint("Number of instances = %s", total_instances)
    output_correct_results()
    output_incorrect_results()
    output_other_stats()
  }
}

class EvalStatsWithRank(
  driver_stats: ExperimentDriverStats,
  prefix: String,
  max_rank_for_credit: Int = 10
) extends EvalStats(driver_stats, prefix, Map[String, String]()) {
  val incorrect_by_exact_rank = intmap[Int]()
  val correct_by_up_to_rank = intmap[Int]()
  var incorrect_past_max_rank = 0
  var total_credit = 0

  def record_result(rank: Int) {
    assert(rank >= 1)
    val correct = rank == 1
    super.record_result(correct, reason = null)
    if (rank <= max_rank_for_credit) {
      total_credit += max_rank_for_credit + 1 - rank
      incorrect_by_exact_rank(rank) += 1
      for (i <- rank to max_rank_for_credit)
        correct_by_up_to_rank(i) += 1
    } else
      incorrect_past_max_rank += 1
  }

  override def output_correct_results() {
    super.output_correct_results()
    val possible_credit = max_rank_for_credit * total_instances
    output_fraction("Percent correct with partial credit",
      total_credit, possible_credit)
    for (i <- 2 to max_rank_for_credit) {
      output_fraction("  Correct is at or above rank %s" format i,
        correct_by_up_to_rank(i), total_instances)
    }
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
    for (i <- 2 to max_rank_for_credit) {
      output_fraction("  Incorrect, with correct at rank %s" format i,
        incorrect_by_exact_rank(i),
        total_instances)
    }
    output_fraction("  Incorrect, with correct not in top %s" format
      max_rank_for_credit,
      incorrect_past_max_rank, total_instances)
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
 * @tparam TCoord type of a coordinate
 * @tparam TDoc type of a document
 * @tparam TCell type of a cell
 * @tparam TGrid type of a cell grid
 *
 * @param document document whose coordinate is predicted
 * @param cell_grid cell grid against which error comparison should be done
 * @param pred_coord predicted coordinate of the document
 */
class DocumentEvaluationResult[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc],
  TGrid <: CellGrid[TCoord, TDoc, TCell]
](
  val document: TDoc,
  val cell_grid: TGrid,
  val pred_coord: TCoord
) {
  /**
   * True cell in the cell grid in which the document belongs
   */
  val true_cell = cell_grid.find_best_cell_for_coord(document.coord, true)
  /**
   * Number of documents in the true cell
   */
  val num_docs_in_true_cell = true_cell.combined_dist.num_docs_for_word_dist
  /**
   * Central point of the true cell
   */
  val true_center = true_cell.get_center_coord()
  /**
   * "True distance" (rather than e.g. degree distance) between document's
   * coordinate and central point of true cell
   */
  val true_truedist = document.distance_to_coord(true_center)
  /**
   * "True distance" (rather than e.g. degree distance) between document's
   * coordinate and predicted coordinate
   */
  val pred_truedist = document.distance_to_coord(pred_coord)

  def record_result(stats: DocumentEvalStats) {
    stats.record_predicted_distance(pred_truedist)
  }
}

/**
 * Subclass of `DocumentEvaluationResult` where the predicted coordinate
 * is a point, not necessarily the central point of one of the grid cells.
 *
 * @tparam TCoord type of a coordinate
 * @tparam TDoc type of a document
 * @tparam TCell type of a cell
 * @tparam TGrid type of a cell grid
 *
 * @param document document whose coordinate is predicted
 * @param cell_grid cell grid against which error comparison should be done
 * @param pred_coord predicted coordinate of the document
 */
class CoordDocumentEvaluationResult[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc],
  TGrid <: CellGrid[TCoord, TDoc, TCell]
](
  document: TDoc,
  cell_grid: TGrid,
  pred_coord: TCoord
) extends DocumentEvaluationResult[TCoord, TDoc, TCell, TGrid](
  document, cell_grid, pred_coord
) {
  override def record_result(stats: DocumentEvalStats) {
    super.record_result(stats)
    // It doesn't really make sense to record a result as "correct" or
    // "incorrect" but we need to record something; just do "false"
    // FIXME: Fix the incorrect assumption here that "correct" or
    // "incorrect" always exists.
    stats.asInstanceOf[CoordDocumentEvalStats].record_result(false)
  }
}

/**
 * Subclass of `DocumentEvaluationResult` where the predicted coordinate
 * is specifically the central point of one of the grid cells.
 *
 * @tparam TCoord type of a coordinate
 * @tparam TDoc type of a document
 * @tparam TCell type of a cell
 * @tparam TGrid type of a cell grid
 *
 * @param document document whose coordinate is predicted
 * @param pred_cell top-ranked predicted cell in which the document should
 *        belong
 * @param true_rank rank of the document's true cell among all of the
 *        predicted cell
 */
class RankedDocumentEvaluationResult[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc],
  TGrid <: CellGrid[TCoord, TDoc, TCell]
](
  document: TDoc,
  val pred_cell: TCell,
  val true_rank: Int
) extends DocumentEvaluationResult[TCoord, TDoc, TCell, TGrid](
  document, pred_cell.cell_grid.asInstanceOf[TGrid],
  pred_cell.get_center_coord()
) {
  override def record_result(stats: DocumentEvalStats) {
    super.record_result(stats)
    stats.asInstanceOf[RankedDocumentEvalStats].record_true_rank(true_rank)
  }
}

/**
 * A basic class for accumulating statistics from multiple evaluation
 * results.
 */
trait DocumentEvalStats extends EvalStats {
  // "True dist" means actual distance in km's or whatever.
  val true_dists = mutable.Buffer[Double]()
  val oracle_true_dists = mutable.Buffer[Double]()

  def record_predicted_distance(pred_true_dist: Double) {
    true_dists += pred_true_dist
  }

  def record_oracle_distance(oracle_true_dist: Double) {
    oracle_true_dists += oracle_true_dist
  }

  protected def output_result_with_units(result: Double): String

  override def output_incorrect_results() {
    super.output_incorrect_results()
    errprint("  Mean true error distance = %s",
      output_result_with_units(mean(true_dists)))
    errprint("  Median true error distance = %s",
      output_result_with_units(median(true_dists)))
    errprint("  Mean oracle true error distance = %s",
      output_result_with_units(mean(oracle_true_dists)))
  }
}

/**
 * A class for accumulating statistics from multiple evaluation results,
 * where the results directly specify a coordinate (rather than e.g. a cell).
 */
abstract class CoordDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  prefix: String
) extends EvalStats(driver_stats, prefix, Map[String, String]())
  with DocumentEvalStats {
}

/**
 * A class for accumulating statistics from multiple evaluation results,
 * including statistics on the rank of the true cell.
 */
abstract class RankedDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  prefix: String,
  max_rank_for_credit: Int = 10
) extends EvalStatsWithRank(driver_stats, prefix, max_rank_for_credit)
  with DocumentEvalStats {
  def record_true_rank(rank: Int) {
    record_result(rank)
  }
}

/**
 * Class for accumulating statistics from multiple document evaluation results,
 * with separate sets of statistics for different intervals of error distances
 * and number of documents in true cell. ("Grouped" in the sense that we may be
 * computing not only results for the documents as a whole but also for various
 * subgroups.)
 *
 * @tparam TCoord type of a coordinate
 * @tparam TDoc type of a document
 * @tparam TCell type of a cell
 * @tparam TGrid type of a cell grid
 * @tparam TEvalRes type of object holding result of evaluating a document
 *
 * @param driver_stats Object (possibly a trait) through which global-level
 *   program statistics can be accumulated (in a Hadoop context, this maps
 *   to counters).
 * @param cell_grid Cell grid against which results were derived.
 * @param results_by_range If true, record more detailed range-by-range
 *   subresults.  Not on by default because Hadoop may choke on the large
 *   number of counters created this way.
 */
abstract class GroupedDocumentEvalStats[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc],
  TGrid <: CellGrid[TCoord, TDoc, TCell],
  TEvalRes <: DocumentEvaluationResult[TCoord, TDoc, TCell, TGrid]
](
  driver_stats: ExperimentDriverStats,
  cell_grid: TGrid,
  results_by_range: Boolean
) {
  def create_stats(prefix: String): DocumentEvalStats
  def create_stats_for_range[T](prefix: String, range: T) =
    create_stats(prefix + ".byrange." + range)

  val all_document = create_stats("")

  // naitr = "num documents in true cell"
  val docs_by_naitr = new IntTableByRange(Seq(1, 10, 25, 100),
    create_stats_for_range("num_documents_in_true_cell", _))

  // Results for documents where the location is at a certain distance
  // from the center of the true statistical cell.  The key is measured in
  // fractions of a tiling cell (determined by 'dist_fraction_increment',
  // e.g. if dist_fraction_increment = 0.25 then values in the range of
  // [0.25, 0.5) go in one bin, [0.5, 0.75) go in another, etc.).  We measure
  // distance is two ways: true distance (in km or whatever) and "degree
  // distance", as if degrees were a constant length both latitudinally
  // and longitudinally.
  val dist_fraction_increment = 0.25
  def docmap(prefix: String) =
    new SettingDefaultHashMap[Double, DocumentEvalStats](
      create_stats_for_range(prefix, _))
  val docs_by_true_dist_to_true_center =
    docmap("true_dist_to_true_center")

  // Similar, but distance between location and center of top predicted
  // cell.
  val dist_fractions_for_error_dist = Seq(
    0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6, 8,
    12, 16, 24, 32, 48, 64, 96, 128, 192, 256,
    // We're never going to see these
    384, 512, 768, 1024, 1536, 2048)
  val docs_by_true_dist_to_pred_center =
    new DoubleTableByRange(dist_fractions_for_error_dist,
      create_stats_for_range("true_dist_to_pred_center", _))

  def record_one_result(stats: DocumentEvalStats, res: TEvalRes) {
    res.record_result(stats)
  }

  def record_one_oracle_result(stats: DocumentEvalStats, res: TEvalRes) {
    stats.record_oracle_distance(res.true_truedist)
  }

  def record_result(res: TEvalRes) {
    record_one_result(all_document, res)
    record_one_oracle_result(all_document, res)
    // Stephen says recording so many counters leads to crashes (at the 51st
    // counter or something), so don't do it unless called for.
    if (results_by_range)
      record_result_by_range(res)
  }

  def record_result_by_range(res: TEvalRes) {
    val naitr = docs_by_naitr.get_collector(res.num_docs_in_true_cell)
    record_one_result(naitr, res)
  }

  def increment_counter(name: String) {
    all_document.increment_counter(name)
  }

  def output_results(all_results: Boolean = false) {
    errprint("")
    errprint("Results for all documents:")
    all_document.output_results()
    /* FIXME: This code specific to MultiRegularCellGrid is kind of ugly.
       Perhaps it should go elsewhere.

       FIXME: Also note that we don't actually do anything here, because of
       the 'if (false)'.  See above.
     */
    //if (all_results)
    if (false)
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
      errprint("  in true cell is in the range [%s,%s]:",
        lower, upper - 1)
      obj.output_results()
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Main evaluation code                        //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for evaluating a corpus of test documents.
 * Uses the command-line parameters to determine which documents
 * should be skipped.
 *
 * @tparam TEvalDoc Type of document to evaluate.
 * @tparam TEvalRes Type of result of evaluating a document.
 *
 * @param stratname Name of the strategy used for performing evaluation.
 *   This is output in various status messages.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such, in particular command-line parameters that allow a subset of the
 *   total set of documents to be evaluated.
 */
abstract class CorpusEvaluator[TEvalDoc, TEvalRes](
  stratname: String,
  val driver: GridLocateDriver
) {
  var documents_processed = 0
  val results = mutable.Map[TEvalDoc, TEvalRes]()
  var skip_initial = driver.params.skip_initial_test_docs
  var skip_n = 0

  /**
   * Return true if we should skip the next document due to parameters
   * calling for certain documents in a certain sequence to be skipped.
   */
  def would_skip_by_parameters() = {
    var do_skip = false
    if (skip_initial != 0) {
      skip_initial -= 1
      do_skip = true
    } else if (skip_n != 0) {
      skip_n -= 1
      do_skip = true
    } else
      skip_n = driver.params.every_nth_test_doc - 1
    do_skip
  }
        
  /**
   * Return true if we should stop processing, given that `new_processed`
   * items have already been processed.
   */
  def would_stop_processing(new_processed: Int) = {
    // If max # of docs reached, stop
    val stop = (driver.params.num_test_docs > 0 &&
                 new_processed >= driver.params.num_test_docs)
    if (stop) {
      errprint("")
      errprint("Stopping because limit of %s documents reached",
        driver.params.num_test_docs)
    }
    stop
  }

  /**
   * Return true if document would be skipped; false if processed and
   * evaluated.
   */
  def would_skip_document(doc: TEvalDoc, doctag: String) = false

  /**
   * Evaluate a document.  Return an object describing the results of the
   * evaluation.
   *
   * @param document Document to evaluate.
   * @param doctag A short string identifying the document (e.g. '#25'),
   *   to be printed out at the beginning of diagnostic lines describing
   *   the document and its evaluation results.
   */
  def evaluate_document(doc: TEvalDoc, doctag: String):
    TEvalRes

  /**
   * Output results so far.  If 'isfinal', this is the last call, so
   * output more results.
   */
  def output_results(isfinal: Boolean = false): Unit

  val task = new MeteredTask("document", "evaluating",
    maxtime = driver.params.max_time_per_stage)
  var last_elapsed = 0.0
  var last_processed = 0

  /** Process a document.  This checks to see whether we should evaluate
   * the document (e.g. based on parameters indicating which documents
   * to evaluate), and evaluates as necessary, storing the results into
   * `results`.
   *
   * @param doc Document to be processed.
   * @return Tuple `(processed, keep_going)` where `processed` indicates
   *   whether the document was processed or skipped, and `keep_going`
   *   indicates whether processing of further documents should continue or
   *   stop.
   */
  def process_document(doc: TEvalDoc): (Boolean, Boolean) = {
    // errprint("Processing document: %s", doc)
    val num_processed = task.num_processed
    val doctag = "#%d" format (1 + num_processed)
    if (would_skip_document(doc, doctag)) {
      errprint("Skipped document %s", doc)
      (false, true)
    } else {
      val do_skip = would_skip_by_parameters()
      if (do_skip)
        errprint("Passed over document %s", doctag)
      else {
        // Don't put side-effecting code inside of an assert!
        val result = evaluate_document(doc, doctag)
        assert(result != null)
        results(doc) = result
      }

      if (task.item_processed())
        (!do_skip, false)
      else {
        val new_elapsed = task.elapsed_time
        val new_processed = task.num_processed

        if (would_stop_processing(new_processed)) {
          task.finish()
          (!do_skip, false)
        } else {
          // If five minutes and ten documents have gone by, print out results
          if ((new_elapsed - last_elapsed >= 300 &&
            new_processed - last_processed >= 10)) {
            errprint("Results after %d documents (strategy %s):",
              task.num_processed, stratname)
            output_results(isfinal = false)
            errprint("End of results after %d documents (strategy %s):",
              task.num_processed, stratname)
            last_elapsed = new_elapsed
            last_processed = new_processed
          }
          (!do_skip, true)
        }
      }
    }
  }

  def finish() {
    task.finish()

    errprint("")
    errprint("Final results for strategy %s: All %d documents processed:",
      stratname, task.num_processed)
    errprint("Ending operation at %s", curtimehuman())
    output_results(isfinal = true)
    errprint("Ending final results for strategy %s", stratname)
  }

  /** Process a set of files, extracting the documents in each one and
    * evaluating them using `process_document`.
    */
  def process_files(filehand: FileHandler, files: Iterable[String]): Boolean
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
 * @tparam TCoord Type of the coordinate assigned to a document
 * @tparam XTDoc Type of the training and test documents
 * @tparam TCell Type of a cell in a cell grid
 * @tparam XTGrid Type of a cell grid
 * @tparam TEvalRes Type of result of evaluating a document.
 *
 * @param strategy Object encapsulating the strategy used for performing
 *   evaluation.
 * @param stratname Name of the strategy used for performing evaluation.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such.
 *
 * Note that we are forced to use the strange names `XTDoc` and `XTGrid`
 * because of an apparent Scala bug that prevents use of the more obvious
 * names `TDoc` and `TGrid` due to a naming clash.  Possibly there is a
 * solution to this problem but if so I can't figure it out.
 */
abstract class CellGridEvaluator[
  TCoord,
  XTDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, XTDoc],
  // SCALABUG: No way to access something called 'TGrid' at this scope in the
  // line below where it says 'type TGrid = XTGrid'
  XTGrid <: CellGrid[TCoord, XTDoc, TCell],
  TEvalRes <: DocumentEvaluationResult[TCoord, XTDoc, TCell, XTGrid]
](
  val strategy: GridLocateDocumentStrategy[TCell, XTGrid],
  val stratname: String,
  driver: GridLocateDriver { type TGrid = XTGrid; type TDoc = XTDoc } // GridLocateDocumentTypeDriver
) extends CorpusEvaluator[XTDoc, TEvalRes](stratname, driver) {
  def create_grouped_eval_stats(driver: GridLocateDriver, // GridLocateDocumentTypeDriver
    cell_grid: XTGrid, results_by_range: Boolean):
    GroupedDocumentEvalStats[TCoord, XTDoc, TCell, XTGrid, TEvalRes]

  val evalstats = create_grouped_eval_stats(driver,
    strategy.cell_grid, results_by_range = driver.params.results_by_range)

  def output_results(isfinal: Boolean = false) {
    evalstats.output_results(all_results = isfinal)
 }

  /**
   * A file processor that reads corpora containing document metadata and
   * creates a DistDocument for each document described, and evaluates it.
   *
   * @param suffix Suffix specifying the type of document file wanted
   *   (e.g. "counts" or "document-metadata"
   * @param cell_grid Cell grid to add newly created DistDocuments to
   */
  class EvaluateCorpusFileProcessor(
    suffix: String
  ) extends DistDocumentFileProcessor(suffix, driver) {
    def handle_document(fieldvals: Seq[String]) = {
      val doc = driver.document_table.create_and_init_document(
        schema, fieldvals, false)
      if (doc == null) (false, true)
      else {
        doc.dist.finish_after_global()
        process_document(doc)
      }
    }

    def process_lines(lines: Iterator[String],
        filehand: FileHandler, file: String,
        compression: String, realname: String) = {
      var should_stop = false
      breakable {
        for (line <- lines) {
          if (!parse_row(line)) {
            should_stop = true
            break
          }
        }
      }
      output_resource_usage()
      !should_stop
    }
  }

  def process_files(filehand: FileHandler, files: Iterable[String]): Boolean = {
    /* NOTE: `files` must actually be a list of directories, e.g. as
       comes from the value of --input-corpus. */
    for (dir <- files) {
      val fileproc = new EvaluateCorpusFileProcessor(
        driver.params.eval_set + "-" + driver.document_file_suffix)
      fileproc.read_schema_from_corpus(filehand, dir)
      if (!fileproc.process_files(filehand, Seq(dir)))
        return false
    }
    return true
  }

  //title = None
  //words = []
  //for line in openr(filename, errors="replace"):
  //  if (rematch("Article title: (.*)$", line))
  //    if (title != null)
  //      yield (title, words)
  //    title = m_[1]
  //    words = []
  //  else if (rematch("Link: (.*)$", line))
  //    args = m_[1].split('|')
  //    truedoc = args[0]
  //    linkword = truedoc
  //    if (len(args) > 1)
  //      linkword = args[1]
  //    words.append(linkword)
  //  else:
  //    words.append(line)
  //if (title != null)
  //  yield (title, words)

  override def would_skip_document(document: XTDoc, doctag: String) = {
    if (document.dist == null) {
      // This can (and does) happen when --max-time-per-stage is set,
      // so that the counts for many documents don't get read in.
      if (driver.params.max_time_per_stage == 0.0 && driver.params.num_training_docs == 0)
        warning("Can't evaluate document %s without distribution", document)
      true
    } else false
  }

  /**
   * Compare the document to the pseudo-documents associated with each cell,
   * using the strategy for this evaluator.  Return a tuple
   * (pred_cells, true_rank), where:
   *
   *  pred_cells = List of predicted cells, from best to worst; each list
   *     entry is actually a tuple of (cell, score) where lower scores
   *     are better
   *  true_rank = Rank of true cell among predicted cells
   *
   * @param document Document to evaluate.
   * @param true_cell Cell in the cell grid which contains the document.
   */
  def return_ranked_cells(document: XTDoc, true_cell: TCell) = {
    if (driver.params.oracle_results)
      (Array((true_cell, 0.0)), 1)
    else {
      def get_computed_results() = {
        val cells = strategy.return_ranked_cells(document.dist).toArray
        var rank = 1
        var broken = false
        breakable {
          for ((cell, value) <- cells) {
            if (cell eq true_cell) {
              broken = true
              break
            }
            rank += 1
          }
        }
        if (!broken)
          rank = 1000000000
        (cells, rank)
      }

      get_computed_results()
    }
  }

  /**
   * Actual implementation of code to evaluate a document.  Optionally
   * Return an object describing the results of the evaluation, and
   * optionally print out information on these results.
   *
   * @param document Document to evaluate.
   * @param doctag A short string identifying the document (e.g. '#25'),
   *   to be printed out at the beginning of diagnostic lines describing
   *   the document and its evaluation results.
   * @param true_cell Cell in the cell grid which contains the document.
   * @param want_indiv_results Whether we should print out individual
   *   evaluation results for the document.
   */
  def imp_evaluate_document(document: XTDoc, doctag: String,
      true_cell: TCell, want_indiv_results: Boolean): TEvalRes

  /**
   * Evaluate a document, record statistics about it, etc.  Calls
   * `imp_evaluate_document` to do the document evaluation and optionally
   * print out information on the results, and records the results in
   * `evalstat`.
   *
   * Return an object describing the results of the evaluation.
   *
   * @param document Document to evaluate.
   * @param doctag A short string identifying the document (e.g. '#25'),
   *   to be printed out at the beginning of diagnostic lines describing
   *   the document and its evaluation results.
   */
  def evaluate_document(document: XTDoc, doctag: String): TEvalRes = {
    assert(!would_skip_document(document, doctag))
    assert(document.dist.finished)
    val true_cell =
      strategy.cell_grid.find_best_cell_for_coord(document.coord, true)
    if (debug("lots") || debug("commontop")) {
      val naitr = true_cell.combined_dist.num_docs_for_word_dist
      errprint("Evaluating document %s with %s word-dist documents in true cell",
        document, naitr)
    }
    val want_indiv_results =
      !driver.params.oracle_results && !driver.params.no_individual_results
    val result = imp_evaluate_document(document, doctag, true_cell,
      want_indiv_results)
    evalstats.record_result(result)
    if (result.num_docs_in_true_cell == 0) {
      evalstats.increment_counter("documents.no_training_documents_in_cell")
    }
    result
  }
}

/**
 * An implementation of `CellGridEvaluator` that compares the test
 * document against each pseudo-document in the cell grid, ranks them by
 * score and computes the document's location by the central point of the
 * top-ranked cell.
 *
 * @tparam TCoord Type of the coordinate assigned to a document
 * @tparam XTDoc Type of the training and test documents
 * @tparam TCell Type of a cell in a cell grid
 * @tparam XTGrid Type of a cell grid
 * @tparam TEvalRes Type of result of evaluating a document.
 *
 * @param strategy Object encapsulating the strategy used for performing
 *   evaluation.
 * @param stratname Name of the strategy used for performing evaluation.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such.
 */
abstract class RankedCellGridEvaluator[
  TCoord,
  XTDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, XTDoc],
  XTGrid <: CellGrid[TCoord, XTDoc, TCell],
  TEvalRes <: DocumentEvaluationResult[TCoord, XTDoc, TCell, XTGrid]
](
  strategy: GridLocateDocumentStrategy[TCell, XTGrid],
  stratname: String,
  driver: GridLocateDriver { type TGrid = XTGrid; type TDoc = XTDoc } // GridLocateDocumentTypeDriver
) extends CellGridEvaluator[
  TCoord, XTDoc, TCell, XTGrid, TEvalRes
](strategy, stratname, driver) {
  /**
   * Create an evaluation-result object describing the top-ranked
   * predicted cell and the rank of the document's true cell among
   * all predicted cells.
   */
  def create_cell_evaluation_result(document: XTDoc, pred_cell: TCell,
    true_rank: Int): TEvalRes

  val num_nearest_neighbors = driver.params.num_nearest_neighbors

  /**
   * Print out the evaluation result, possibly along with some of the
   * top-ranked cells.
   */
  def print_individual_result(doctag: String, document: XTDoc,
    result: TEvalRes, pred_cells: Array[(TCell, Double)]) {
    errprint("%s:Document %s:", doctag, document)
    // errprint("%s:Document distribution: %s", doctag, document.dist)
    errprint("%s:  %d types, %f tokens",
      doctag, document.dist.num_word_types, document.dist.num_word_tokens)
    errprint("%s:  true cell at rank: %s", doctag,
      result.asInstanceOf[RankedDocumentEvaluationResult[_,_,_,_]].true_rank)
    errprint("%s:  true cell: %s", doctag, result.true_cell)
    for (i <- 0 until 5) {
      errprint("%s:  Predicted cell (at rank %s, kl-div %s): %s",
        doctag, i + 1, pred_cells(i)._2, pred_cells(i)._1)
    }

    //for (num_nearest_neighbors <- 2 to 100 by 2) {
    val kNN = pred_cells.take(num_nearest_neighbors).map(_._1)
    val kNNranks = pred_cells.take(num_nearest_neighbors).zipWithIndex.map(p => (p._1._1, p._2+1)).toMap
    val closest_half_with_dists = kNN.map(n => (n, document.distance_to_coord(n.get_center_coord))).sortWith(_._2 < _._2).take(num_nearest_neighbors/2)

    closest_half_with_dists.zipWithIndex.foreach(
      c => errprint("%s:  #%s close neighbor: %s; error distance: %s",
        doctag, kNNranks(c._1._1), c._1._1.get_center_coord,
        document.output_distance(c._1._2)))

    errprint("%s:  Distance %s to true cell center at %s",
      doctag, document.output_distance(result.true_truedist), result.true_center)
    errprint("%s:  Distance %s to predicted cell center at %s",
      doctag, document.output_distance(result.pred_truedist), result.pred_coord)

    val avg_dist_of_neighbors = mean(closest_half_with_dists.map(_._2))
    errprint("%s:  Average distance from true cell center to %s closest cells' centers from %s best matches: %s",
      doctag, (num_nearest_neighbors/2), num_nearest_neighbors,
      document.output_distance(avg_dist_of_neighbors))

    if(avg_dist_of_neighbors < result.pred_truedist)
      driver.increment_local_counter("instances.num_where_avg_dist_of_neighbors_beats_pred_truedist.%s" format num_nearest_neighbors)
    //}
  }

  def imp_evaluate_document(document: XTDoc, doctag: String,
      true_cell: TCell, want_indiv_results: Boolean): TEvalRes = {
    val (pred_cells, true_rank) = return_ranked_cells(document, true_cell)
    val result =
      create_cell_evaluation_result(document, pred_cells(0)._1, true_rank)

    if (debug("all-scores")) {
      for (((cell, value), index) <- pred_cells.zipWithIndex) {
        errprint("%s: %6d: Cell at %s: score = %g", doctag, index + 1,
          cell.describe_indices(), value)
      }
    }
    if (want_indiv_results) {
      //val cells_for_average = pred_cells.zip(pred_cells.map(_._1.center))
      //for((cell, score) <- pred_cells) {
      //  val scell = cell.asInstanceOf[GeoCell[GeoCoord, GeoDoc]]
      //}
      print_individual_result(doctag, document, result, pred_cells)
    }

    return result
  }
}

/**
 * A general implementation of `CellGridEvaluator` that returns a single
 * best point for a given test document.
 *
 * @tparam TCoord Type of the coordinate assigned to a document
 * @tparam XTDoc Type of the training and test documents
 * @tparam TCell Type of a cell in a cell grid
 * @tparam XTGrid Type of a cell grid
 * @tparam TEvalRes Type of result of evaluating a document.
 *
 * @param strategy Object encapsulating the strategy used for performing
 *   evaluation.
 * @param stratname Name of the strategy used for performing evaluation.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such.
 */
abstract class CoordCellGridEvaluator[
  TCoord,
  XTDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, XTDoc],
  XTGrid <: CellGrid[TCoord, XTDoc, TCell],
  TEvalRes <: DocumentEvaluationResult[TCoord, XTDoc, TCell, XTGrid]
](
  strategy: GridLocateDocumentStrategy[TCell, XTGrid],
  stratname: String,
  driver: GridLocateDriver { type TGrid = XTGrid; type TDoc = XTDoc } // GridLocateDocumentTypeDriver
) extends CellGridEvaluator[
  TCoord, XTDoc, TCell, XTGrid, TEvalRes
](strategy, stratname, driver) {
  /**
   * Create an evaluation-result object describing the predicted coordinate.
   */
  def create_coord_evaluation_result(document: XTDoc, cell_grid: XTGrid,
    pred_coord: TCoord): TEvalRes

  /**
   * Print out the evaluation result.
   */
  def print_individual_result(doctag: String, document: XTDoc,
      result: TEvalRes) {
    errprint("%s:Document %s:", doctag, document)
    // errprint("%s:Document distribution: %s", doctag, document.dist)
    errprint("%s:  %d types, %f tokens",
      doctag, document.dist.num_word_types, document.dist.num_word_tokens)
    errprint("%s:  true cell: %s", doctag, result.true_cell)

    errprint("%s:  Distance %s to true cell center at %s",
      doctag, document.output_distance(result.true_truedist), result.true_center)
    errprint("%s:  Distance %s to predicted cell center at %s",
      doctag, document.output_distance(result.pred_truedist), result.pred_coord)
  }

  def find_best_point(document: XTDoc, true_cell: TCell): TCoord

  def imp_evaluate_document(document: XTDoc, doctag: String,
      true_cell: TCell, want_indiv_results: Boolean): TEvalRes = {
    val pred_coord = find_best_point(document, true_cell)
    val result = create_coord_evaluation_result(document, strategy.cell_grid,
      pred_coord)

    if (want_indiv_results)
      print_individual_result(doctag, document, result)

    return result
  }
}

/**
 * An implementation of `CellGridEvaluator` that compares the test
 * document against each pseudo-document in the cell grid, selects the
 * top N ranked pseudo-documents for some N, and uses the mean-shift
 * algorithm to determine a single point that is hopefully in the middle
 * of the strongest cluster of points among the central points of the
 * pseudo-documents.
 *
 * @tparam TCoord Type of the coordinate assigned to a document
 * @tparam XTDoc Type of the training and test documents
 * @tparam TCell Type of a cell in a cell grid
 * @tparam XTGrid Type of a cell grid
 * @tparam TEvalRes Type of result of evaluating a document.
 *
 * @param strategy Object encapsulating the strategy used for performing
 *   evaluation.
 * @param stratname Name of the strategy used for performing evaluation.
 * @param driver Driver class that encapsulates command-line parameters and
 *   such.
 */
abstract class MeanShiftCellGridEvaluator[
  TCoord,
  XTDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, XTDoc],
  XTGrid <: CellGrid[TCoord, XTDoc, TCell],
  TEvalRes <: DocumentEvaluationResult[TCoord, XTDoc, TCell, XTGrid]
](
  strategy: GridLocateDocumentStrategy[TCell, XTGrid],
  stratname: String,
  driver: GridLocateDriver { type TGrid = XTGrid; type TDoc = XTDoc }, // GridLocateDocumentTypeDriver
  k_best: Int,
  mean_shift_window: Double,
  mean_shift_max_stddev: Double,
  mean_shift_max_iterations: Int
) extends CoordCellGridEvaluator[
  TCoord, XTDoc, TCell, XTGrid, TEvalRes
](strategy, stratname, driver) {
  def create_mean_shift_obj(h: Double, max_stddev: Double,
    max_iterations: Int): MeanShift[TCoord]

  val mean_shift_obj = create_mean_shift_obj(mean_shift_window,
    mean_shift_max_stddev, mean_shift_max_iterations)

  def find_best_point(document: XTDoc, true_cell: TCell) = {
    val (pred_cells, true_rank) = return_ranked_cells(document, true_cell)
    val top_k = pred_cells.take(k_best).map(_._1.get_center_coord)
    val shifted_values = mean_shift_obj.mean_shift(top_k)
    mean_shift_obj.vec_mean(shifted_values)
  }
}

/**
 * A trait used when '--eval-format' is not 'internal', i.e. the test documents
 * don't come from the same corpus used to supply the training documents,
 * but come from some separate text file.  This is a general interface for
 * iterating over files and returning the test documents in those files
 * (possibly more than one per file).
 */
trait DocumentIteratingEvaluator[TEvalDoc, TEvalRes] extends
  CorpusEvaluator[TEvalDoc, TEvalRes] {
  /**
   * Return an Iterable listing the documents retrievable from the given
   * filename.
   */
  def iter_documents(filehand: FileHandler, filename: String):
    Iterable[TEvalDoc]

  class EvaluationFileProcessor extends FileProcessor {
    /* Process all documents in a given file.  If return value is false,
       processing was interrupted due to a limit being reached, and
       no more files should be processed. */
    def process_file(filehand: FileHandler, filename: String): Boolean = {
      for (doc <- iter_documents(filehand, filename)) {
        val (processed, keep_going) = process_document(doc)
        if (!keep_going)
          return false
      }
      return true
    }
  }

  def process_files(filehand: FileHandler, files: Iterable[String]) = {
    val fileproc = new EvaluationFileProcessor
    fileproc.process_files(filehand, files)
  }
}
