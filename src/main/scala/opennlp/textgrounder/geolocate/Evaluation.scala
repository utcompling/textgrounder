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

package opennlp.textgrounder.geolocate

import collection.mutable

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.experiment.ExperimentDriverStats
import opennlp.textgrounder.util.mathutil.{mean, median}
import opennlp.textgrounder.util.ioutil.{FileHandler, FileProcessor}
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.osutil.{curtimehuman, output_resource_usage}
import opennlp.textgrounder.util.printutil.{errprint, warning}

/* FIXME: Eliminate this. */
import GeolocateDriver.Params

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

//////// Statistics for geolocating documents

abstract class GeolocateDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  prefix: String,
  max_rank_for_credit: Int = 10
) extends EvalStatsWithRank(driver_stats, prefix, max_rank_for_credit) {
  // "True dist" means actual distance in km's or whatever.
  val true_dists = mutable.Buffer[Double]()
  val oracle_true_dists = mutable.Buffer[Double]()

  def record_result(rank: Int, pred_true_dist: Double) {
    super.record_result(rank)
    true_dists += pred_true_dist
  }

  def record_oracle_result(oracle_true_dist: Double) {
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
 * Class for statistics for geolocating documents, with separate
 * sets of statistics for different intervals of error distances and
 * number of documents in true cell.
 */

abstract class GroupedGeolocateDocumentEvalStats[TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc]](
  driver_stats: ExperimentDriverStats,
  cell_grid: CellGrid[TCoord,TDoc,TCell],
  results_by_range: Boolean
) {
  type TBasicEvalStats <: GeolocateDocumentEvalStats
  type TDocEvalRes <:
    DocumentEvaluationResult[TCoord, TDoc, TCell]

  def create_stats(prefix: String): TBasicEvalStats
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
    new SettingDefaultHashMap[Double, TBasicEvalStats](
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

  def record_one_result(stats: TBasicEvalStats, res: TDocEvalRes) {
    stats.record_result(res.true_rank, res.pred_truedist)
  }

  def record_one_oracle_result(stats: TBasicEvalStats, res: TDocEvalRes) {
    stats.record_oracle_result(res.true_truedist)
  }

  def record_result(res: TDocEvalRes) {
    record_one_result(all_document, res)
    record_one_oracle_result(all_document, res)
    // Stephen says recording so many counters leads to crashes (at the 51st
    // counter or something), so don't do it unless called for.
    if (results_by_range)
      record_result_by_range(res)
  }

  def record_result_by_range(res: TDocEvalRes) {
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
 * Abstract class for reading documents from a test file and evaluating
 * on them.
 *
 * @tparam TEvalDoc Type of document to evaluate.
 * @tparam TEvalRes Type of result of evaluating a document.
 */
abstract class TestFileEvaluator[TEvalDoc, TEvalRes](val stratname: String) {
  var documents_processed = 0

  /**
   * Return an Iterable listing the documents retrievable from the given
   * filename.
   */
  def iter_documents(filehand: FileHandler,
    filename: String): Iterable[TEvalDoc]

  /**
   * Return true if document would be skipped; false if processed and
   * evaluated.
   */
  def would_skip_document(doc: TEvalDoc, doctag: String) = false

  /**
   * Return true if document was actually processed and evaluated; false
   * if skipped.
   */
  def evaluate_document(doc: TEvalDoc, doctag: String):
    TEvalRes

  /**
   * Output results so far.  If 'isfinal', this is the last call, so
   * output more results.
   */
  def output_results(isfinal: Boolean = false): Unit
}

abstract class GeolocateDocumentEvaluator[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc],
  TGrid <: CellGrid[TCoord, TDoc, TCell],
  TEvalDoc,
  TEvalRes
](
  val strategy: GeolocateDocumentStrategy[TCoord, TDoc, TCell, TGrid],
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends TestFileEvaluator[TEvalDoc, TEvalRes](stratname) with
  DefaultEvaluationOutputter[TEvalDoc, TEvalRes] {
  type TGroupedEvalStats <:
    GroupedGeolocateDocumentEvalStats[TCoord,TDoc,TCell]
  def create_grouped_eval_stats(driver: GeolocateDocumentTypeDriver,
    cell_grid: TGrid, results_by_range: Boolean):
    TGroupedEvalStats
  val evalstats = create_grouped_eval_stats(driver,
    strategy.cell_grid, results_by_range = driver.params.results_by_range)

  def output_results(isfinal: Boolean = false) {
    evalstats.output_results(all_results = isfinal)
  }
}

case class DocumentEvaluationResult[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc]
](
  document: TDoc,
  pred_cell: TCell,
  true_rank: Int
) {
  val true_cell = pred_cell.cell_grid.find_best_cell_for_coord(document.coord)
  val num_docs_in_true_cell = true_cell.combined_dist.num_docs_for_word_dist
  val true_center = true_cell.get_center_coord()
  val true_truedist = document.distance_to_coord(true_center)
  val pred_center = pred_cell.get_center_coord()
  val pred_truedist = document.distance_to_coord(pred_center)
}

trait EvaluationOutputter[TEvalDoc, TEvalRes] {
  def evaluate_and_output_results(filehand: FileHandler,
    files: Iterable[String]): mutable.Map[TEvalDoc, TEvalRes]
}

trait DefaultEvaluationOutputter[TEvalDoc, TEvalRes] extends
    EvaluationOutputter[TEvalDoc, TEvalRes] {
  this:TestFileEvaluator[TEvalDoc, TEvalRes] =>
  /**
    Evaluate on all of the given files, outputting periodic results and
    results after all files are done.  If the evaluator uses documents as
    documents (so that it doesn't need any external test files), the value
    of 'files' should be a sequence of one item, which is null. (If an
    empty sequence is passed in, no evaluation will happen.)

    Also returns an object containing the results.
   */
  def evaluate_and_output_results(filehand: FileHandler,
      files: Iterable[String]) = {
    val results = mutable.Map[TEvalDoc, TEvalRes]()
    val task = new MeteredTask("document", "evaluating")
    var last_elapsed = 0.0
    var last_processed = 0
    var skip_initial = Params.skip_initial_test_docs
    var skip_n = 0

    class EvaluationFileProcessor extends FileProcessor {
      /* Process all documents in a given file.  If return value is false,
         processing was interrupted due to a limit being reached, and
         no more files should be processed. */
      def process_file(filehand: FileHandler, filename: String): Boolean = {
        for (doc <- iter_documents(filehand, filename)) {
          // errprint("Processing document: %s", doc)
          val num_processed = task.num_processed
          val doctag = "#%d" format (1 + num_processed)
          if (would_skip_document(doc, doctag))
            errprint("Skipped document %s", doc)
          else {
            var do_skip = false
            if (skip_initial != 0) {
              skip_initial -= 1
              do_skip = true
            } else if (skip_n != 0) {
              skip_n -= 1
              do_skip = true
            } else
              skip_n = Params.every_nth_test_doc - 1
            if (do_skip)
              errprint("Passed over document %s", doctag)
            else {
              // Don't put side-effecting code inside of an assert!
              val result = evaluate_document(doc, doctag)
              assert(result != null)
              results(doc) = result
            }
            task.item_processed()
            val new_elapsed = task.elapsed_time
            val new_processed = task.num_processed

            // If max # of docs reached, stop
            if ((Params.num_test_docs > 0 &&
              new_processed >= Params.num_test_docs)) {
              errprint("")
              errprint("Stopping because limit of %s documents reached",
                Params.num_test_docs)
              task.finish()
              return false
            }

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
          }
        }

        return true
      }
    }

    new EvaluationFileProcessor().process_files(filehand, files)

    task.finish()

    errprint("")
    errprint("Final results for strategy %s: All %d documents processed:",
      stratname, task.num_processed)
    errprint("Ending operation at %s", curtimehuman())
    output_results(isfinal = true)
    errprint("Ending final results for strategy %s", stratname)
    results
  }
}

