///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.geolocate

import math._
import collection.mutable
import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.mathutil._
import opennlp.textgrounder.util.ioutil.{FileHandler, FileProcessor}
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.osutil._
import opennlp.textgrounder.util.printutil.{errprint, warning}
import opennlp.textgrounder.util.textutil._

import GeolocateDriver.Params
import GeolocateDriver.Debug._

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

class GeolocateDocumentEvalStats(
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

  protected def km_and_miles(kmdist: Double) = {
    "%.2f km (%.2f miles)" format (kmdist, kmdist / km_per_mile)
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
    errprint("  Mean true error distance = %s",
      km_and_miles(mean(true_dists)))
    errprint("  Median true error distance = %s",
      km_and_miles(median(true_dists)))
    errprint("  Mean oracle true error distance = %s",
      km_and_miles(mean(oracle_true_dists)))
  }
}

class SphereGeolocateDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  prefix: String,
  max_rank_for_credit: Int = 10
) extends GeolocateDocumentEvalStats(
  driver_stats, prefix, max_rank_for_credit) {
  // "True dist" means actual distance in km's or whatever.
  // "Degree dist" is the distance in degrees.
  val degree_dists = mutable.Buffer[Double]()
  val oracle_degree_dists = mutable.Buffer[Double]()

  def record_result(rank: Int, pred_true_dist: Double,
      pred_degree_dist: Double) {
    super.record_result(rank, pred_true_dist)
    degree_dists += pred_degree_dist
  }

  def record_oracle_result(oracle_true_dist: Double,
      oracle_degree_dist: Double) {
    super.record_oracle_result(oracle_true_dist)
    oracle_degree_dists += oracle_degree_dist
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
    errprint("  Mean degree error distance = %.2f degrees",
      mean(degree_dists))
    errprint("  Median degree error distance = %.2f degrees",
      median(degree_dists))
    errprint("  Median oracle true error distance = %s",
      km_and_miles(median(oracle_true_dists)))
  }
}

/**
 * Class for statistics for geolocating documents, with separate
 * sets of statistics for different intervals of error distances and
 * number of documents in true cell.
 */

abstract class GroupedGeolocateDocumentEvalStats[CoordType,
  DocumentType <: DistDocument[CoordType],
  CellType <: GeoCell[CoordType, DocumentType]](
  driver_stats: ExperimentDriverStats,
  cell_grid: CellGrid[CoordType,DocumentType,CellType],
  results_by_range: Boolean
) {
  type BasicEvalStatsType <: GeolocateDocumentEvalStats
  type DocumentEvaluationResultType <:
    DocumentEvaluationResult[CoordType, DocumentType, CellType]

  def create_stats(prefix: String): BasicEvalStatsType
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
    new SettingDefaultHashMap[Double, BasicEvalStatsType](
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

  def record_one_result(stats: BasicEvalStatsType, res: DocumentEvaluationResultType) {
    stats.record_result(res.true_rank, res.pred_truedist)
  }

  def record_one_oracle_result(stats: BasicEvalStatsType, res: DocumentEvaluationResultType) {
    stats.record_oracle_result(res.pred_truedist)
  }

  def record_result(res: DocumentEvaluationResultType) {
    record_one_result(all_document, res)
    record_one_oracle_result(all_document, res)
    // Stephen says recording so many counters leads to crashes (at the 51st
    // counter or something), so don't do it unless called for.
    if (results_by_range)
      record_result_by_range(res)
  }

  def record_result_by_range(res: DocumentEvaluationResultType) {
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

class SphereGroupedGeolocateDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  cell_grid: SphereCellGrid,
  results_by_range: Boolean
) extends GroupedGeolocateDocumentEvalStats[
  SphereCoord, SphereDocument, SphereCell](
  driver_stats, cell_grid, results_by_range) {
  type BasicEvalStatsType = SphereGeolocateDocumentEvalStats
  type DocumentEvaluationResultType = SphereDocumentEvaluationResult
  override def create_stats(prefix: String) =
    new SphereGeolocateDocumentEvalStats(driver_stats, prefix)

  val docs_by_degree_dist_to_true_center =
    docmap("degree_dist_to_true_center")

  val docs_by_degree_dist_to_pred_center =
    new DoubleTableByRange(dist_fractions_for_error_dist,
      create_stats_for_range("degree_dist_to_pred_center", _))

  override def record_one_result(stats: BasicEvalStatsType,
      res: DocumentEvaluationResultType) {
    stats.record_result(res.true_rank, res.pred_truedist, res.pred_degdist)
  }

  override def record_one_oracle_result(stats: BasicEvalStatsType,
      res: DocumentEvaluationResultType) {
    stats.record_oracle_result(res.pred_truedist, res.pred_degdist)
  }

  override def record_result_by_range(res: DocumentEvaluationResultType) {
    super.record_result_by_range(res)

    /* FIXME: This code specific to MultiRegularCellGrid is kind of ugly.
       Perhaps it should go elsewhere.

       FIXME: Also note that we don't actually make use of the info we
       record here. See below.
     */
    if (cell_grid.isInstanceOf[MultiRegularCellGrid]) {
      val multigrid = cell_grid.asInstanceOf[MultiRegularCellGrid]

      /* For distance to center of true cell, which will be small (no more
         than width_of_multi_cell * size-of-tiling-cell); we convert to
         fractions of tiling-cell size and record in ranges corresponding
         to increments of 0.25 (see above). */
      /* True distance (in both km and degrees) as a fraction of
         cell size */
      val frac_true_truedist = res.true_truedist / multigrid.km_per_cell
      val frac_true_degdist = res.true_degdist / multigrid.degrees_per_cell
      /* Round the fractional distances to multiples of
         dist_fraction_increment */
      val fracinc = dist_fraction_increment
      val rounded_frac_true_truedist =
        fracinc * floor(frac_true_degdist / fracinc)
      val rounded_frac_true_degdist =
        fracinc * floor(frac_true_degdist / fracinc)
      docs_by_true_dist_to_true_center(rounded_frac_true_truedist).
        record_result(res.true_rank, res.pred_truedist, res.pred_degdist)
      docs_by_degree_dist_to_true_center(rounded_frac_true_degdist).
        record_result(res.true_rank, res.pred_truedist, res.pred_degdist)

      /* For distance to center of predicted cell, which may be large, since
         predicted cell may be nowhere near the true cell.  Again we convert
         to fractions of tiling-cell size and record in the ranges listed in
         dist_fractions_for_error_dist (see above). */
      /* Predicted distance (in both km and degrees) as a fraction of
         cell size */
      val frac_pred_truedist = res.pred_truedist / multigrid.km_per_cell
      val frac_pred_degdist = res.pred_degdist / multigrid.degrees_per_cell
      docs_by_true_dist_to_pred_center.get_collector(frac_pred_truedist).
        record_result(res.true_rank, res.pred_truedist, res.pred_degdist)
      docs_by_degree_dist_to_pred_center.get_collector(frac_pred_degdist).
        record_result(res.true_rank, res.pred_truedist, res.pred_degdist)
     } else if (cell_grid.isInstanceOf[KdTreeCellGrid]) {
       // for kd trees, we do something similar to above, but round to the nearest km...
       val kdgrid = cell_grid.asInstanceOf[KdTreeCellGrid]
       all_document.record_oracle_result(res.true_truedist, res.true_degdist)
       docs_by_true_dist_to_true_center(round(res.true_truedist)).
         record_result(res.true_rank, res.pred_truedist, res.pred_degdist)
       docs_by_degree_dist_to_true_center(round(res.true_degdist)).
         record_result(res.true_rank, res.pred_truedist, res.pred_degdist)
    }
  }

  override def output_results_by_range() {
    super.output_results_by_range()
    errprint("")

    if (cell_grid.isInstanceOf[MultiRegularCellGrid]) {
      val multigrid = cell_grid.asInstanceOf[MultiRegularCellGrid]

      for (
        (frac_truedist, obj) <-
          docs_by_true_dist_to_true_center.toSeq sortBy (_._1)
      ) {
        val lowrange = frac_truedist * multigrid.km_per_cell
        val highrange = ((frac_truedist + dist_fraction_increment) *
          multigrid.km_per_cell)
        errprint("")
        errprint("Results for documents where distance to center")
        errprint("  of true cell in km is in the range [%.2f,%.2f):",
          lowrange, highrange)
        obj.output_results()
      }
      errprint("")
      for (
        (frac_degdist, obj) <-
          docs_by_degree_dist_to_true_center.toSeq sortBy (_._1)
      ) {
        val lowrange = frac_degdist * multigrid.degrees_per_cell
        val highrange = ((frac_degdist + dist_fraction_increment) *
          multigrid.degrees_per_cell)
        errprint("")
        errprint("Results for documents where distance to center")
        errprint("  of true cell in degrees is in the range [%.2f,%.2f):",
          lowrange, highrange)
        obj.output_results()
      }
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Main evaluation code                        //
/////////////////////////////////////////////////////////////////////////////

/**
 * General trait for classes representing documents to evaluate.
 */
trait EvaluationDocument {
}

/**
 * General trait for classes representing result of evaluating a document.
 */
trait EvaluationResult {
}

/**
 * Abstract class for reading documents from a test file and evaluating
 * on them.
 */
abstract class TestFileEvaluator(val stratname: String) {
  var documents_processed = 0

  type EvalDocumentType <: EvaluationDocument
  type EvalResultType <: EvaluationResult

  /**
   * Return an Iterable listing the documents retrievable from the given
   * filename.
   */
  def iter_documents(filehand: FileHandler,
    filename: String): Iterable[EvalDocumentType]

  /**
   * Return true if document would be skipped; false if processed and
   * evaluated.
   */
  def would_skip_document(doc: EvalDocumentType, doctag: String) = false

  /**
   * Return true if document was actually processed and evaluated; false
   * if skipped.
   */
  def evaluate_document(doc: EvalDocumentType, doctag: String):
    EvalResultType

  /**
   * Output results so far.  If 'isfinal', this is the last call, so
   * output more results.
   */
  def output_results(isfinal: Boolean = false): Unit
}

abstract class GeolocateDocumentEvaluator[CoordType,
    DocumentType <: DistDocument[CoordType],
    CellType <: GeoCell[CoordType, DocumentType],
    CellGridType <: CellGrid[CoordType, DocumentType, CellType]](
  val strategy: GeolocateDocumentStrategy[CoordType, DocumentType, CellType,
    CellGridType],
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends TestFileEvaluator(stratname) {
  type GroupedEvalStatsType <:
    GroupedGeolocateDocumentEvalStats[CoordType,DocumentType,CellType]
  def create_grouped_eval_stats(driver: GeolocateDocumentTypeDriver,
    cell_grid: CellGridType, results_by_range: Boolean):
    GroupedEvalStatsType
  val evalstats = create_grouped_eval_stats(driver,
    strategy.cell_grid, results_by_range = driver.params.results_by_range)

  def output_results(isfinal: Boolean = false) {
    evalstats.output_results(all_results = isfinal)
  }
}

abstract class SphereGeolocateDocumentEvaluator(
  override val strategy: SphereGeolocateDocumentStrategy,
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends GeolocateDocumentEvaluator[SphereCoord, SphereDocument, SphereCell,
  SphereCellGrid](strategy, stratname, driver) {
  type GroupedEvalStatsType = SphereGroupedGeolocateDocumentEvalStats
  def create_grouped_eval_stats(driver: GeolocateDocumentTypeDriver,
    cell_grid: SphereCellGrid, results_by_range: Boolean) =
    new GroupedEvalStatsType(driver, cell_grid.asInstanceOf[SphereCellGrid],
      results_by_range)
}

case class DocumentEvaluationResult[CoordType,
    DocumentType <: DistDocument[CoordType],
    CellType <: GeoCell[CoordType, DocumentType]](
  document: DocumentType,
  pred_cell: CellType,
  true_rank: Int
) extends EvaluationResult {
  val true_cell = pred_cell.cell_grid.find_best_cell_for_coord(document.coord)
  val num_docs_in_true_cell = true_cell.word_dist_wrapper.num_docs_for_word_dist
  val true_center = true_cell.get_center_coord()
  val true_truedist = document.distance_to_coord(true_center)
  val pred_center = pred_cell.get_center_coord()
  val pred_truedist = document.distance_to_coord(pred_center)
}

class SphereDocumentEvaluationResult(
  document: SphereDocument,
  pred_cell: SphereCell,
  true_rank: Int
) extends DocumentEvaluationResult[SphereCoord, SphereDocument, SphereCell](
  document, pred_cell, true_rank
) {
  val true_degdist = document.degree_distance_to_coord(true_center)
  val pred_degdist = document.degree_distance_to_coord(pred_center)
}


/**
  Class to do document geolocating on documents from the document data, in
  the dev or test set.
 */
class InternalGeolocateDocumentEvaluator(
  strategy: SphereGeolocateDocumentStrategy,
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends SphereGeolocateDocumentEvaluator(strategy, stratname, driver) {

  type EvalDocumentType = SphereDocument
  type EvalResultType = SphereDocumentEvaluationResult

  def iter_documents(filehand: FileHandler, filename: String) = {
    assert(filename == null)
    for (doc <- driver.document_table.documents_by_split(driver.params.eval_set))
      yield doc
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

  override def would_skip_document(document: EvalDocumentType, doctag: String) = {
    if (document.dist == null) {
      // This can (and does) happen when --max-time-per-stage is set,
      // so that the counts for many documents don't get read in.
      if (driver.params.max_time_per_stage == 0.0 && driver.params.num_training_docs == 0)
        warning("Can't evaluate document %s without distribution", document)
      true
    } else false
  }

  def evaluate_document(document: EvalDocumentType, doctag: String):
      EvalResultType = {
    if (would_skip_document(document, doctag)) {
      evalstats.increment_counter("documents.skipped")
      return null
    }
    assert(document.dist.finished)
    val true_cell =
      strategy.cell_grid.find_best_cell_for_coord(document.coord)
    if (debug("lots") || debug("commontop")) {
      val naitr = true_cell.word_dist_wrapper.num_docs_for_word_dist
      errprint("Evaluating document %s with %s word-dist documents in true cell",
        document, naitr)
    }

    /* That is:

       pred_cells = List of predicted cells, from best to worst; each list
          entry is actually a tuple of (cell, score) where lower scores
          are better
       true_rank = Rank of true cell among predicted cells
     */
    val (pred_cells, true_rank) =
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
    val result =
      new SphereDocumentEvaluationResult(document, pred_cells(0)._1, true_rank)

    val want_indiv_results =
      !driver.params.oracle_results && !driver.params.no_individual_results
    evalstats.record_result(result)
    if (result.num_docs_in_true_cell == 0) {
      evalstats.increment_counter("documents.no_training_documents_in_cell")
    }
    if (want_indiv_results) {
      errprint("%s:Document %s:", doctag, document)
      // errprint("%s:Document distribution: %s", doctag, document.dist)
      errprint("%s:  %d types, %d tokens",
        doctag, document.dist.num_word_types, document.dist.num_word_tokens)
      errprint("%s:  true cell at rank: %s", doctag, true_rank)
      errprint("%s:  true cell: %s", doctag, result.true_cell)
      for (i <- 0 until 5) {
        errprint("%s:  Predicted cell (at rank %s): %s",
          doctag, i + 1, pred_cells(i)._1)
      }
      errprint("%s:  Distance %.2f km to true cell center at %s",
        doctag, result.true_truedist, result.true_center)
      errprint("%s:  Distance %.2f km to predicted cell center at %s",
        doctag, result.pred_truedist, result.pred_center)
      assert(doctag(0) == '#')
      if (debug("gridrank") ||
        (debuglist("gridrank") contains doctag.drop(1))) {
        val grsize = debugval("gridranksize").toInt
        if (!true_cell.isInstanceOf[MultiRegularCell])
          warning("Can't output ranking grid, cell not of right type")
        else {
          strategy.cell_grid.asInstanceOf[MultiRegularCellGrid].
            output_ranking_grid(
              pred_cells.asInstanceOf[Seq[(MultiRegularCell, Double)]],
              true_cell.asInstanceOf[MultiRegularCell], grsize)
        }
      }
    }

    return result
  }
}

class TitledDocumentResult extends EvaluationResult {
}

/**
 * A class for geolocation where each test document is a chapter in a book
 * in the PCL Travel corpus.
 */
class PCLTravelGeolocateDocumentEvaluator(
  strategy: SphereGeolocateDocumentStrategy,
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends SphereGeolocateDocumentEvaluator(strategy, stratname, driver) {
  case class TitledDocument(
    title: String, text: String) extends EvaluationDocument 
  type EvalDocumentType = TitledDocument
  type EvalResultType = TitledDocumentResult

  def iter_documents(filehand: FileHandler, filename: String) = {

    val dom = try {
      // On error, just return, so that we don't have problems when called
      // on the whole PCL corpus dir (which includes non-XML files).
      // FIXME!! Needs to use the FileHandler somehow for Hadoop access.
      xml.XML.loadFile(filename)
    } catch {
      case _ => {
        warning("Unable to parse XML filename: %s", filename)
        null
      }
    }

    if (dom == null) Seq[TitledDocument]()
    else for {
      chapter <- dom \\ "div" if (chapter \ "@type").text == "chapter"
      val (heads, nonheads) = chapter.child.partition(_.label == "head")
      val headtext = (for (x <- heads) yield x.text) mkString ""
      val text = (for (x <- nonheads) yield x.text) mkString ""
      //errprint("Head text: %s", headtext)
      //errprint("Non-head text: %s", text)
    } yield TitledDocument(headtext, text)
  }

  def evaluate_document(doc: TitledDocument, doctag: String) = {
    val dist = driver.word_dist_factory.create_word_dist()
    val the_stopwords =
      if (driver.params.include_stopwords_in_document_dists) Set[String]()
      else driver.stopwords
    for (text <- Seq(doc.title, doc.text)) {
      dist.add_document(split_text_into_words(text, ignore_punc = true),
        ignore_case = !driver.params.preserve_case_words,
        stopwords = the_stopwords)
    }
    dist.finish(minimum_word_count = driver.params.minimum_word_count)
    val cells = strategy.return_ranked_cells(dist)
    errprint("")
    errprint("Document with title: %s", doc.title)
    val num_cells_to_show = 5
    for ((rank, cellval) <- (1 to num_cells_to_show) zip cells) {
      val (cell, vall) = cellval
      if (debug("pcl-travel")) {
        errprint("  Rank %d, goodness %g:", rank, vall)
        errprint(cell.struct().toString) // indent=4
      } else
        errprint("  Rank %d, goodness %g: %s", rank, vall, cell.shortstr())
    }

    new TitledDocumentResult()
  }
}

abstract class EvaluationOutputter {
  def evaluate_and_output_results(filehand: FileHandler,
    files: Iterable[String]): Unit
}

class DefaultEvaluationOutputter(
  val stratname: String,
  val evalobj: TestFileEvaluator
) extends EvaluationOutputter {
  val results = mutable.Map[EvaluationDocument, EvaluationResult]()
  /**
    Evaluate on all of the given files, outputting periodic results and
    results after all files are done.  If the evaluator uses documents as
    documents (so that it doesn't need any external test files), the value
    of 'files' should be a sequence of one item, which is null. (If an
    empty sequence is passed in, no evaluation will happen.)

    Also returns an object containing the results.
   */
  def evaluate_and_output_results(filehand: FileHandler,
      files: Iterable[String]) {
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
        for (doc <- evalobj.iter_documents(filehand, filename)) {
          // errprint("Processing document: %s", doc)
          val num_processed = task.num_processed
          val doctag = "#%d" format (1 + num_processed)
          if (evalobj.would_skip_document(doc, doctag))
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
              val result = evalobj.evaluate_document(doc, doctag)
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
              evalobj.output_results(isfinal = false)
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
    evalobj.output_results(isfinal = true)
    errprint("Ending final results for strategy %s", stratname)
  }
}

