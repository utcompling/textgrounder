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
//////// SphereEvaluation.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import math.{round, floor}
import collection.mutable
import util.control.Breaks._

import opennlp.textgrounder.util.collectionutil.{DoubleTableByRange}
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment.ExperimentDriverStats
import opennlp.textgrounder.util.mathutil.{mean, median}
import opennlp.textgrounder.util.ioutil.{FileHandler}
import opennlp.textgrounder.util.printutil.{errprint, warning}
import opennlp.textgrounder.util.textutil.split_text_into_words

import GeolocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                 General statistics on evaluation results                //
/////////////////////////////////////////////////////////////////////////////

//////// Statistics for geolocating documents

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

  protected def km_and_miles(kmdist: Double) = {
    "%.2f km (%.2f miles)" format (kmdist, kmdist / km_per_mile)
  }

  protected def output_result_with_units(kmdist: Double) = km_and_miles(kmdist)

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
    stats.record_oracle_result(res.true_truedist, res.true_degdist)
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
 * Class to do document geolocating on documents from the document data, in
 * the dev or test set.
 *
 * FIXME!! Should probably be generalized to work beyond simply SphereDocuments
 * and such.
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

    if (debug("all-scores")) {
      for (((cell, value), index) <- pred_cells.zipWithIndex) {
        errprint("%s: %6d: Cell at %s: score = %g", doctag, index + 1,
          cell.describe_indices(), value)
      }
    }
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
        errprint(cell.struct.toString) // indent=4
      } else
        errprint("  Rank %d, goodness %g: %s", rank, vall, cell.shortstr)
    }

    new TitledDocumentResult()
  }
}

