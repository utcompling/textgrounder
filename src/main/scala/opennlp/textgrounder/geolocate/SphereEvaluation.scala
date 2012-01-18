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
import opennlp.textgrounder.util.osutil.output_resource_usage
import opennlp.textgrounder.util.printutil.{errprint, warning}
import opennlp.textgrounder.util.textutil.split_text_into_words

import opennlp.textgrounder.gridlocate._
import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                 General statistics on evaluation results                //
/////////////////////////////////////////////////////////////////////////////

//////// Statistics for geolocating documents

class SphereDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  prefix: String,
  max_rank_for_credit: Int = 10
) extends DocumentEvalStats(
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

class SphereGroupedDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  cell_grid: SphereCellGrid,
  results_by_range: Boolean
) extends GroupedDocumentEvalStats[
  SphereCoord, SphereDocument, SphereCell](
  driver_stats, cell_grid, results_by_range) {
  type TBasicEvalStats = SphereDocumentEvalStats
  type TDocEvalRes = SphereDocumentEvaluationResult
  override def create_stats(prefix: String) =
    new SphereDocumentEvalStats(driver_stats, prefix)

  val docs_by_degree_dist_to_true_center =
    docmap("degree_dist_to_true_center")

  val docs_by_degree_dist_to_pred_center =
    new DoubleTableByRange(dist_fractions_for_error_dist,
      create_stats_for_range("degree_dist_to_pred_center", _))

  override def record_one_result(stats: TBasicEvalStats,
      res: TDocEvalRes) {
    stats.record_result(res.true_rank, res.pred_truedist, res.pred_degdist)
  }

  override def record_one_oracle_result(stats: TBasicEvalStats,
      res: TDocEvalRes) {
    stats.record_oracle_result(res.true_truedist, res.true_degdist)
  }

  override def record_result_by_range(res: TDocEvalRes) {
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

abstract class GeolocateDocumentEvaluator[TEvalDoc, TEvalRes](
  strategy: GridLocateDocumentStrategy[SphereCell, SphereCellGrid],
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends DocumentEvaluator[SphereCoord, SphereDocument, SphereCell,
  SphereCellGrid, TEvalDoc, TEvalRes](strategy, stratname, driver) {
  type TGroupedEvalStats = SphereGroupedDocumentEvalStats
  def create_grouped_eval_stats(driver: GridLocateDriver,
    cell_grid: SphereCellGrid, results_by_range: Boolean) =
    new SphereGroupedDocumentEvalStats(driver,
      cell_grid.asInstanceOf[SphereCellGrid],
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
 */
class CorpusGeolocateDocumentEvaluator(
  strategy: GridLocateDocumentStrategy[SphereCell, SphereCellGrid],
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends CorpusDocumentEvaluator[
  SphereCoord, SphereDocument, SphereCell, SphereCellGrid,
  SphereDocumentEvaluationResult
](strategy, stratname, driver) {
  // FIXME, the following 8 lines are copied from GeolocateDocumentEvaluator
  type TGroupedEvalStats = SphereGroupedDocumentEvalStats
  def create_grouped_eval_stats(driver: GridLocateDriver,
    cell_grid: SphereCellGrid, results_by_range: Boolean) =
    new SphereGroupedDocumentEvalStats(driver,
      cell_grid.asInstanceOf[SphereCellGrid],
      results_by_range)
  def create_evaluation_result(document: SphereDocument, pred_cell: SphereCell,
      true_rank: Int) =
    new SphereDocumentEvaluationResult(document, pred_cell, true_rank)

  val num_nearest_neighbors = driver.params.num_nearest_neighbors

  def print_individual_result(doctag: String, document: SphereDocument,
      result: SphereDocumentEvaluationResult,
      pred_cells: Array[(SphereCell, Double)]) {
    errprint("%s:Document %s:", doctag, document)
    // errprint("%s:Document distribution: %s", doctag, document.dist)
    errprint("%s:  %d types, %f tokens",
      doctag, document.dist.num_word_types, document.dist.num_word_tokens)
    errprint("%s:  true cell at rank: %s", doctag, result.true_rank)
    errprint("%s:  true cell: %s", doctag, result.true_cell)
    for (i <- 0 until 5) {
      errprint("%s:  Predicted cell (at rank %s, kl-div %s): %s",
        doctag, i + 1, pred_cells(i)._2, pred_cells(i)._1)
    }

    //for (num_nearest_neighbors <- 2 to 100 by 2) {
    val kNN = pred_cells.take(num_nearest_neighbors).map(_._1)
    val kNNranks = pred_cells.take(num_nearest_neighbors).zipWithIndex.map(p => (p._1._1, p._2+1)).toMap
    val closest_half_with_dists = kNN.map(n => (n, spheredist(n.get_center_coord, document.coord))).sortWith(_._2 < _._2).take(num_nearest_neighbors/2)

    closest_half_with_dists.zipWithIndex.foreach(c => errprint("%s:  #%s close neighbor: %s; error distance: %.2f km",
      doctag, kNNranks(c._1._1), c._1._1.get_center_coord, c._1._2))

    errprint("%s:  Distance %.2f km to true cell center at %s",
      doctag, result.true_truedist, result.true_center)
    errprint("%s:  Distance %.2f km to predicted cell center at %s",
      doctag, result.pred_truedist, result.pred_center)

    val avg_dist_of_neighbors = mean(closest_half_with_dists.map(_._2))
    errprint("%s:  Average distance from true cell center to %s closest cells' centers from %s best matches: %.2f km",
      doctag, (num_nearest_neighbors/2), num_nearest_neighbors, avg_dist_of_neighbors)

    if(avg_dist_of_neighbors < result.pred_truedist)
      driver.increment_local_counter("instances.num_where_avg_dist_of_neighbors_beats_pred_truedist.%s" format num_nearest_neighbors)
    //}

  
    assert(doctag(0) == '#')
    if (debug("gridrank") ||
      (debuglist("gridrank") contains doctag.drop(1))) {
      val grsize = debugval("gridranksize").toInt
      if (!result.true_cell.isInstanceOf[MultiRegularCell])
        warning("Can't output ranking grid, cell not of right type")
      else {
        strategy.cell_grid.asInstanceOf[MultiRegularCellGrid].
          output_ranking_grid(
            pred_cells.asInstanceOf[Seq[(MultiRegularCell, Double)]],
            result.true_cell.asInstanceOf[MultiRegularCell], grsize)
      }
    }
  }
}

case class TitledDocument(title: String, text: String)
class TitledDocumentResult { }

/**
 * A class for geolocation where each test document is a chapter in a book
 * in the PCL Travel corpus.
 */
class PCLTravelGeolocateDocumentEvaluator(
  strategy: GridLocateDocumentStrategy[SphereCell, SphereCellGrid],
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends GeolocateDocumentEvaluator[
  TitledDocument, TitledDocumentResult
](strategy, stratname, driver) with DocumentIteratingEvaluator[
  TitledDocument, TitledDocumentResult
] {
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
    for (text <- Seq(doc.title, doc.text))
      dist.add_document(split_text_into_words(text, ignore_punc = true))
    dist.finish_before_global()
    dist.finish_after_global()
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

