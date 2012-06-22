///////////////////////////////////////////////////////////////////////////////
//  SphereEvaluation.scala
//
//  Copyright (C) 2010, 2011, 2012 Ben Wing, The University of Texas at Austin
//  Copyright (C) 2012 Mike Speriosu, The University of Texas at Austin
//  Copyright (C) 2011 Stephen Roller, The University of Texas at Austin
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

/**
 * A general trait for encapsulating SphereDocument-specific behavior.
 * In this case, this is largely the computation of "degree distances" in
 * addition to "true distances", and making sure results are output in
 * miles and km.
 */
trait SphereDocumentEvalStats extends DocumentEvalStats {
  // "True dist" means actual distance in km's or whatever.
  // "Degree dist" is the distance in degrees.
  val degree_dists = mutable.Buffer[Double]()
  val oracle_degree_dists = mutable.Buffer[Double]()

  def record_predicted_degree_distance(pred_degree_dist: Double) {
    degree_dists += pred_degree_dist
  }

  def record_oracle_degree_distance(oracle_degree_dist: Double) {
    oracle_degree_dists += oracle_degree_dist
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
 * SphereDocument version of `CoordDocumentEvalStats`.
 */
class CoordSphereDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  prefix: String
) extends CoordDocumentEvalStats(driver_stats, prefix)
  with SphereDocumentEvalStats {
}

/**
 * SphereDocument version of `RankedDocumentEvalStats`.
 */
class RankedSphereDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  prefix: String,
  max_rank_for_credit: Int = 10
) extends RankedDocumentEvalStats(driver_stats, prefix, max_rank_for_credit)
  with SphereDocumentEvalStats {
}

/**
 * SphereDocument version of `GroupedDocumentEvalStats`.  This keeps separate
 * sets of statistics for different subgroups of the test documents, i.e.
 * those within particular ranges of one or more quantities of interest.
 */
class GroupedSphereDocumentEvalStats(
  driver_stats: ExperimentDriverStats,
  cell_grid: SphereCellGrid,
  results_by_range: Boolean,
  is_ranked: Boolean
) extends GroupedDocumentEvalStats[
  SphereCoord, SphereDocument, SphereCell, SphereCellGrid,
  SphereDocumentEvaluationResult
](driver_stats, cell_grid, results_by_range) {
  override def create_stats(prefix: String) = {
    if (is_ranked)
      new RankedSphereDocumentEvalStats(driver_stats, prefix)
    else
      new CoordSphereDocumentEvalStats(driver_stats, prefix)
  }

  val docs_by_degree_dist_to_true_center =
    docmap("degree_dist_to_true_center")

  val docs_by_degree_dist_to_pred_center =
    new DoubleTableByRange(dist_fractions_for_error_dist,
      create_stats_for_range("degree_dist_to_pred_center", _))

  override def record_one_result(stats: DocumentEvalStats,
      res: SphereDocumentEvaluationResult) {
    super.record_one_result(stats, res)
    stats.asInstanceOf[SphereDocumentEvalStats].
      record_predicted_degree_distance(res.pred_degdist)
  }

  override def record_one_oracle_result(stats: DocumentEvalStats,
      res: SphereDocumentEvaluationResult) {
    super.record_one_oracle_result(stats, res)
    stats.asInstanceOf[SphereDocumentEvalStats].
      record_oracle_degree_distance(res.true_degdist)
  }

  override def record_result_by_range(res: SphereDocumentEvaluationResult) {
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
      res.record_result(docs_by_true_dist_to_true_center(
        rounded_frac_true_truedist))
      res.record_result(docs_by_degree_dist_to_true_center(
        rounded_frac_true_degdist))

      /* For distance to center of predicted cell, which may be large, since
         predicted cell may be nowhere near the true cell.  Again we convert
         to fractions of tiling-cell size and record in the ranges listed in
         dist_fractions_for_error_dist (see above). */
      /* Predicted distance (in both km and degrees) as a fraction of
         cell size */
      val frac_pred_truedist = res.pred_truedist / multigrid.km_per_cell
      val frac_pred_degdist = res.pred_degdist / multigrid.degrees_per_cell
      res.record_result(docs_by_true_dist_to_pred_center.get_collector(
        frac_pred_truedist))
      res.record_result(docs_by_degree_dist_to_pred_center.get_collector(
        frac_pred_degdist))
     } else if (cell_grid.isInstanceOf[KdTreeCellGrid]) {
       // for kd trees, we do something similar to above, but round to the nearest km...
       val kdgrid = cell_grid.asInstanceOf[KdTreeCellGrid]
       res.record_result(docs_by_true_dist_to_true_center(
         round(res.true_truedist)))
       res.record_result(docs_by_degree_dist_to_true_center(
         round(res.true_degdist)))
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
 * A general trait holding SphereDocument-specific code for storing the
 * result of evaluation on a document.  Here we simply compute the
 * true and predicted "degree distances" -- i.e. measured in degrees,
 * rather than in actual distance along a great circle.
 */
trait SphereDocumentEvaluationResult extends DocumentEvaluationResult[
  SphereCoord, SphereDocument, SphereCell, SphereCellGrid
] {
  val xdocument: SphereDocument
  /* The following must be declared as 'lazy' because 'xdocument' above isn't
     initialized at creation time (which is impossible because traits can't
     have construction parameters). */
  /**
   * Distance in degrees between document's coordinate and central
   * point of true cell
   */
  lazy val true_degdist = xdocument.degree_distance_to_coord(true_center)
  /**
   * Distance in degrees between document's coordinate and predicted
   * coordinate
   */
  lazy val pred_degdist = xdocument.degree_distance_to_coord(pred_coord)
}

/**
 * Result of evaluating a SphereDocument using an algorithm that does
 * cell-by-cell comparison and computes a ranking of all the cells.
 * The predicted coordinate is the central point of the top-ranked cell,
 * and the cell grid is derived from the cell.
 *
 * @param document document whose coordinate is predicted
 * @param pred_cell top-ranked predicted cell in which the document should
 *        belong
 * @param true_rank rank of the document's true cell among all of the
 *        predicted cell
 */
class RankedSphereDocumentEvaluationResult(
  document: SphereDocument,
  pred_cell: SphereCell,
  true_rank: Int
) extends RankedDocumentEvaluationResult[
  SphereCoord, SphereDocument, SphereCell, SphereCellGrid
  ](
  document, pred_cell, true_rank
) with SphereDocumentEvaluationResult {
  val xdocument = document
}

/**
 * Result of evaluating a SphereDocument using an algorithm that
 * predicts a coordinate that is not necessarily the central point of
 * any cell (e.g. using a mean-shift algorithm).
 *
 * @param document document whose coordinate is predicted
 * @param cell_grid cell grid against which error comparison should be done
 * @param pred_coord predicted coordinate of the document
 */
class CoordSphereDocumentEvaluationResult(
  document: SphereDocument,
  cell_grid: SphereCellGrid,
  pred_coord: SphereCoord
) extends CoordDocumentEvaluationResult[
  SphereCoord, SphereDocument, SphereCell, SphereCellGrid
  ](
  document, cell_grid, pred_coord
) with SphereDocumentEvaluationResult {
  val xdocument = document
}

/**
 * Specialization of `RankedCellGridEvaluator` for SphereCoords (latitude/
 * longitude coordinates on the surface of a sphere).  Class for evaluating
 * (geolocating) a test document using a strategy that ranks the cells in the
 * cell grid and picks the central point of the top-ranked one.
 */
class RankedSphereCellGridEvaluator(
  strategy: GridLocateDocumentStrategy[SphereCell, SphereCellGrid],
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends RankedCellGridEvaluator[
  SphereCoord, SphereDocument, SphereCell, SphereCellGrid,
  SphereDocumentEvaluationResult
](strategy, stratname, driver) {
  def create_grouped_eval_stats(driver: GridLocateDriver,
    cell_grid: SphereCellGrid, results_by_range: Boolean) =
    new GroupedSphereDocumentEvalStats(
      driver, cell_grid, results_by_range, is_ranked = true)
  def create_cell_evaluation_result(document: SphereDocument,
      pred_cell: SphereCell, true_rank: Int) =
    new RankedSphereDocumentEvaluationResult(document, pred_cell, true_rank)

  override def print_individual_result(doctag: String, document: SphereDocument,
      result: SphereDocumentEvaluationResult,
      pred_cells: Array[(SphereCell, Double)]) {
    super.print_individual_result(doctag, document, result, pred_cells)

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

/**
 * Specialization of `MeanShiftCellGridEvaluator` for SphereCoords (latitude/
 * longitude coordinates on the surface of a sphere).  Class for evaluating
 * (geolocating) a test document using a mean-shift strategy, i.e. picking the
 * K-best-ranked cells and using the mean-shift algorithm to derive a single
 * point that hopefully should be in the center of the largest cluster.
 */
class MeanShiftSphereCellGridEvaluator(
  strategy: GridLocateDocumentStrategy[SphereCell, SphereCellGrid],
  stratname: String,
  driver: GeolocateDocumentTypeDriver,
  k_best: Int,
  mean_shift_window: Double,
  mean_shift_max_stddev: Double,
  mean_shift_max_iterations: Int
) extends MeanShiftCellGridEvaluator[
  SphereCoord, SphereDocument, SphereCell, SphereCellGrid,
  SphereDocumentEvaluationResult
](strategy, stratname, driver, k_best, mean_shift_window,
  mean_shift_max_stddev, mean_shift_max_iterations) {
  def create_grouped_eval_stats(driver: GridLocateDriver,
    cell_grid: SphereCellGrid, results_by_range: Boolean) =
    new GroupedSphereDocumentEvalStats(
      driver, cell_grid, results_by_range, is_ranked = false)
  def create_coord_evaluation_result(document: SphereDocument,
      cell_grid: SphereCellGrid, pred_coord: SphereCoord) =
    new CoordSphereDocumentEvaluationResult(document, cell_grid, pred_coord)
  def create_mean_shift_obj(h: Double, max_stddev: Double,
    max_iterations: Int) = new SphereMeanShift(h, max_stddev, max_iterations)
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
) extends CorpusEvaluator[
  TitledDocument, TitledDocumentResult
](stratname, driver) with DocumentIteratingEvaluator[
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

  def output_results(isfinal: Boolean = false) {
  }
}

