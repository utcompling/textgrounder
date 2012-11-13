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

import opennlp.textgrounder.{util => tgutil}
import tgutil.collectionutil.{DoubleTableByRange}
import tgutil.distances._
import tgutil.experiment.ExperimentDriverStats
import tgutil.mathutil.{mean, median}
import tgutil.ioutil.{FileHandler}
import tgutil.osutil.output_resource_usage
import tgutil.printutil.{errprint, warning}
import tgutil.textutil.split_text_into_words

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
  create_stats: (ExperimentDriverStats, String) => DocumentEvalStats
) extends GroupedDocumentEvalStats[SphereCoord](
  driver_stats, cell_grid, results_by_range, create_stats
) {
  val docs_by_degree_dist_to_true_center =
    docmap("degree_dist_to_true_center")

  val docs_by_degree_dist_to_pred_center =
    new DoubleTableByRange(dist_fractions_for_error_dist,
      create_stats_for_range("degree_dist_to_pred_center", _))

  override def record_one_result(stats: DocumentEvalStats,
      res: DocumentEvaluationResult[SphereCoord]) {
    super.record_one_result(stats, res)
    stats.asInstanceOf[SphereDocumentEvalStats].
      record_predicted_degree_distance(res.pred_degdist)
  }

  override def record_one_oracle_result(stats: DocumentEvalStats,
      res: DocumentEvaluationResult[SphereCoord]) {
    super.record_one_oracle_result(stats, res)
    stats.asInstanceOf[SphereDocumentEvalStats].
      record_oracle_degree_distance(res.true_degdist)
  }

  override def record_result_by_range(
    res: DocumentEvaluationResult[SphereCoord]
  ) {
    super.record_result_by_range(res)

    /* FIXME: This code specific to MultiRegularCellGrid is kind of ugly.
       Perhaps it should go elsewhere.

       FIXME: Also note that we don't actually make use of the info we
       record here. See below.
     */
    cell_grid match {
      case multigrid: MultiRegularCellGrid => {
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
       }

      case kdgrid: KdTreeCellGrid => {
        // for kd trees, we do something similar to above,
        // but round to the nearest km...
        res.record_result(docs_by_true_dist_to_true_center(
          round(res.true_truedist)))
        res.record_result(docs_by_degree_dist_to_true_center(
         round(res.true_degdist)))
      }
    }
  }

  override def output_results_by_range() {
    super.output_results_by_range()
    errprint("")

    cell_grid match {
      case multigrid: MultiRegularCellGrid => {
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
}

/////////////////////////////////////////////////////////////////////////////
//                             Main evaluation code                        //
/////////////////////////////////////////////////////////////////////////////

/**
 * Specialization of `RankedCellGridEvaluator` for SphereCoords (latitude/
 * longitude coordinates on the surface of a sphere).  Class for evaluating
 * (geolocating) a test document using a strategy that ranks the cells in the
 * cell grid and picks the central point of the top-ranked one.
 *
 * Only needed to support debug("gridrank").
 */
class RankedSphereCellGridEvaluator(
  strategy: GridLocateDocumentStrategy[SphereCoord],
  stratname: String,
  driver: GeolocateDocumentTypeDriver,
  evalstats: GroupedDocumentEvalStats[SphereCoord]
) extends RankedCellGridEvaluator[SphereCoord](
  strategy, stratname, driver, evalstats
) {
  override def print_individual_result(doctag: String,
      document: SphereDocument,
      result: TEvalRes,
      pred_cells: Iterable[(SphereCell, Double)]) {
    super.print_individual_result(doctag, document, result, pred_cells)

    assert(doctag(0) == '#')
    if (debug("gridrank") ||
      (debuglist("gridrank") contains doctag.drop(1))) {
      val grsize = debugval("gridranksize").toInt
      result.true_cell match {
        case multireg: MultiRegularCell =>
          strategy.cell_grid.asInstanceOf[MultiRegularCellGrid].
            output_ranking_grid(
              pred_cells.asInstanceOf[Iterable[(MultiRegularCell, Double)]],
              multireg, grsize)
        case _ =>
          warning("Can't output ranking grid, cell not of right type")
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
  strategy: GridLocateDocumentStrategy[SphereCoord],
  stratname: String,
  driver: GeolocateDocumentTypeDriver,
  filehand: FileHandler,
  filenames: Iterable[String]
) extends CorpusEvaluator(stratname, driver) {
  type TEvalDoc = TitledDocument
  type TEvalRes = TitledDocumentResult
  def iter_documents = {
    filenames.toIterator.flatMap(filename => {
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

      if (dom == null) Iterator[Nothing]()
      else (for {
        chapter <- dom \\ "div" if (chapter \ "@type").text == "chapter"
        val (heads, nonheads) = chapter.child.partition(_.label == "head")
        val headtext = (for (x <- heads) yield x.text) mkString ""
        val text = (for (x <- nonheads) yield x.text) mkString ""
        //errprint("Head text: %s", headtext)
        //errprint("Non-head text: %s", text)
      } yield (filehand, filename, TitledDocument(headtext, text))).toIterator
    })
  }

  def iter_document_stats = iter_documents.map {
    case (filehand, filename, doc) =>
      new DocumentStatus(filehand, filename, Some(doc), "processed", "", "")
  }

  def evaluate_document(doc: TitledDocument, doctag: String) = {
    val dist = driver.word_dist_factory.create_word_dist()
    for (text <- Seq(doc.title, doc.text))
      dist.add_document(split_text_into_words(text, ignore_punc = true))
    dist.finish_before_global()
    dist.finish_after_global()
    val cells =
      strategy.return_ranked_cells(dist, include = Iterable[SphereCell]())
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

