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

package opennlp.textgrounder
package geolocate

import math.{round, floor}
import collection.mutable
import scala.util.control.Breaks._

import util.collection.{DoubleTableByRange}
import util.distances._
import util.experiment.ExperimentDriverStats
import util.math.{mean, median}
import util.io.{FileHandler}
import util.print.{errprint, warning}
import util.text.split_text_into_words

import gridlocate._
import gridlocate.GridLocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                 General statistics on evaluation results                //
/////////////////////////////////////////////////////////////////////////////

//////// Statistics for geolocating documents

/**
 * A general trait for encapsulating SphereDoc-specific behavior.
 * In this case, this is largely the computation of "degree distances" in
 * addition to "true distances", and making sure results are output in
 * miles and km.
 */
class SphereDocEvalStats(
  driver_stats: ExperimentDriverStats,
  wrapped:DocEvalStats[SphereCoord]
) extends EvalStats(driver_stats, null, null) with DocEvalStats[SphereCoord] {
  // "True dist" means actual distance in km's or whatever.
  // "Degree dist" is the distance in degrees.
  val degree_dists = mutable.Buffer[Double]()
  val oracle_degree_dists = mutable.Buffer[Double]()

  override def increment_counter(counter: String) {
    wrapped.increment_counter(counter)
  }

  override def record_result(res: DocEvalResult[SphereCoord]) {
    wrapped.record_result(res)
    degree_dists += res.pred_degdist
    oracle_degree_dists += res.true_degdist
  }

  val output_result_with_units: Double => String =
    (kmdist: Double) => throw new UnsupportedOperationException("should not be called")

  override def output_incorrect_results() {
    wrapped.output_incorrect_results()
    errprint("  Mean degree error distance = %.2f degrees",
      mean(degree_dists))
    errprint("  Median degree error distance = %.2f degrees",
      median(degree_dists))
  }

  override def output_results() {
    wrapped.output_result_header()
    wrapped.output_correct_results()
    output_incorrect_results()
    wrapped.output_other_stats()
  }
}

/**
 * SphereDoc version of `GroupedDocEvalStats`.  This keeps separate
 * sets of statistics for different subgroups of the test documents, i.e.
 * those within particular ranges of one or more quantities of interest.
 */
class GroupedSphereDocEvalStats(
  driver_stats: ExperimentDriverStats,
  grid: SphereGrid,
  create_stats: (ExperimentDriverStats, String) => DocEvalStats[SphereCoord]
) extends GroupedDocEvalStats[SphereCoord](
  driver_stats, grid,
  (driver: ExperimentDriverStats, prefix: String) => {
    val wrapped = create_stats(driver, prefix)
    new SphereDocEvalStats(driver, wrapped)
  }
) {
  val docs_by_degree_dist_to_true_center =
    docmap("degree_dist_to_true_center")

  val docs_by_degree_dist_to_pred_center =
    new DoubleTableByRange(dist_fractions_for_error_dist,
      create_stats_for_range("degree_dist_to_pred_center", _))

  override def record_result_by_range(
    res: DocEvalResult[SphereCoord]
  ) {
    super.record_result_by_range(res)

    /* FIXME: This code specific to MultiRegularGrid is kind of ugly.
       Perhaps it should go elsewhere.

       FIXME: Also note that we don't actually make use of the info we
       record here. See below.
     */
    grid match {
      case multigrid: MultiRegularGrid => {
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
          record_result(res)
        docs_by_degree_dist_to_true_center(rounded_frac_true_degdist).
          record_result(res)

        /* For distance to center of predicted cell, which may be large, since
           predicted cell may be nowhere near the true cell.  Again we convert
           to fractions of tiling-cell size and record in the ranges listed in
           dist_fractions_for_error_dist (see above). */
        /* Predicted distance (in both km and degrees) as a fraction of
           cell size */
        val frac_pred_truedist = res.pred_truedist / multigrid.km_per_cell
        val frac_pred_degdist = res.pred_degdist / multigrid.degrees_per_cell
        docs_by_true_dist_to_pred_center.get_collector(frac_pred_truedist).
          record_result(res)
        docs_by_degree_dist_to_pred_center.get_collector(frac_pred_degdist).
          record_result(res)
       }

      case kdgrid: KdTreeGrid => {
        // for kd trees, we do something similar to above,
        // but round to the nearest km...
        docs_by_true_dist_to_true_center(round(res.true_truedist)).
          record_result(res)
        docs_by_degree_dist_to_true_center(round(res.true_degdist)).
          record_result(res)
      }
    }
  }

  override def output_results_by_range() {
    super.output_results_by_range()
    errprint("")

    grid match {
      case multigrid: MultiRegularGrid => {
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
 * Specialization of `RankedGridEvaluator` for SphereCoords (latitude/
 * longitude coordinates on the surface of a sphere).  Class for evaluating
 * (geolocating) a test document using a strategy that ranks the cells in the
 * cell grid and picks the central point of the top-ranked one.
 *
 * Only needed to support debug("gridrank").
 */
class RankedSphereGridEvaluator(
  strategy: GridLocateDocStrategy[SphereCoord],
  stratname: String,
  driver: GeolocateDocTypeDriver,
  evalstats: DocEvalStats[SphereCoord]
) extends RankedGridEvaluator[SphereCoord](
  strategy, stratname, driver, evalstats
) {
  override def imp_evaluate_document(document: GeoDoc[SphereCoord],
      true_cell: GeoCell[SphereCoord]) = {
    val result = super.imp_evaluate_document(document, true_cell)
    new RankedSphereDocEvalResult(
      result.asInstanceOf[FullRankedDocEvalResult[SphereCoord]]
    )
  }
}

class RankedSphereDocEvalResult(
  wrapped: FullRankedDocEvalResult[SphereCoord]
) extends FullRankedDocEvalResult[SphereCoord](
  wrapped.document, wrapped.pred_cells, wrapped.true_rank
) {
  override def print_result(doctag: String,
      driver: GridLocateDocDriver[SphereCoord]) {
    wrapped.print_result(doctag, driver)

    assert(doctag(0) == '#')
    if (debug("gridrank") ||
      (debuglist("gridrank") contains doctag.drop(1))) {
      val grsize = debugval("gridranksize").toInt
      wrapped.true_cell match {
        case multireg: MultiRegularCell =>
          multireg.grid.
            output_ranking_grid(
              wrapped.pred_cells.
                asInstanceOf[Iterable[(MultiRegularCell, Double)]],
              multireg, grsize)
        case _ =>
          warning("Can't output ranking grid, cell not of right type")
      }
    }
  }
}

case class TitledDoc(title: String, text: String)
class TitledDocResult { }

/**
 * A class for geolocation where each test document is a chapter in a book
 * in the PCL Travel corpus.
 */
class PCLTravelGeolocateDocEvaluator(
  strategy: GridLocateDocStrategy[SphereCoord],
  stratname: String,
  grid: GeoGrid[SphereCoord],
  filehand: FileHandler,
  filenames: Iterable[String]
) extends CorpusEvaluator(stratname, grid.driver) {
  type TEvalDoc = TitledDoc
  type TEvalRes = TitledDocResult

  def iter_document_stats = {
    filenames.toIterator.flatMap { filename =>
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
      } yield DocStatus(filehand, filename, 0, Some(TitledDoc(headtext, text)),
                "processed", "", "")).toIterator
    }
  }

  def evaluate_document(doc: TitledDoc) = {
    val dist = grid.docfact.word_dist_factory.create_word_dist
    for (text <- Seq(doc.title, doc.text))
      dist.add_document(split_text_into_words(text, ignore_punc = true))
    dist.finish_before_global()
    dist.finish_after_global()
    val cells =
      strategy.return_ranked_cells(dist, include = Iterable[SphereCell]())
    // FIXME: This should be output by a result object we return.
    errprint("")
    errprint("Document with title: %s", doc.title)
    val num_cells_to_show = 5
    for ((rank, cellval) <- (1 to num_cells_to_show) zip cells) {
      val (cell, vall) = cellval
      if (debug("pcl-travel")) {
        errprint("  Rank %d, goodness %g:", rank, vall)
        errprint(cell.xmldesc.toString) // indent=4
      } else
        errprint("  Rank %d, goodness %g: %s", rank, vall, cell.shortstr)
    }

    new TitledDocResult()
  }

  def output_results(isfinal: Boolean = false) {
  }
}

