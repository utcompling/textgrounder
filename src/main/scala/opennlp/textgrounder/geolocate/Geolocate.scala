///////////////////////////////////////////////////////////////////////////////
//  Geolocate.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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

import scala.util.matching.Regex
import scala.util.Random
import math._
import collection.mutable

import util.argparser._
import util.collection._
import util.spherical._
import util.experiment._
import util.io.{FileHandler, LocalFileHandler}
import util.os._
import util.print.{errprint, outprint}
import util.text.format_double
import util.textdb.Encoder
import util.time.format_minutes_seconds

import util.debug._
import gridlocate._

import langmodel.{LangModel,LangModelFactory,Unigram}

/*

This module is the main driver module for the Geolocate subproject.
The Geolocate subproject does document-level geolocation and is part
of TextGrounder.  An underlying GridLocate framework is provided
for doing work of various sorts with documents that are amalgamated
into grids (e.g. over the Earth or over dates or times).  This means
that code for Geolocate is split between `textgrounder.geolocate` and
`textgrounder.gridlocate`.  See also GridLocate.scala.

The Geolocate code works as follows:

-- The main entry class is GeolocateDocument.  This is hooked into
   GeolocateDocumentDriver.  The driver classes implement the logic for
   running the program. There is a superclass GeolocateDriver that provides
   all the logic of building the grid but doesn't evaluate documents; this is
   so that other supporting apps (e.g. GenerateKML, WriteGrid) can use the
   same logic. There are also specialized subclasses
   StandaloneGeolocateDocumentDriver and HadoopGeolocateDocumentDriver so that
   we can run both standalone and from Hadoop. (FIXME: We should probably
   switch to using Scoobi instead of raw Hadoop.) The distinction between
   App and Driver classes stems from the experiment-application framework in
   textgrounder.util.experiment and was originally designed to allow for
   separate invocation programmatically and from the command line. (FIXME:
   This never quite worked as intended and should be merged.) The actual entry
   point that is invoked from the JVM is `ExperimentApp.main`, which
   immediately passes control to `ExperimentApp.implement_main` (this is so
   that the same logic can be used for Hadoop, which has a different entry
   point).
-- The class GeolocateDocumentParameters holds descriptions of all of the
   various command-line parameters, as well as the values of those
   parameters when read from the command line (or alternatively, filled in
   by another program using the programmatic interface).  This inherits
   from GeolocateParameters, which supplies parameters common to other
   TextGrounder apps.  Argument parsing is handled using
   textgrounder.util.argparser, a custom argument-parsing package built on
   top of Argot.
-- The driver class has one main method: `run` implements the actual program.

In order to support all the various command-line parameters, the logic for
doing geolocation is split up into various classes:

-- Classes exist in `gridlocate` for an individual document (GridDoc),
   the factory of all documents (GridDocFactory), the grid containing cells
   into which the documents are placed (Grid), and the individual cells
   in the grid (GridCell).  There also needs to be a class specifying a
   coordinate identifying a document (e.g. time or latitude/longitude pair).
   Specific versions of all of these are created for Geolocate, identified
   by the word "Sphere" (SphereDoc, SphereCell, SphereCoord, etc.),
   which is intended to indicate the fact that the grid refers to locations
   on the surface of a sphere.
-- The cell grid class SphereGrid has subclasses for the different types of
   grids (MultiRegularGrid, KDTreeGrid).
-- Different types of ranker objects (subclasses of
   SphereGridRanker, in turn a subclass of GridRanker)
   implement the different inference methods specified using `--ranker`,
   e.g. KLDivergenceGridRanker or NaiveBayesGridRanker. The driver method
   `setup_for_run` creates the necessary ranker objects.
-- Evaluation is performed using different GridEvaluator objects, e.g.
   RankedSphereGridEvaluator and MeanShiftSphereGridEvaluator. 
*/

/**
 * Constants used in various places esp. debugging code.
 */
object GeolocateConstants {
  val default_gridranksize = 11
  def gridranksize = debugint("gridranksize", default_gridranksize)
}

/////////////////////////////////////////////////////////////////////////////
//                           Evaluation strategies                         //
/////////////////////////////////////////////////////////////////////////////

abstract class SphereGridRanker(
  ranker_name: String,
  sphere_grid: SphereGrid
) extends SimpleGridRanker[SphereCoord](ranker_name, sphere_grid) { }

class CellDistMostCommonToponymSphereGridRanker(
  ranker_name: String,
  sphere_grid: SphereGrid
) extends SphereGridRanker(ranker_name, sphere_grid) {
  val cdist_factory =
    new CellDistFactory[SphereCoord](sphere_grid.driver.params.lru_cache_size)

  def return_ranked_cells(_lang_model: LangModel, correct: SphereCell,
      include_correct: Boolean) = {
    val lang_model = Unigram.check_unigram_lang_model(_lang_model)
    val wikipedia_fact = get_sphere_docfact(sphere_grid).wikipedia_subfactory

    // Look for a toponym, then a proper noun, then any word.
    // FIXME: Use invalid_gram
    // FIXME: Should predicate be passed an index and have to do its own
    // unmemoizing?
    var maxword = lang_model.find_most_common_gram(
      word => word(0).isUpper && wikipedia_fact.word_is_toponym(word))
    if (maxword == None) {
      maxword = lang_model.find_most_common_gram(
        word => word(0).isUpper)
    }
    if (maxword == None)
      maxword = lang_model.find_most_common_gram(word => true)
    cdist_factory.get_cell_dist(sphere_grid, maxword.get).
      get_ranked_cells(correct, include_correct)
  }
}

class SalienceMostCommonToponymSphereGridRanker(
  ranker_name: String,
  sphere_grid: SphereGrid
) extends SphereGridRanker(ranker_name, sphere_grid) {
  def return_ranked_cells(_lang_model: LangModel, correct: SphereCell,
      include_correct: Boolean) = {
    val lang_model = Unigram.check_unigram_lang_model(_lang_model)
    val wikipedia_fact = get_sphere_docfact(sphere_grid).wikipedia_subfactory

    var maxword = lang_model.find_most_common_gram(
      word => word(0).isUpper && wikipedia_fact.word_is_toponym(word))
    if (maxword == None) {
      maxword = lang_model.find_most_common_gram(
        word => wikipedia_fact.word_is_toponym(word))
    }
    if (debug("commontop"))
      errprint("  maxword = %s", maxword)
    val cands =
      if (maxword != None)
        wikipedia_fact.construct_candidates(
          lang_model.gram_to_string(maxword.get))
      else Seq[SphereDoc]()
    if (debug("commontop"))
      errprint("  candidates = %s", cands)
    // Sort candidate list by salience score
    val cand_salience =
      (for (cand <- cands) yield (cand, cand.salience.getOrElse(0.0))).
        // sort by second element of tuple, in reverse order
        sortWith(_._2 > _._2)
    if (debug("commontop"))
      errprint("  sorted candidates = %s", cand_salience)

    def find_good_cells_for_coord(cands: Iterable[(SphereDoc, Double)]) = {
      for {
        (cand, salience) <- cand_salience
        cell <- {
          val retval = sphere_grid.find_best_cell_for_document(cand,
            create_non_recorded = false)
          if (retval == None)
            errprint("Strange, found no cell for candidate %s", cand)
          retval
        }
      } yield (cell, salience)
    }

    // Convert to cells
    val candcells = find_good_cells_for_coord(cand_salience)

    if (debug("commontop"))
      errprint("  cell candidates = %s", candcells)

    // Append random cells and remove duplicates
    merge_numbered_sequences_uniquely(candcells,
      new RandomGridRanker[SphereCoord](ranker_name, sphere_grid).
        return_ranked_cells(lang_model, correct, include_correct))
  }
}

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

/**
 * Class retrieving command-line arguments or storing programmatic
 * configuration parameters.
 *
 * @param parser If specified, should be a parser for retrieving the
 *   value of command-line arguments from the command line.  Provided
 *   that the parser has been created and initialized by creating a
 *   previous instance of this same class with the same parser (a
 *   "shadow field" class), the variables below will be initialized with
 *   the values given by the user on the command line.  Otherwise, they
 *   will be initialized with the default values for the parameters.
 *   Because they are vars, they can be freely set to other values.
 *
 */
class GeolocateParameters(val parser: ArgParser
    ) extends GridLocateParameters {
  //// Options indicating how to generate the cells we compare against
  var degrees_per_cell =
    ap.option[Double]("degrees-per-cell", "dpc", metavar = "DEGREES",
      must = be_>=(0.0),
      help = """Size (in degrees, a floating-point number) of the tiling
cells that cover the Earth.  Default is 1.0 if neither --miles-per-cell
nor --km-per-cell is given. """)
  var miles_per_cell =
    ap.option[Double]("miles-per-cell", "mpc", metavar = "MILES",
      must = be_>=(0.0),
      help = """Size (in miles, a floating-point number) of the tiling
cells that cover the Earth.  If given, it overrides the value of
--degrees-per-cell.  No default, as the default of --degrees-per-cell
is used.""")
  var km_per_cell =
    ap.option[Double]("km-per-cell", "kpc", metavar = "KM",
      must = be_>=(0.0),
      help = """Size (in kilometers, a floating-point number) of the tiling
cells that cover the Earth.  If given, it overrides the value of
--degrees-per-cell.  No default, as the default of --degrees-per-cell
is used.""")

  // Handle different ways of specifying grid size

  def check_set(value: Double) = {
    assert (value >= 0)
    if (value > 0) 1 else 0
  }
  val num_set =
    check_set(miles_per_cell) +
    check_set(km_per_cell) +
    check_set(degrees_per_cell)
  if (num_set == 0)
    degrees_per_cell = 1.0
  else if (num_set > 1)
    ap.error("Only one of --miles-per-cell, --km-per-cell, --degrees-per-cell may be given")
  val (computed_dpc, computed_mpc, computed_kpc) =
    (degrees_per_cell, miles_per_cell, km_per_cell) match {
      case (deg, miles, km) if deg > 0 =>
        (deg, deg * miles_per_degree, deg * km_per_degree)
      case (deg, miles, km) if miles > 0 =>
        (miles / miles_per_degree, miles, miles * km_per_mile)
      case (deg, miles, km) if km > 0 =>
        (km / km_per_degree, km / km_per_mile, km)
    }

  // NOTE! Setting this will NOT change the values returned using the
  // argparser programmatic interface onto iterating through params or
  // retrieving them by name. That requires reflection and is difficult
  // or impossible (given current Scala limitations) to implement in a way
  // that works cleanly, interfaces with the desired way of retrieving/
  // setting parameters as variables (so that type-checking works, etc.)
  // and doesn't require needless extra boilerplate.
  degrees_per_cell = computed_dpc
  km_per_cell = computed_kpc
  miles_per_cell = computed_mpc

  // The *-most-common-toponym rankers require case preserving
  // (as if set by --preseve-case-words), while most other rankers want
  // the opposite.  So check to make sure we don't have a clash.
  if (ranker endsWith "most-common-toponym") {
    errprint("Forcibly setting --preseve-case-words to true")
    preserve_case_words = true
  }

  //// Lat/long offset of cells from (0,0)
  var cell_offset_degrees =
    ap.option[String]("cell-offset-degrees", "cod", metavar = "LATLONG",
      default="0.0,0.0",
      help = """Latitude/longtitude offset (in degrees, as two floating-point
numbers separated by a comma) of the corner of any grid cell. This ensures
that the given coordinate falls on a corner between four grid cells.""")

  var width_of_multi_cell =
    ap.option[Int]("width-of-multi-cell", metavar = "CELLS", default = 1,
      must = be_>(0),
      help = """Width of the cell used to compute a language model
for geolocation purposes, in terms of number of tiling cells.
NOTE: It's unlikely you want to change this.  It may be removed entirely in
later versions.  In normal circumstances, the value is 1, i.e. use a single
tiling cell to compute each multi cell.  If the value is more than
1, the multi cells overlap.""")


  //// Options for using KD trees, and related parameters
  var kd_tree =
    ap.flag("kd-tree", "kd", "kdtree",
      help = """Specifies we should use a KD tree rather than uniform
grid cell.""")

  var kd_bucket_size =
    ap.option[Int]("kd-bucket-size", "kdbs", "bucket-size", default = 200,
      metavar = "INT",
      must = be_>(0),
      help = """Bucket size before splitting a leaf into two children.
Default %default.""")

  var kd_split_method =
    ap.option[String]("kd-split-method", "kdsm", metavar = "SPLIT_METHOD",
      default = "halfway",
      choices = Seq("halfway", "median", "maxmargin"),
      help = """Chooses which leaf-splitting method to use. Valid options are
'halfway', which splits into two leaves of equal degrees, 'median', which
splits leaves to have an equal number of documents, and 'maxmargin',
which splits at the maximum margin between two points. All splits are always
on the longest dimension. Default '%default'.""")

  var kd_use_backoff =
    ap.flag("kd-backoff", "kd-use-backoff",
      help = """Specifies if we should back off to larger cell
language models.""")

  var kd_interpolate_weight =
    ap.option[Double]("kd-interpolate-weight", "kdiw", default = 0.0,
      must = be_>=(0.0),
      help = """Specifies the weight given to parent language models.
Default value '%default' means no interpolation is used.""")

  //// Combining the kd-tree model with the cell-grid model
  val combined_kd_grid =
    ap.flag("combined-kd-grid", help = """Combine both the KD tree and
uniform grid cell models?""")

  ////////////// Begin former GeolocateDocParameters

  var eval_format =
    ap.option[String]("f", "eval-format",
      default = "internal",
      must = Must({ x: String => x != "raw-text" },
        // FIXME!!
        "Raw-text reading not implemented yet"),
      choices = Seq("internal", "raw-text" //, "pcl-travel"
      ),
      help = """Format of evaluation file(s).  The evaluation files themselves
are specified using --eval-file.  The following formats are
recognized:

'internal' is the normal format.  It means to consider documents to be
documents to evaluate, and to use the development or test set specified
in the document file as the set of documents to evaluate.  There is
no eval file for this format.

'raw-text' assumes that the eval file is simply raw text.  (NOT YET
IMPLEMENTED.)
""")

  if (eval_format == "internal") {
    if (eval_file.length > 0)
      ap.error("--eval-file should not be given when --eval-format=internal")
  } else if (eval_file.length == 0)
    ap.error("Must specify evaluation file(s) using --eval-file")

//'pcl-travel' is another alternative.  It assumes that each evaluation file
//is in PCL-Travel XML format, and uses each chapter in the evaluation
//file as a document to evaluate.""")

  override protected def ranker_choices = super.ranker_choices ++ Seq(
        Seq("salience-most-common-toponym"),
        Seq("cell-distribution-most-common-toponym",
            "celldist-most-common-toponym"))

  override protected def ranker_baseline_help =
    super.ranker_baseline_help +
"""'salience-most-common-toponym' means to look for the toponym that
occurs the most number of times in the document, and then use the salience
baseline to match it to a location.

'celldist-most-common-toponym' is similar, but uses the cell distribution
of the most common toponym.

"""
  var coord_strategy =
    ap.option[String]("coord-strategy", "cs",
      default = "top-ranked",
      choices = Seq("top-ranked", "mean-shift"),
      help = """Strategy to use to choose the best coordinate for a document.

'top-ranked' means to choose the single best-ranked cell according to the
scoring ranker specified using '--ranker', and use its central point.

'mean-shift' means to take the K best cells (according to '--k-best'),
and then compute a single point using the mean-shift algorithm.  This
algorithm works by steadily shifting each point towards the others by
computing an average of the points surrounding a given point, weighted
by a function that drops off rapidly as the distance from the point
increases (specifically, the weighting is the same as for a Gaussian density,
with a parameter H, specified using '--mean-shift-window', that corresponds to
the standard deviation in the Gaussian distribution function).  The idea is
that the points will eventually converge on the largest cluster within the
original points.  The algorithm repeatedly moves the points closer to each
other until either the total standard deviation of the points (i.e.
approximately the average distance of the points from their mean) is less than
the value specified by '--mean-shift-max-stddev', or the number of iterations
exceeds '--mean-shift-max-iterations'.

Default '%default'.""")

  var k_best =
    ap.option[Int]("k-best", "kb",
      default = 10,
      must = be_>(0),
      help = """Value of K for use in the mean-shift algorithm
(see '--coord-strategy').  For this value of K, we choose the K best cells
and then apply the mean-shift algorithm to the central points of those cells.

Default '%default'.""")

  var mean_shift_window =
    ap.option[Double]("mean-shift-window", "msw",
      default = 1.0,
      must = be_>(0.0),
      help = """Window to use in the mean-shift algorithm
(see '--coord-strategy').

Default '%default'.""")

  var mean_shift_max_stddev =
    ap.option[Double]("mean-shift-max-stddev", "msms",
      default = 1e-10,
      must = be_>(0.0),
      help = """Maximum allowed standard deviation (i.e. approximately the
average distance of the points from their mean) among the points selected by
the mean-shift algorithm (see '--coord-strategy').

Default '%default'.""")

  var mean_shift_max_iterations =
    ap.option[Int]("mean-shift-max-iterations", "msmi",
      default = 100,
      must = be_>(0),
      help = """Maximum number of iterations in the mean-shift algorithm
(see '--coord-strategy').

Default '%default'.""")
}


trait GeolocateDriver extends GridLocateDriver[SphereCoord] {
  override type TParam <: GeolocateParameters

  def deserialize_coord(coord: String) = SphereCoord.deserialize(coord)

  protected def create_document_factory(
      lang_model_factory: DocLangModelFactory) =
    new SphereDocFactory(this, lang_model_factory)

  protected def create_grid(docfact: GridDocFactory[SphereCoord]) = {
    val sphere_docfact = docfact.asInstanceOf[SphereDocFactory]
    val cod = SphereCoord.deserialize(params.cell_offset_degrees)
    def create_multi_regular_grid = {
      new MultiRegularGrid(params.degrees_per_cell,
        cod,
        params.width_of_multi_cell, sphere_docfact)      
    }
    def create_kd_tree_grid = {
      KdTreeGrid(sphere_docfact, params.kd_bucket_size, params.kd_split_method,
        params.kd_use_backoff, params.kd_interpolate_weight)
    }
    if (params.combined_kd_grid) {
      val kdcg = create_kd_tree_grid
      val mrcg = create_multi_regular_grid
      new CombinedModelGrid(sphere_docfact, Seq(mrcg, kdcg))
    } else if (params.kd_tree) {
      create_kd_tree_grid
    } else {
      create_multi_regular_grid
    }
  }

  override def create_named_ranker(ranker_name: String,
      grid: Grid[SphereCoord]) = {
    ranker_name match {
      case "salience-most-common-toponym" =>
        new SalienceMostCommonToponymSphereGridRanker(ranker_name, grid)
      case "celldist-most-common-toponym" =>
        new CellDistMostCommonToponymSphereGridRanker(ranker_name, grid)
      case other => super.create_named_ranker(other, grid)
    }
  }

  /**
   * Create the document evaluator object used to evaluate a given
   * cell in a cell grid.
   *
   * @param ranker Ranker object that implements the mechanism for
   *   scoring different pseudodocuments against a document.  The
   *   ranker computes a ranked list of all pseudodocuments, with
   *   corresponding scores.  The document evaluator then uses this to
   *   finish evaluating the document (e.g. picking the top-ranked one,
   *   applying the mean-shift algorithm, etc.).
   */
  def create_cell_evaluator(ranker: GridRanker[SphereCoord]) = {
    val output_result_with_units =
      (kmdist: Double) => "%.2f km" format kmdist // km_and_miles(kmdist)

    val create_stats: (ExperimentDriverStats, String) =>
        DocEvalStats[SphereCoord] =
      params.coord_strategy match {
        case "top-ranked" =>
          new RankedDocEvalStats(_, _, output_result_with_units)
        case "mean-shift" =>
          new CoordDocEvalStats(_, _, output_result_with_units)
      }

    val evalstats =
      if (params.results_by_range)
        new GroupedSphereDocEvalStats(this, ranker.grid,
          create_stats = create_stats)
      else
        new SphereDocEvalStats(this, create_stats(this, ""))

    params.coord_strategy match {
      case "top-ranked" =>
        new RankedSphereGridEvaluator(ranker, this, evalstats)
      case "mean-shift" =>
        new MeanShiftGridEvaluator[SphereCoord](ranker, this,
          evalstats,
          params.k_best,
          new SphereMeanShift(params.mean_shift_window,
            params.mean_shift_max_stddev, params.mean_shift_max_iterations)
        )
    }
  }
}

trait GeolocateDocumentDriver extends GeolocateDriver {
  type TRunRes = Iterable[_]

  /**
   * Do the actual document geolocation.  Results to stderr (see above), and
   * also returned.  The current return type is an Iterable of objects, one
   * per evaluated document, describing the results of evaluation on that
   * object.  The type of the object depends on the value of
   * `params.eval_format`.
   *
   * NOTE: We force evaluation in this function because currently we mostly
   * depend on side effects (e.g. printing results to stdout/stderr).
   */
  def run() = {
    val ranker = create_ranker
    val grid = ranker.grid
    if (debug("no-evaluation"))
      Iterable()
    else {
      val results_iter =
        params.eval_format match {
//          case "pcl-travel" => {
//            val evalobj =
//              new PCLTravelGeolocateDocEvaluator(ranker, grid,
//                get_file_handler, params.eval_file)
//            evalobj.evaluate_documents(evalobj.iter_document_stats)
//          }
          case "internal" => {
            val docstats =
              params.input.toIterator.flatMap(dir =>
                grid.docfact.read_document_statuses_from_textdb(
                  get_file_handler, dir, document_textdb_suffix))
            val evalobj = create_cell_evaluator(ranker)
            evalobj.evaluate_documents(docstats)
          }
          case "raw-text" => ???
        }
      // The call to `toIndexedSeq` forces evaluation of the Iterator
      val results = results_iter.toIndexedSeq
      note_result("time.begin", beginning_time)
      note_result("time.begin.human", humandate_full(beginning_time))
      note_raw_result("arguments", Encoder.string_seq(original_args))
      ending_time = curtimesecs
      note_result("time.end", ending_time)
      note_result("time.end.human", humandate_full(ending_time))
      val elapsed = ending_time - beginning_time
      note_result("time.running", elapsed)
      note_result("time.running.human", format_minutes_seconds(elapsed))

      if (params.results != null) {
        val filehand = get_file_handler
        write_results_file(results.toIterator, filehand, params.results)
      }
      results
    }
  }
}

class StandaloneGeolocateDocumentDriver extends
    GeolocateDocumentDriver with StandaloneExperimentDriverStats {
  override type TParam = GeolocateParameters
}

abstract class GeolocateApp(appname: String) extends
    GridLocateApp(appname) {
  override type TDriver <: GeolocateDriver

  override def output_command_line_parameters() {
    super.output_command_line_parameters()

    // Output computed values
    val results = Seq(
      ("computed-degrees-per-cell", "Computed degrees per cell",
        params.degrees_per_cell),
      ("computed-km-per-cell", "Computed kilometers per cell",
        params.km_per_cell),
      ("computed-miles-per-cell", "Computed miles per cell",
        params.miles_per_cell)
    )
    for ((field, english, value) <- results) {
      driver.note_result(field, value, english)
      errprint("%s: %s", english, format_double(value))
    }
  }
}

class GeolocateDocumentTypeApp(appname: String) extends
    GeolocateApp(appname) {
  type TDriver = StandaloneGeolocateDocumentDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

/**
 * The normal application for geolocating a document.
 */
object GeolocateDocument extends
    GeolocateDocumentTypeApp("GeolocateDocument") { }

/**
 * An application for generating a tag describing the command-line
 * parameters, for use as part of a filename. The only output is the tag
 * itself.
 */
object GeolocateDocumentTag extends
    GeolocateDocumentTypeApp("GeolocateDocumentTag") {
  // Suppress normal output of command-line params
  override def output_command_line_parameters() { }

  override def run_program(args: Array[String]) = {
    /////// Various ways of handling params

    /**
     * Output the value only. Typical for choice options.
     */
    def valonly: Any => String = value => value match {
      case null => ""
      case d:Double => {
        if (d == d.toLong) "%s" format d.toLong
        else format_double(d)
      }
      case seq:Seq[_] => seqvalonly()(value)
      // Eliminate embedded spaces, which cause various problems in filenames
      case _ => ("%s" format value).replace(" ", "_")
    }

    /**
     * Output the value only. Typical for choice options.
     */
    def seqvalonly(sep: String = ",") = (value: Any) => value match {
      case null => ""
      case seq:Seq[_] => seq.map(valonly) mkString sep
      case _ => ???
    }

    /**
     * Output 'name=value' for a specified name (usually a shortened
     * version of the param's name). Normally used for options with
     * string values.
     */
    def full(name: String, handler: Any => String = valonly,
        noequals: Boolean = false) =
      (value: Any) => value match {
        // If the value somehow is null or false, no output at all
        case null | false => ""
        // If true (a flag), only the flag's name
        case true => "%s" format name
        case _ => "%s%s%s" format (
          name,
          if (noequals) "" else "=",
          handler(value)
        )
      }

    /**
     * Output 'namevalue' -- same as full() but omitting the = sign.
     * Typical for options with numeric values.
     */
    def short(name: String) = full(name, noequals = true)
    /**
     * Echo the given string. Used for flags or choice options that
     * effectively function like flags.
     */
    def echo(name: String) = (_: Any) => name
    /**
     * Ignore this param.
     */
    def omit = echo("")
    /**
     * Default handling of param.
     */
    def default = null
    /**
     * Return tail of filename.
     */
    def filetail = (value: Any) => value match {
      case x:String => {
        val (dir, tail) = util.io.localfh.split_filename(x)
        tail
      }
      case _ => valonly(value)
    }
    /**
     * Return tail of sequence of filenames, concatenated with +.
     */
    def filetail_seq = (value: Any) => {
      val xs = value.asInstanceOf[Seq[String]]
      xs map { x => filetail(x) } mkString "+"
    }

    /////// How to handle params.
    
    /**
     * The order in the list is the order in which the params are output.
     * Any params not given here are put after all listed params, in the
     * order listed in the original specification of the params,
     * formatted as 'name=value'.
     */
    val param_handling = Seq[(String, Any => String)](
      ("input", xs => xs.asInstanceOf[Seq[String]] map { x =>
        val (dir, tail) = util.io.localfh.split_filename(x)
        if (dir.endsWith("twitter-geotext"))
          "geotext-" + tail.replace("docthresh-", "thresh")
        else
          tail
      } mkString "+"
      ),
      ("weights-file", full("weights", filetail)),
      ("stopwords-file", full("stopwords", filetail)),
      ("salience-file", full("salience", filetail)),
      ("whitelist-file", full("whitelist", filetail)),
      ("eval-file", full("eval-file", filetail_seq)),
      ("ranker", valonly),
      ("lang-model", xs => xs match {
        case (x:String, y:String) => x + y
      }),
      ("interpolate", x => x match {
        case "yes" => "interpolate"
        case "no" => "backoff"
        case _ => ""
      }),
      ("tf-idf", default),
      ("rerank", default),
      ("rerank-optimizer", valonly),
      ("rerank-features", full("feats")),
      ("rerank-binning", full("bin")),
      ("rerank-top-n", short("top")),
      ("rerank-num-training-splits", short("nsplits")),
      ("rerank-initial-weights", full("initweights")),
      ("rerank-random-restart", short("random-restart")),
      ("rerank-lang-model", xs => xs match {
        case (x:String, y:String) => "rerank-" + x + y
      }),
      ("rerank-interpolate", x => x match {
        case "yes" => "rerank-interpolate"
        case "no" => "rerank-backoff"
        case _ => ""
      }),
      ("pa-cost-type", full("pa-cost")),
      ("pa-variant", short("pa-var")),
      ("perceptron-aggressiveness", short("perceptron-aggr")),
      ("perceptron-error-threshold", short("perceptron-errthresh")),
      ("perceptron-rounds", short("perceptron-rounds")),
      ("perceptron-decay", short("perceptron-decay")),
      ("degrees-per-cell", short("deg")),
      ("miles-per-cell", short("miles")),
      ("km-per-cell", short("km")),
      ("cell-offset-degrees", full("offsetdeg")),
      ("kd-tree", default),
      ("kd-split-method", valonly),
      ("kd-backoff", default),
      ("kd-bucket-size", short("bucketsize")),
      ("kd-interpolate-weight", short("kd-interpweight")),
      ("combined-kd-grid", default),
      ("center-method", valonly),
      ("language", full("lang")),
      ("num-nearest-neighbors", short("knn")),
      ("eval-set", valonly),
      ("num-training-docs", short("ntrain")),
      ("num-test-docs", short("ntest")),
      ("debug", default),
      ("verbose", omit),
      ("results", omit),
      ("no-parallel", omit),
      ("print-results", omit)
    )

    // Map listing how to handle params.
    val handling_map = param_handling.toMap

    // Map listing ordering of params (mapping param to a number).
    val ordering_map = param_handling.map { _._1}.zipWithIndex.toMap

    // Get non-default params as a list of (name, value) tuples,
    // sort properly.
    val params =
      (for (name <- arg_parser.argNames if arg_parser.specified(name))
        yield (name, arg_parser(name))).toSeq.sortBy {
          case (name, value) =>
            ordering_map.getOrElse(name, ordering_map.size)
        }

    // Generate tag. Note that any tag parts that end up blank are
    // omitted entirely.
    val tag =
      (for ((name, value) <- params) yield {
        val fn =
          if (handling_map.contains(name) &&
              handling_map(name) != null)
            handling_map(name)
          else if (arg_parser.getType(name) == classOf[Boolean])
            echo(name)
          else
            full(name)
        fn(value)
      }) filter { _ != "" } mkString "."

    // Output tag, but convert any slashes to underscores; otherwise attempts
    // to use tags with slashes will fail.
    outprint("%s", tag.replace("/", "_"))
    0
  }
}

