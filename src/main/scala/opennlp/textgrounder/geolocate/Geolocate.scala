///////////////////////////////////////////////////////////////////////////////
//  Geolocate.scala
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
import util.text.format_float
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

-- The main entry class is GeolocateDocApp.  This is hooked into
   GeolocateDocDriver.  The driver classes implement the logic for
   running the program -- in fact, this logic in the superclass
   GeolocateDocTypeDriver so that a separate Hadoop driver can be
   provided.  The separate driver class is provided so that we can run
   the geolocate app and other TextGrounder apps programmatically as well
   as from the command line, and the complication of multiple driver
   classes is (at least partly) due to supporting various apps, e.g.
   GenerateKML (a separate app for generating KML files graphically
   illustrating the corpus).  The mechanism that implements the driver
   class is in textgrounder.util.experiment.  The actual entry point is
   in ExperimentApp.main(), although the entire implementation is in
   ExperimentApp.implement_main().
-- The class GeolocateDocParameters holds descriptions of all of the
   various command-line parameters, as well as the values of those
   parameters when read from the command line (or alternatively, filled in
   by another program using the programmatic interface).  This inherits
   from GeolocateParameters, which supplies parameters common to other
   TextGrounder apps.  Argument parsing is handled using
   textgrounder.util.argparser, a custom argument-parsing package built on
   top of Argot.
-- The driver class has three main methods. `handle_parameters` verifies
   that valid combinations of parameters were specified. `setup_for_run`
   creates some internal structures necessary for running, and
   `run_after_setup` does the actual running.  The reason for the separation
   of the two is that only the former is used by the Hadoop driver.
   (FIXME: Perhaps there's a better way of handling this.)

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

/////////////////////////////////////////////////////////////////////////////
//                           Evaluation strategies                         //
/////////////////////////////////////////////////////////////////////////////

abstract class SphereGridRanker(
  ranker_name: String,
  sphere_grid: SphereGrid
) extends GridRanker[SphereCoord](ranker_name, sphere_grid) { }

class CellDistMostCommonToponymSphereGridRanker(
  ranker_name: String,
  sphere_grid: SphereGrid
) extends SphereGridRanker(ranker_name, sphere_grid) {
  val cdist_factory =
    new CellDistFactory[SphereCoord](sphere_grid.driver.params.lru_cache_size)

  def return_ranked_cells(_lang_model: LangModel, include: Iterable[SphereCell]) = {
    val lang_model = Unigram.check_unigram_lang_model(_lang_model)
    val wikipedia_fact = get_sphere_docfact(sphere_grid).wikipedia_subfactory

    // Look for a toponym, then a proper noun, then any word.
    // FIXME: Use invalid_word
    // FIXME: Should predicate be passed an index and have to do its own
    // unmemoizing?
    var maxword = lang_model.find_most_common_word(
      word => word(0).isUpper && wikipedia_fact.word_is_toponym(word))
    if (maxword == None) {
      maxword = lang_model.find_most_common_word(
        word => word(0).isUpper)
    }
    if (maxword == None)
      maxword = lang_model.find_most_common_word(word => true)
    cdist_factory.get_cell_dist(sphere_grid, maxword.get).
      get_ranked_cells(include)
  }
}

class SalienceMostCommonToponymSphereGridRanker(
  ranker_name: String,
  sphere_grid: SphereGrid
) extends SphereGridRanker(ranker_name, sphere_grid) {
  def return_ranked_cells(_lang_model: LangModel, include: Iterable[SphereCell]) = {
    val lang_model = Unigram.check_unigram_lang_model(_lang_model)
    val wikipedia_fact = get_sphere_docfact(sphere_grid).wikipedia_subfactory

    var maxword = lang_model.find_most_common_word(
      word => word(0).isUpper && wikipedia_fact.word_is_toponym(word))
    if (maxword == None) {
      maxword = lang_model.find_most_common_word(
        word => wikipedia_fact.word_is_toponym(word))
    }
    if (debug("commontop"))
      errprint("  maxword = %s", maxword)
    val cands =
      if (maxword != None)
        wikipedia_fact.construct_candidates(
          lang_model.item_to_string(maxword.get))
      else Seq[SphereDoc]()
    if (debug("commontop"))
      errprint("  candidates = %s", cands)
    // Sort candidate list by salience score
    val cand_salience =
      (for (cand <- cands) yield (cand,
        cand.asInstanceOf[WikipediaDoc].adjusted_salience)).
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
        return_ranked_cells(lang_model, include))
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
      help = """Size (in degrees, a floating-point number) of the tiling
cells that cover the Earth.  Default is 1.0 if neither --miles-per-cell
nor --km-per-cell is given. """)
  var miles_per_cell =
    ap.option[Double]("miles-per-cell", "mpc", metavar = "MILES",
      help = """Size (in miles, a floating-point number) of the tiling
cells that cover the Earth.  If given, it overrides the value of
--degrees-per-cell.  No default, as the default of --degrees-per-cell
is used.""")
  var km_per_cell =
    ap.option[Double]("km-per-cell", "kpc", metavar = "KM",
      help = """Size (in kilometers, a floating-point number) of the tiling
cells that cover the Earth.  If given, it overrides the value of
--degrees-per-cell.  No default, as the default of --degrees-per-cell
is used.""")
  var width_of_multi_cell =
    ap.option[Int]("width-of-multi-cell", metavar = "CELLS", default = 1,
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
      help = """Value of K for use in the mean-shift algorithm
(see '--coord-strategy').  For this value of K, we choose the K best cells
and then apply the mean-shift algorithm to the central points of those cells.

Default '%default'.""")

  var mean_shift_window =
    ap.option[Double]("mean-shift-window", "msw",
      default = 1.0,
      help = """Window to use in the mean-shift algorithm
(see '--coord-strategy').

Default '%default'.""")

  var mean_shift_max_stddev =
    ap.option[Double]("mean-shift-max-stddev", "msms",
      default = 1e-10,
      help = """Maximum allowed standard deviation (i.e. approximately the
average distance of the points from their mean) among the points selected by
the mean-shift algorithm (see '--coord-strategy').

Default '%default'.""")

  var mean_shift_max_iterations =
    ap.option[Int]("mean-shift-max-iterations", "msmi",
      default = 100,
      help = """Maximum number of iterations in the mean-shift algorithm
(see '--coord-strategy').

Default '%default'.""")
}


trait GeolocateDriver extends GridLocateDriver[SphereCoord] {
  override type TParam <: GeolocateParameters

  def deserialize_coord(coord: String) = SphereCoord.deserialize(coord)

  override def handle_parameters() {
    super.handle_parameters()

    if (params.width_of_multi_cell <= 0)
      param_error("Width of multi cell must be positive")

    // Handle different ways of specifying grid size

    def check_set(value: Double, desc: String) = {
      if (value < 0)
        param_error(desc + " must be positive if specified")
      if (value > 0)
        1
      else 
        0
    }
    val num_set =
      check_set(params.miles_per_cell, "Miles per cell") +
      check_set(params.km_per_cell, "Kilometers per cell") +
      check_set(params.degrees_per_cell, "Degrees per cell")
    if (num_set == 0)
      params.degrees_per_cell = 1.0
    else if (num_set > 1)
      param_error("Only one of --miles-per-cell, --km-per-cell, --degrees-per-cell may be given")
    val (computed_dpc, computed_mpc, computed_kpc) =
      (params.degrees_per_cell, params.miles_per_cell, params.km_per_cell) match {
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
    params.degrees_per_cell = computed_dpc
    params.km_per_cell = computed_kpc
    params.miles_per_cell = computed_mpc


    // The *-most-common-toponym rankers require case preserving
    // (as if set by --preseve-case-words), while most other rankers want
    // the opposite.  So check to make sure we don't have a clash.
    if (params.ranker endsWith "most-common-toponym") {
      errprint("Forcibly setting --preseve-case-words to true")
      params.preserve_case_words = true
    }

    if (params.eval_format == "raw-text") {
      // FIXME!!!!
      param_error("Raw-text reading not implemented yet")
    }

    if (params.eval_format == "internal") {
      if (params.eval_file.length > 0)
        param_error("--eval-file should not be given when --eval-format=internal")
    } else
      need_seq(params.eval_file, "eval-file", "evaluation file(s)")
  }

  protected def create_document_factory(
      lang_model_factory: DocLangModelFactory) =
    new SphereDocFactory(this, lang_model_factory)

  protected def create_grid(docfact: GridDocFactory[SphereCoord]) = {
    val sphere_docfact = docfact.asInstanceOf[SphereDocFactory]
    if (params.combined_kd_grid) {
      val kdcg =
        KdTreeGrid(sphere_docfact, params.kd_bucket_size, params.kd_split_method,
          params.kd_use_backoff, params.kd_interpolate_weight)
      val mrcg =
        new MultiRegularGrid(params.degrees_per_cell,
          params.width_of_multi_cell, sphere_docfact)
      new CombinedModelGrid(sphere_docfact, Seq(mrcg, kdcg))
    } else if (params.kd_tree) {
      KdTreeGrid(sphere_docfact, params.kd_bucket_size, params.kd_split_method,
        params.kd_use_backoff, params.kd_interpolate_weight)
    } else {
      new MultiRegularGrid(params.degrees_per_cell,
        params.width_of_multi_cell, sphere_docfact)
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
              params.input_corpus.toIterator.flatMap(dir =>
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
      errprint("%s: %s", english, format_float(value))
    }
  }
}

class GeolocateDocumentTypeApp extends GeolocateApp("geolocate-document") {
  type TDriver = StandaloneGeolocateDocumentDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

/**
 * The normal application for geolocating a document.
 */
object GeolocateDocumentApp extends GeolocateDocumentTypeApp { }

/**
 * An application for generating a tag describing the command-line
 * parameters, for use as part of a filename. The only output is the tag
 * itself.
 */
object GeolocateDocumentTagApp extends GeolocateDocumentTypeApp {
  // Suppress normal output of command-line params
  override def output_command_line_parameters() { }

  override def run_program(args: Array[String]) = {
    /////// Various ways of handling params

    /**
     * Output the value only. Typical for choice options.
     */
    def valonly = (value: Any) => value match {
      case null => ""
      case d:Double => {
        if (d == d.toLong) "%s" format d.toLong
        else format_float(d)
      }
      case _ => "%s" format value
    }

    /**
     * Output 'name=value' for a specified name (usually a shortened
     * version of the param's name). Normally used for options with
     * string values.
     */
    def full(name: String, noequals: Boolean = false) =
      (value: Any) => value match {
        // If the value somehow is null or false, no output at all
        case null | false => ""
        // If true (a flag), only the flag's name
        case true => "%s" format name
        case _ => "%s%s%s" format (
          name,
          if (noequals) "" else "=",
          valonly(value)
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

    /////// How to handle params.
    
    /**
     * The order in the list is the order in which the params are output.
     * Any params not given here are put after all listed params, in the
     * order listed in the original specification of the params,
     * formatted as 'name=value'.
     */
    val param_handling = Seq[(String, Any => String)](
      ("input-corpus", xs => xs.asInstanceOf[Seq[String]] map { x =>
        val (dir, tail) = util.io.localfh.split_filename(x)
        if (dir.endsWith("twitter-geotext"))
          "geotext-" + tail.replace("docthresh-", "thresh")
        else
          tail
      } mkString "+"
      ),
      ("ranker", valonly),
      ("lang-model", valonly),
      ("interpolate", x => x match {
        case "yes" => "interpolate"
        case "no" => "backoff"
        case _ => ""
      }),
      ("jelinek-factor", valonly),
      ("dirichlet-factor", valonly),
      ("tf-idf", echo("tfidf")),
      ("rerank", x => x match {
        case "pointwise" => "rerank"
        case _ => "no-rerank"
      }),
      ("rerank-top-n", short("top")),
      ("rerank-instance", valonly),
      ("rerank-classifier", valonly),
      ("pa-variant", short("var")),
      ("perceptron-aggressiveness", short("aggr")),
      ("perceptron-error-threshold", short("errthresh")),
      ("perceptron-rounds", short("rounds")),
      ("degrees-per-cell", short("deg")),
      ("miles-per-cell", short("miles")),
      ("km-per-cell", short("km")),
      ("kd-tree", echo("kdtree")),
      ("kd-split-method", valonly),
      ("kd-backoff", echo("kdbackoff")),
      ("kd-bucket-size", short("bucketsize")),
      ("center-method", valonly),
      ("language", full("lang")),
      ("num-nearest-neighbors", short("knn")),
      ("eval-set", valonly),
      ("num-training-docs", short("ntrain")),
      ("num-test-docs", short("ntest")),
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
        if (handling_map.contains(name))
          handling_map(name)(value)
        else
          full(name)(value)
      }) filter { _ != "" } mkString "."

    outprint("%s", tag)
    0
  }
}

