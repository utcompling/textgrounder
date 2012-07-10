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

package opennlp.textgrounder.geolocate

import util.matching.Regex
import util.Random
import math._
import collection.mutable

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil.{FileHandler, LocalFileHandler}
import opennlp.textgrounder.util.osutil.output_resource_usage
import opennlp.textgrounder.util.printutil.errprint

import opennlp.textgrounder.gridlocate._
import GridLocateDriver.Debug._

import opennlp.textgrounder.worddist.{WordDist,WordDistFactory}
import opennlp.textgrounder.worddist.WordDist.memoizer._

/*

This module is the main driver module for the Geolocate subproject.
The Geolocate subproject does document-level geolocation and is part
of TextGrounder.  An underlying GridLocate framework is provided
for doing work of various sorts with documents that are amalgamated
into grids (e.g. over the Earth or over dates or times).  This means
that code for Geolocate is split between `textgrounder.geolocate` and
`textgrounder.gridlocate`.  See also GridLocate.scala.

The Geolocate code works as follows:

-- The main entry class is GeolocateDocumentApp.  This is hooked into
   GeolocateDocumentDriver.  The driver classes implement the logic for
   running the program -- in fact, this logic in the superclass
   GeolocateDocumentTypeDriver so that a separate Hadoop driver can be
   provided.  The separate driver class is provided so that we can run
   the geolocate app and other TextGrounder apps programmatically as well
   as from the command line, and the complication of multiple driver
   classes is (at least partly) due to supporting various apps, e.g.
   GenerateKML (a separate app for generating KML files graphically
   illustrating the corpus).  The mechanism that implements the driver
   class is in textgrounder.util.experiment.  The actual entry point is
   in ExperimentApp.main(), although the entire implementation is in
   ExperimentApp.implement_main().
-- The class GeolocateDocumentParameters holds descriptions of all of the
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

-- Classes exist in `gridlocate` for an individual document (DistDocument),
   the table of all documents (DistDocumentTable), the grid containing cells
   into which the documents are placed (CellGrid), and the individual cells
   in the grid (GeoCell).  There also needs to be a class specifying a
   coordinate identifying a document (e.g. time or latitude/longitude pair).
   Specific versions of all of these are created for Geolocate, identified
   by the word "Sphere" (SphereDocument, SphereCell, SphereCoord, etc.),
   which is intended to indicate the fact that the grid refers to locations
   on the surface of a sphere.
-- The cell grid class SphereGrid has subclasses for the different types of
   grids (MultiRegularCellGrid, KDTreeCellGrid).
-- Different types of strategy objects (subclasses of
   GeolocateDocumentStrategy, in turn a subclass of GridLocateDocumentStrategy)
   implement the different inference methods specified using `--strategy`,
   e.g. KLDivergenceStrategy or NaiveBayesDocumentStrategy. The driver method
   `setup_for_run` creates the necessary strategy objects.
-- Evaluation is performed using different CellGridEvaluator objects, e.g.
   RankedSphereCellGridEvaluator and MeanShiftSphereCellGridEvaluator. 
*/

/////////////////////////////////////////////////////////////////////////////
//                           Evaluation strategies                         //
/////////////////////////////////////////////////////////////////////////////

abstract class GeolocateDocumentStrategy(
  sphere_grid: SphereCellGrid
) extends GridLocateDocumentStrategy[SphereCell, SphereCellGrid](sphere_grid) { }

class CellDistMostCommonToponymGeolocateDocumentStrategy(
  sphere_grid: SphereCellGrid
) extends GeolocateDocumentStrategy(sphere_grid) {
  val cdist_factory =
    new SphereCellDistFactory(sphere_grid.table.driver.params.lru_cache_size)

  def return_ranked_cells(gen_word_dist: WordDist) = {
    val word_dist = UnigramStrategy.check_unigram_dist(gen_word_dist)
    val wikipedia_table = sphere_grid.table.wikipedia_subtable

    // Look for a toponym, then a proper noun, then any word.
    // FIXME: How can 'word' be null?
    // FIXME: Use invalid_word
    // FIXME: Should predicate be passed an index and have to do its own
    // unmemoizing?
    var maxword = word_dist.find_most_common_word(
      word => word(0).isUpper && wikipedia_table.word_is_toponym(word))
    if (maxword == None) {
      maxword = word_dist.find_most_common_word(
        word => word(0).isUpper)
    }
    if (maxword == None)
      maxword = word_dist.find_most_common_word(word => true)
    cdist_factory.get_cell_dist(sphere_grid, maxword.get).get_ranked_cells()
  }
}

class LinkMostCommonToponymGeolocateDocumentStrategy(
  sphere_grid: SphereCellGrid
) extends GeolocateDocumentStrategy(sphere_grid) {
  def return_ranked_cells(gen_word_dist: WordDist) = {
    val word_dist = UnigramStrategy.check_unigram_dist(gen_word_dist)
    val wikipedia_table = sphere_grid.table.wikipedia_subtable

    var maxword = word_dist.find_most_common_word(
      word => word(0).isUpper && wikipedia_table.word_is_toponym(word))
    if (maxword == None) {
      maxword = word_dist.find_most_common_word(
        word => wikipedia_table.word_is_toponym(word))
    }
    if (debug("commontop"))
      errprint("  maxword = %s", maxword)
    val cands =
      if (maxword != None)
        wikipedia_table.construct_candidates(
          unmemoize_string(maxword.get))
      else Seq[SphereDocument]()
    if (debug("commontop"))
      errprint("  candidates = %s", cands)
    // Sort candidate list by number of incoming links
    val candlinks =
      (for (cand <- cands) yield (cand,
        cand.asInstanceOf[WikipediaDocument].adjusted_incoming_links.toDouble)).
        // sort by second element of tuple, in reverse order
        sortWith(_._2 > _._2)
    if (debug("commontop"))
      errprint("  sorted candidates = %s", candlinks)

    def find_good_cells_for_coord(cands: Iterable[(SphereDocument, Double)]) = {
      for {
        (cand, links) <- candlinks
        val cell = {
          val retval = sphere_grid.find_best_cell_for_coord(cand.coord, false)
          if (retval == null)
            errprint("Strange, found no cell for candidate %s", cand)
          retval
        }
        if (cell != null)
      } yield (cell, links)
    }

    // Convert to cells
    val candcells = find_good_cells_for_coord(candlinks)

    if (debug("commontop"))
      errprint("  cell candidates = %s", candcells)

    // Append random cells and remove duplicates
    merge_numbered_sequences_uniquely(candcells,
      new RandomGridLocateDocumentStrategy[SphereCell, SphereCellGrid](sphere_grid).
        return_ranked_cells(word_dist))
  }
}

class SphereAverageCellProbabilityStrategy(
  sphere_grid: SphereCellGrid
) extends AverageCellProbabilityStrategy[
  SphereCell, SphereCellGrid
](sphere_grid) {
  type TCellDistFactory = SphereCellDistFactory
  def create_cell_dist_factory(lru_cache_size: Int) =
    new SphereCellDistFactory(lru_cache_size)
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
class GeolocateParameters(parser: ArgParser = null) extends
    GridLocateParameters(parser) {
  //// Options indicating how to generate the cells we compare against
  var degrees_per_cell =
    ap.option[Double]("degrees-per-cell", "dpc", metavar = "DEGREES",
      default = 1.0,
      help = """Size (in degrees, a floating-point number) of the tiling
cells that cover the Earth.  Default %default. """)
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
      help = """Width of the cell used to compute a statistical
distribution for geolocation purposes, in terms of number of tiling cells.
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

  var center_method =
    ap.option[String]("center-method", "cm", metavar = "CENTER_METHOD",
      default = "centroid",
      choices = Seq("centroid", "center"),
      help = """Chooses whether to use center or centroid for cell
center calculation. Options are either 'centroid' or 'center'.
Default '%default'.""")

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
      help = """Specifies if we should back off to larger cell distributions.""")

  var kd_interpolate_weight =
    ap.option[Double]("kd-interpolate-weight", "kdiw", default = 0.0,
      help = """Specifies the weight given to parent distributions.
Default value '%default' means no interpolation is used.""")

  //// Combining the kd-tree model with the cell-grid model
  val combined_kd_grid =
    ap.flag("combined-kd-grid", help = """Combine both the KD tree and
uniform grid cell models?""")

}

abstract class GeolocateDriver extends GridLocateDriver {
  type TGrid = SphereCellGrid
  type TDoc = SphereDocument
  type TDocTable = SphereDocumentTable
  override type TParam <: GeolocateParameters
  var degrees_per_cell = 0.0

  override def handle_parameters() {
    if (params.miles_per_cell < 0)
      param_error("Miles per cell must be positive if specified")
    if (params.km_per_cell < 0)
      param_error("Kilometers per cell must be positive if specified")
    if (params.degrees_per_cell < 0)
      param_error("Degrees per cell must be positive if specified")
    if (params.miles_per_cell > 0 && params.km_per_cell > 0)
      param_error("Only one of --miles-per-cell and --km-per-cell can be given")
    degrees_per_cell =
      if (params.miles_per_cell > 0)
        params.miles_per_cell / miles_per_degree
      else if (params.km_per_cell > 0)
        params.km_per_cell / km_per_degree
      else
        params.degrees_per_cell
    if (params.width_of_multi_cell <= 0)
      param_error("Width of multi cell must be positive")

    super.handle_parameters()
  }

  protected def initialize_document_table(word_dist_factory: WordDistFactory) = {
    new SphereDocumentTable(this, word_dist_factory)
  }

  protected def initialize_cell_grid(table: SphereDocumentTable) = {
    if (params.combined_kd_grid) {
      val kdcg =
        KdTreeCellGrid(table, params.kd_bucket_size, params.kd_split_method,
          params.kd_use_backoff, params.kd_interpolate_weight)
      val mrcg =
        new MultiRegularCellGrid(degrees_per_cell,
          params.width_of_multi_cell, table)
      new CombinedModelCellGrid(table, Seq(mrcg, kdcg))
    } else if (params.kd_tree) {
      KdTreeCellGrid(table, params.kd_bucket_size, params.kd_split_method,
        params.kd_use_backoff, params.kd_interpolate_weight)
    } else {
      new MultiRegularCellGrid(degrees_per_cell,
        params.width_of_multi_cell, table)
    }
  }
}

class GeolocateDocumentParameters(
  parser: ArgParser = null
) extends GeolocateParameters(parser) {
  var eval_format =
    ap.option[String]("f", "eval-format",
      default = "internal",
      choices = Seq("internal", "raw-text", "pcl-travel"),
      help = """Format of evaluation file(s).  The evaluation files themselves
are specified using --eval-file.  The following formats are
recognized:

'internal' is the normal format.  It means to consider documents to be
documents to evaluate, and to use the development or test set specified
in the document file as the set of documents to evaluate.  There is
no eval file for this format.

'raw-text' assumes that the eval file is simply raw text.  (NOT YET
IMPLEMENTED.)

'pcl-travel' is another alternative.  It assumes that each evaluation file
is in PCL-Travel XML format, and uses each chapter in the evaluation
file as a document to evaluate.""")

  var strategy =
    ap.multiOption[String]("s", "strategy",
      default = Seq("partial-kl-divergence"),
      //      choices = Seq(
      //        "baseline", "none",
      //        "full-kl-divergence",
      //        "partial-kl-divergence",
      //        "symmetric-full-kl-divergence",
      //        "symmetric-partial-kl-divergence",
      //        "cosine-similarity",
      //        "partial-cosine-similarity",
      //        "smoothed-cosine-similarity",
      //        "smoothed-partial-cosine-similarity",
      //        "average-cell-probability",
      //        "naive-bayes-with-baseline",
      //        "naive-bayes-no-baseline",
      //        ),
      aliases = Map(
        "baseline" -> null, "none" -> null,
        "full-kl-divergence" ->
          Seq("full-kldiv", "full-kl"),
        "partial-kl-divergence" ->
          Seq("partial-kldiv", "partial-kl", "part-kl"),
        "symmetric-full-kl-divergence" ->
          Seq("symmetric-full-kldiv", "symmetric-full-kl", "sym-full-kl"),
        "symmetric-partial-kl-divergence" ->
          Seq("symmetric-partial-kldiv", "symmetric-partial-kl", "sym-part-kl"),
        "cosine-similarity" ->
          Seq("cossim"),
        "partial-cosine-similarity" ->
          Seq("partial-cossim", "part-cossim"),
        "smoothed-cosine-similarity" ->
          Seq("smoothed-cossim"),
        "smoothed-partial-cosine-similarity" ->
          Seq("smoothed-partial-cossim", "smoothed-part-cossim"),
        "average-cell-probability" ->
          Seq("avg-cell-prob", "acp"),
        "naive-bayes-with-baseline" ->
          Seq("nb-base"),
        "naive-bayes-no-baseline" ->
          Seq("nb-nobase")),
      help = """Strategy/strategies to use for geolocation.
'baseline' means just use the baseline strategy (see --baseline-strategy).

'none' means don't do any geolocation.  Useful for testing the parts that
read in data and generate internal structures.

'full-kl-divergence' (or 'full-kldiv') searches for the cell where the KL
divergence between the document and cell is smallest.

'partial-kl-divergence' (or 'partial-kldiv') is similar but uses an
abbreviated KL divergence measure that only considers the words seen in the
document; empirically, this appears to work just as well as the full KL
divergence.

'average-cell-probability' (or 'celldist') involves computing, for each word,
a probability distribution over cells using the word distribution of each cell,
and then combining the distributions over all words in a document, weighted by
the count the word in the document.

'naive-bayes-with-baseline' and 'naive-bayes-no-baseline' use the Naive
Bayes algorithm to match a test document against a training document (e.g.
by assuming that the words of the test document are independent of each
other, if we are using a unigram word distribution).  The "baseline" is
currently 

Default is 'partial-kl-divergence'.

NOTE: Multiple --strategy options can be given, and each strategy will
be tried, one after the other.""")

  var coord_strategy =
    ap.option[String]("coord-strategy", "cs",
      default = "top-ranked",
      choices = Seq("top-ranked", "mean-shift"),
      help = """Strategy/strategies to use to choose the best coordinate for
a document.

'top-ranked' means to choose the single best-ranked cell according to the
scoring strategy specified using '--strategy', and use its central point.

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

  var baseline_strategy =
    ap.multiOption[String]("baseline-strategy", "bs",
      default = Seq("internal-link"),
      choices = Seq("internal-link", "random",
        "num-documents", "link-most-common-toponym",
        "cell-distribution-most-common-toponym"),
      aliases = Map(
        "internal-link" -> Seq("link"),
        "num-documents" -> Seq("num-docs", "numdocs"),
        "cell-distribution-most-common-toponym" ->
          Seq("celldist-most-common-toponym")),
      help = """Strategy to use to compute the baseline.

'internal-link' (or 'link') means use number of internal links pointing to the
document or cell.

'random' means choose randomly.

'num-documents' (or 'num-docs' or 'numdocs'; only in cell-type matching) means
use number of documents in cell.

'link-most-common-toponym' means to look for the toponym that occurs the
most number of times in the document, and then use the internal-link
baseline to match it to a location.

'celldist-most-common-toponym' is similar, but uses the cell distribution
of the most common toponym.

Default '%default'.

NOTE: Multiple --baseline-strategy options can be given, and each strategy will
be tried, one after the other.  Currently, however, the *-most-common-toponym
strategies cannot be mixed with other baseline strategies, or with non-baseline
strategies, since they require that --preserve-case-words be set internally.""")
}

// FUCK ME.  Have to make this abstract and GeolocateDocumentDriver a subclass
// so that the TParam can be overridden in HadoopGeolocateDocumentDriver.
abstract class GeolocateDocumentTypeDriver extends GeolocateDriver {
  override type TParam <: GeolocateDocumentParameters
  type TRunRes =
    Seq[(String, GridLocateDocumentStrategy[SphereCell, SphereCellGrid],
         CorpusEvaluator[_,_])]

  var strategies: Seq[(String, GridLocateDocumentStrategy[SphereCell, SphereCellGrid])] = _

  override def handle_parameters() {
    super.handle_parameters()

    if (params.strategy contains "baseline") {
      var need_case = false
      var need_no_case = false
      for (bstrat <- params.baseline_strategy) {
        if (bstrat.endsWith("most-common-toponym"))
          need_case = true
        else
          need_no_case = true
      }
      if (need_case) {
        if (params.strategy.length > 1 || need_no_case) {
          // That's because we have to set --preserve-case-words, which we
          // generally don't want set for other strategies and which affects
          // the way we construct the training-document distributions.
          param_error("Can't currently mix *-most-common-toponym baseline strategy with other strategies")
        }
        params.preserve_case_words = true
      }
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

  /**
   * Set everything up for document geolocation.  Create and save a
   * sequence of strategy objects, used by us and by the Hadoop interface,
   * which does its own iteration over documents.
   */
  override def setup_for_run() {
    super.setup_for_run()
    val strats_unflat = (
      for (stratname <- params.strategy) yield {
        if (stratname == "baseline") {
          for (basestratname <- params.baseline_strategy) yield {
            val strategy = basestratname match {
              case "link-most-common-toponym" =>
                new LinkMostCommonToponymGeolocateDocumentStrategy(cell_grid)
              case "celldist-most-common-toponym" =>
                new CellDistMostCommonToponymGeolocateDocumentStrategy(cell_grid)
              case "random" =>
                new RandomGridLocateDocumentStrategy[SphereCell, SphereCellGrid](cell_grid)
              case "internal-link" =>
                new MostPopularCellGridLocateDocumentStrategy[SphereCell, SphereCellGrid](cell_grid, true)
              case "num-documents" =>
                new MostPopularCellGridLocateDocumentStrategy[SphereCell, SphereCellGrid](cell_grid, false)
              case _ => {
                assert(false,
                  "Internal error: Unhandled strategy " + basestratname);
                null
              }
            }
            ("baseline " + basestratname, strategy)
          }
        } else {
          val strategy =
            if (stratname.startsWith("naive-bayes-"))
              new NaiveBayesDocumentStrategy[SphereCell, SphereCellGrid](cell_grid,
                use_baseline = (stratname == "naive-bayes-with-baseline"))
            else stratname match {
              case "average-cell-probability" =>
                new SphereAverageCellProbabilityStrategy(cell_grid)
              case "cosine-similarity" =>
                new CosineSimilarityStrategy[SphereCell, SphereCellGrid](cell_grid, smoothed = false,
                  partial = false)
              case "partial-cosine-similarity" =>
                new CosineSimilarityStrategy[SphereCell, SphereCellGrid](cell_grid, smoothed = false,
                  partial = true)
              case "smoothed-cosine-similarity" =>
                new CosineSimilarityStrategy[SphereCell, SphereCellGrid](cell_grid, smoothed = true,
                  partial = false)
              case "smoothed-partial-cosine-similarity" =>
                new CosineSimilarityStrategy[SphereCell, SphereCellGrid](cell_grid, smoothed = true,
                  partial = true)
              case "full-kl-divergence" =>
                new KLDivergenceStrategy[SphereCell, SphereCellGrid](cell_grid, symmetric = false,
                  partial = false)
              case "partial-kl-divergence" =>
                new KLDivergenceStrategy[SphereCell, SphereCellGrid](cell_grid, symmetric = false,
                  partial = true)
              case "symmetric-full-kl-divergence" =>
                new KLDivergenceStrategy[SphereCell, SphereCellGrid](cell_grid, symmetric = true,
                  partial = false)
              case "symmetric-partial-kl-divergence" =>
                new KLDivergenceStrategy[SphereCell, SphereCellGrid](cell_grid, symmetric = true,
                  partial = true)
              case "none" =>
                null
            }
          if (strategy != null)
            Seq((stratname, strategy))
          else
            Seq()
        }
      })
    strategies = strats_unflat reduce (_ ++ _)
  }

  /**
   * Create the document evaluator object used to evaluate a given
   * document.
   *
   * @param strategy Strategy object that implements the mechanism for
   *   scoring different pseudodocuments against a document.  The
   *   strategy computes a ranked list of all pseudodocuments, with
   *   corresponding scores.  The document evaluator then uses this to
   *   finish evaluating the document (e.g. picking the top-ranked one,
   *   applying the mean-shift algorithm, etc.).
   * @param stratname Name of the strategy.
   */
  def create_document_evaluator(
      strategy: GridLocateDocumentStrategy[SphereCell, SphereCellGrid],
      stratname: String) = {
    // Generate reader object
    if (params.eval_format == "pcl-travel")
      new PCLTravelGeolocateDocumentEvaluator(strategy, stratname, this)
    else if (params.coord_strategy == "top-ranked")
      new RankedSphereCellGridEvaluator(strategy, stratname, this)
    else
      new MeanShiftSphereCellGridEvaluator(strategy, stratname, this,
        params.k_best, params.mean_shift_window,
        params.mean_shift_max_stddev,
        params.mean_shift_max_iterations)
  }

  /**
   * Do the actual document geolocation.  Results to stderr (see above), and
   * also returned.
   *
   * The current return type is as follows:
   *
   * Seq[(java.lang.String, GridLocateDocumentStrategy[SphereCell, SphereCellGrid], scala.collection.mutable.Map[evalobj.Document,opennlp.textgrounder.geolocate.EvaluationResult])] where val evalobj: opennlp.textgrounder.geolocate.CorpusEvaluator
   *
   * This means you get a sequence of tuples of
   * (strategyname, strategy, results)
   * where:
   * strategyname = name of strategy as given on command line
   * strategy = strategy object
   * results = map listing results for each document (an abstract type
   * defined in CorpusEvaluator; the result type EvaluationResult
   * is practically an abstract type, too -- the most useful dynamic
   * type in practice is DocumentEvaluationResult)
   */
  def run_after_setup() = {
    process_strategies(strategies)((stratname, strategy) =>
      create_document_evaluator(strategy, stratname))
  }
}

class GeolocateDocumentDriver extends
    GeolocateDocumentTypeDriver with StandaloneExperimentDriverStats {
  override type TParam = GeolocateDocumentParameters
}

abstract class GeolocateApp(appname: String) extends
    GridLocateApp(appname) {
  override type TDriver <: GeolocateDriver
}

object GeolocateDocumentApp extends GeolocateApp("geolocate-document") {
  type TDriver = GeolocateDocumentDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver()
}

