///////////////////////////////////////////////////////////////////////////////
//  GridLocate.scala
//
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

package opennlp.textgrounder.gridlocate

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

import opennlp.textgrounder.worddist._

import WordDist.memoizer._
import GridLocateDriver.Debug._

/*

This file contains the main driver module and associated strategy classes
for GridLocate projects.  "GridLocate" means applications that involve
searching for the best value (in some space) for a given test document by
dividing the space into a grid of some sort (not necessarily regular, and
not necessarily even with non-overlapping grid cells), aggregating all
the documents in a given cell, and finding the best value by searching for
the best grid cell and then returning some representative point (e.g. the
center) as the best value.  The original application was for geolocation,
i.e. assigning a latitude/longitude coordinate to a document, and the grid
was a regular tiling of the Earth's surface based on "squares" of a given
amount of latitude and longitude on each side.  But other applications are
possible, e.g. locating the date of a given biographical document, where
the space ranges over dates in time (one-dimensional) rather than over the
Earth's surface (two-dimensional).

*/

/////////////////////////////////////////////////////////////////////////////
//                               Structures                                //
/////////////////////////////////////////////////////////////////////////////

//  def print_structure(struct: Any, indent: Int = 0) {
//    val indstr = " "*indent
//    if (struct == null)
//      errprint("%snull", indstr)
//    else if (struct.isInstanceOf[Tuple2[Any,Any]]) {
//      val (x,y) = struct.asInstanceOf[Tuple2[Any,Any]]
//      print_structure(List(x,y), indent)
//    } else if (!(struct.isInstanceOf[Seq[Any]]) ||
//               struct.asInstanceOf[Seq[Any]].length == 0)
//      errprint("%s%s", indstr, struct)
//    else {
//      if (struct(0).isInstanceOf[String]) {
//        errprint("%s%s:", indstr, struct.asInstanceOf[String](0))
//        indstr += "  "
//        indent += 2
//        struct = struct.slice(1)
//      }
//      for (s <- struct) {
//        if (isinstance(s, Seq))
//          print_structure(s, indent + 2)
//        else if (isinstance(s, tuple)) {
//          val (key, value) = s
//          if (isinstance(value, Seq)) {
//            errprint("%s%s:", indstr, key)
//            print_structure(value, indent + 2)
//          }
//          else
//            errprint("%s%s: %s", indstr, key, value)
//        }
//        else
//          errprint("%s%s", indstr, s)
//      }
//    }
//  }

object GenericTypes {
  type GenericDistDocument = DistDocument[_]
  type GenericGeoCell = GeoCell[_, _ <: GenericDistDocument]
  type GenericCellGrid = CellGrid[_, _ <: GenericDistDocument,
    _ <: GenericGeoCell]
  type GenericDistDocumentTable =
    DistDocumentTable[_, _ <: GenericDistDocument, _ <: GenericCellGrid]
  type CellGenericCellGrid[TCell <: GenericGeoCell] = CellGrid[_, _ <: GenericDistDocument,
    TCell]
}
import GenericTypes._

/////////////////////////////////////////////////////////////////////////////
//                           Evaluation strategies                         //
/////////////////////////////////////////////////////////////////////////////

object UnigramStrategy {
  def check_unigram_dist(word_dist: WordDist) = {
    word_dist match {
      case x: UnigramWordDist => x
      case _ => throw new IllegalArgumentException("You must use a unigram word distribution with this strategy")
    }
  }
}

/**
 * Abstract class for reading documents from a test file and doing
 * document grid-location on them (as opposed, e.g., to trying to locate
 * individual words).
 */
abstract class GridLocateDocumentStrategy[
  TCell <: GenericGeoCell,
  TGrid <: CellGenericCellGrid[TCell]
](
  val cell_grid: TGrid
) {
  /**
   * For a given word distribution (describing a test document), return
   * an Iterable of tuples, each listing a particular cell on the Earth
   * and a score of some sort.  The results should be in sorted order,
   * with better cells earlier.  Currently there is no guarantee about
   * the particular scores returned; for some strategies, lower scores
   * are better, while for others, higher scores are better.  Currently,
   * the wrapper code outputs the score but doesn't otherwise use it.
   */
  def return_ranked_cells(word_dist: WordDist): Iterable[(TCell, Double)]
}

/**
 * Class that implements a very simple baseline strategy -- pick a random
 * cell.
 */

class RandomGridLocateDocumentStrategy[
  TCell <: GenericGeoCell,
  TGrid <: CellGenericCellGrid[TCell]
](
  cell_grid: TGrid
) extends GridLocateDocumentStrategy[TCell, TGrid](cell_grid) {
  def return_ranked_cells(word_dist: WordDist) = {
    val cells = cell_grid.iter_nonempty_cells()
      val shuffled = (new Random()).shuffle(cells)
      (for (cell <- shuffled) yield (cell, 0.0))
    }
  }

  /**
   * Class that implements a simple baseline strategy -- pick the "most
   * popular" cell (the one either with the largest number of documents, or
   * the most number of links pointing to it, if `internal_link` is true).
   */

  class MostPopularCellGridLocateDocumentStrategy[
    TCell <: GenericGeoCell,
    TGrid <: CellGenericCellGrid[TCell]
  ](
    cell_grid: TGrid,
    internal_link: Boolean
  ) extends GridLocateDocumentStrategy[TCell, TGrid](cell_grid) {
    var cached_ranked_mps: Iterable[(TCell, Double)] = null
    def return_ranked_cells(word_dist: WordDist) = {
      if (cached_ranked_mps == null) {
        cached_ranked_mps = (
          (for (cell <- cell_grid.iter_nonempty_cells())
            yield (cell,
              (if (internal_link)
                 cell.combined_dist.incoming_links
               else
                 cell.combined_dist.num_docs_for_links).toDouble)).
          toArray sortWith (_._2 > _._2))
      }
      cached_ranked_mps
    }
  }

  /**
   * Abstract class that implements a strategy for document geolocation that
   * involves directly comparing the document distribution against each cell
   * in turn and computing a score.
   *
   * @param prefer_minimum If true, lower scores are better; if false, higher
   *   scores are better.
   */
  abstract class MinMaxScoreStrategy[
    TCell <: GenericGeoCell,
    TGrid <: CellGenericCellGrid[TCell]
  ](
    cell_grid: TGrid,
    prefer_minimum: Boolean
  ) extends GridLocateDocumentStrategy[TCell, TGrid](cell_grid) {
    /**
     * Function to return the score of a document distribution against a
     * cell.
     */
    def score_cell(word_dist: WordDist, cell: TCell): Double

    /**
     * Compare a word distribution (for a document, typically) against all
     * cells. Return a sequence of tuples (cell, score) where 'cell'
     * indicates the cell and 'score' the score.
     */
    def return_ranked_cells_serially(word_dist: WordDist) = {
      /*
       The non-parallel way of doing things; Stephen resurrected it when
       merging the Dirichlet stuff.  Attempting to use the parallel method
       caused an assertion failure after about 1200 of 1895 documents using
       GeoText.
       */
        val buffer = mutable.Buffer[(TCell, Double)]()

        for (cell <- cell_grid.iter_nonempty_cells(nonempty_word_dist = true)) {
          if (debug("lots")) {
            errprint("Nonempty cell at indices %s = location %s, num_documents = %s",
              cell.describe_indices(), cell.describe_location(),
              cell.combined_dist.num_docs_for_word_dist)
          }

          val score = score_cell(word_dist, cell)
          buffer += ((cell, score))
        }
        buffer
    }

    /**
     * Compare a word distribution (for a document, typically) against all
     * cells. Return a sequence of tuples (cell, score) where 'cell'
     * indicates the cell and 'score' the score.
     */
    def return_ranked_cells_parallel(word_dist: WordDist) = {
      val cells = cell_grid.iter_nonempty_cells(nonempty_word_dist = true)
      cells.par.map(c => (c, score_cell(word_dist, c))).toBuffer
    }

    def return_ranked_cells(word_dist: WordDist) = {
      // FIXME, eliminate this global reference
      val parallel = !GridLocateDriver.Params.no_parallel
      val cell_buf = {
        if (parallel)
          return_ranked_cells_parallel(word_dist)
        else
          return_ranked_cells_serially(word_dist)
      }

      /* SCALABUG:
         If written simply as 'cell_buf sortWith (_._2 < _._2)',
         return type is mutable.Buffer.  However, if written as an
         if/then as follows, return type is Iterable, even though both
         forks have the same type of mutable.buffer!
       */
      val retval =
        if (prefer_minimum)
          cell_buf sortWith (_._2 < _._2)
        else
          cell_buf sortWith (_._2 > _._2)

      /* If doing things parallel, this code applies for debugging
         (serial has the debugging code embedded into it). */
      if (parallel && debug("lots")) {
        for ((cell, score) <- retval)
          errprint("Nonempty cell at indices %s = location %s, num_documents = %s, score = %s",
            cell.describe_indices(), cell.describe_location(),
            cell.combined_dist.num_docs_for_word_dist, score)
      }
      retval
    }
  }

  /**
   * Class that implements a strategy for document geolocation by computing
   * the KL-divergence between document and cell (approximately, how much
   * the word distributions differ).  Note that the KL-divergence as currently
   * implemented uses the smoothed word distributions.
   *
   * @param partial If true (the default), only do "partial" KL-divergence.
   * This only computes the divergence involving words in the document
   * distribution, rather than considering all words in the vocabulary.
   * @param symmetric If true, do a symmetric KL-divergence by computing
   * the divergence in both directions and averaging the two values.
   * (Not by default; the comparison is fundamentally asymmetric in
   * any case since it's comparing documents against cells.)
   */
  class KLDivergenceStrategy[
    TCell <: GenericGeoCell,
    TGrid <: CellGenericCellGrid[TCell]
  ](
    cell_grid: TGrid,
    partial: Boolean = true,
    symmetric: Boolean = false
  ) extends MinMaxScoreStrategy[TCell, TGrid](cell_grid, true) {

    var self_kl_cache: KLDivergenceCache = null
    val slow = false

    def call_kl_divergence(self: WordDist, other: WordDist) =
      self.kl_divergence(self_kl_cache, other, partial = partial)

    def score_cell(word_dist: WordDist, cell: TCell) = {
      val cell_word_dist = cell.combined_dist.word_dist
      var kldiv = call_kl_divergence(word_dist, cell_word_dist)
      if (symmetric) {
        val kldiv2 = cell_word_dist.kl_divergence(null, word_dist,
          partial = partial)
        kldiv = (kldiv + kldiv2) / 2.0
      }
      kldiv
    }

    override def return_ranked_cells(word_dist: WordDist) = {
      // This will be used by `score_cell` above.
      self_kl_cache = word_dist.get_kl_divergence_cache()

      val cells = super.return_ranked_cells(word_dist)

      if (debug("kldiv") && word_dist.isInstanceOf[FastSlowKLDivergence]) {
        val fast_slow_dist = word_dist.asInstanceOf[FastSlowKLDivergence]
        // Print out the words that contribute most to the KL divergence, for
        // the top-ranked cells
        val num_contrib_cells = 5
        val num_contrib_words = 25
        errprint("")
        errprint("KL-divergence debugging info:")
        for (((cell, _), i) <- cells.take(num_contrib_cells) zipWithIndex) {
          val (_, contribs) =
            fast_slow_dist.slow_kl_divergence_debug(
              cell.combined_dist.word_dist, partial = partial,
              return_contributing_words = true)
          errprint("  At rank #%s, cell %s:", i + 1, cell)
          errprint("    %30s  %s", "Word", "KL-div contribution")
          errprint("    %s", "-" * 50)
          // sort by absolute value of second element of tuple, in reverse order
          val items = (contribs.toArray sortWith ((x, y) => abs(x._2) > abs(y._2))).
            take(num_contrib_words)
          for ((word, contribval) <- items)
            errprint("    %30s  %s", word, contribval)
          errprint("")
        }
      }

      cells
    }
  }

  /**
   * Class that implements a strategy for document geolocation by computing
   * the cosine similarity between the distributions of document and cell.
   * FIXME: We really should transform the distributions by TF/IDF before
   * doing this.
   *
   * @param smoothed If true, use the smoothed word distributions. (By default,
   * use unsmoothed distributions.)
   * @param partial If true, only do "partial" cosine similarity.
   * This only computes the similarity involving words in the document
   * distribution, rather than considering all words in the vocabulary.
   */
  class CosineSimilarityStrategy[
    TCell <: GenericGeoCell,
    TGrid <: CellGenericCellGrid[TCell]
  ](
    cell_grid: TGrid,
    smoothed: Boolean = false,
    partial: Boolean = false
  ) extends MinMaxScoreStrategy[TCell, TGrid](cell_grid, true) {

    def score_cell(word_dist: WordDist, cell: TCell) = {
      var cossim =
        word_dist.cosine_similarity(cell.combined_dist.word_dist,
          partial = partial, smoothed = smoothed)
      assert(cossim >= 0.0)
      // Just in case of round-off problems
      assert(cossim <= 1.002)
      cossim = 1.002 - cossim
      cossim
    }
  }

  /** Use a Naive Bayes strategy for comparing document and cell. */
  class NaiveBayesDocumentStrategy[
    TCell <: GenericGeoCell,
    TGrid <: CellGenericCellGrid[TCell]
  ](
    cell_grid: TGrid,
    use_baseline: Boolean = true
  ) extends MinMaxScoreStrategy[TCell, TGrid](cell_grid, false) {

    def score_cell(word_dist: WordDist, cell: TCell) = {
      val params = cell_grid.table.driver.params
      // Determine respective weightings
      val (word_weight, baseline_weight) = (
        if (use_baseline) {
          if (params.naive_bayes_weighting == "equal") (1.0, 1.0)
          else {
            val bw = params.naive_bayes_baseline_weight.toDouble
            ((1.0 - bw) / word_dist.num_word_tokens, bw)
          }
        } else (1.0, 0.0))

      val word_logprob =
        cell.combined_dist.word_dist.get_nbayes_logprob(word_dist)
      val baseline_logprob =
        log(cell.combined_dist.num_docs_for_links.toDouble /
            cell_grid.total_num_docs_for_links)
      val logprob = (word_weight * word_logprob +
        baseline_weight * baseline_logprob)
      logprob
    }
  }

  abstract class AverageCellProbabilityStrategy[
    TCell <: GenericGeoCell,
    XTGrid <: CellGenericCellGrid[TCell]
  ](
    cell_grid: XTGrid
  ) extends GridLocateDocumentStrategy[TCell, XTGrid](cell_grid) {
    type TCellDistFactory <:
      CellDistFactory[_, _ <: GenericDistDocument, TCell] { type TGrid = XTGrid }
    def create_cell_dist_factory(lru_cache_size: Int): TCellDistFactory

    val cdist_factory =
      create_cell_dist_factory(cell_grid.table.driver.params.lru_cache_size)

    def return_ranked_cells(word_dist: WordDist) = {
      val celldist =
        cdist_factory.get_cell_dist_for_word_dist(cell_grid, word_dist)
      celldist.get_ranked_cells()
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  //                                Segmentation                             //
  /////////////////////////////////////////////////////////////////////////////

  // General idea: Keep track of best possible segmentations up to a maximum
  // number of segments.  Either do it using a maximum number of segmentations
  // (e.g. 100 or 1000) or all within a given factor of the best score (the
  // "beam width", e.g. 10^-4).  Then given the existing best segmentations,
  // we search for new segmentations with more segments by looking at all
  // possible ways of segmenting each of the existing best segments, and
  // finding the best score for each of these.  This is a slow process -- for
  // each segmentation, we have to iterate over all segments, and for each
  // segment we have to look at all possible ways of splitting it, and for
  // each split we have to look at all assignments of cells to the two
  // new segments.  It also seems that we're likely to consider the same
  // segmentation multiple times.
  //
  // In the case of per-word cell dists, we can maybe speed things up by
  // computing the non-normalized distributions over each paragraph and then
  // summing them up as necessary.

  /////////////////////////////////////////////////////////////////////////////
  //                                   Stopwords                             //
  /////////////////////////////////////////////////////////////////////////////

  object Stopwords {
    val stopwords_file_in_tg = "src/main/resources/data/%s/stopwords.txt"

    // Read in the list of stopwords from the given filename.
    def read_stopwords(filehand: FileHandler, stopwords_filename: String,
        language: String) = {
      def compute_stopwords_filename(filename: String) = {
        if (filename != null) filename
        else {
          val tgdir = TextGrounderInfo.get_textgrounder_dir
          // Concatenate directory and rest in most robust way
          filehand.join_filename(tgdir, stopwords_file_in_tg format language)
        }
      }
      val filename = compute_stopwords_filename(stopwords_filename)
      errprint("Reading stopwords from %s...", filename)
      filehand.openr(filename).toSet
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  //                                   Whitelist                             //
  /////////////////////////////////////////////////////////////////////////////

  object Whitelist {
    def read_whitelist(filehand: FileHandler, whitelist_filename: String): Set[String] = {
      if(whitelist_filename == null || whitelist_filename.length == 0)
        Nil.toSet
      else
        filehand.openr(whitelist_filename).toSet
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  //                                  Main code                              //
  /////////////////////////////////////////////////////////////////////////////

  /**
   * General class retrieving command-line arguments or storing programmatic
   * configuration parameters for a Cell-grid-based application.
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
  class GridLocateParameters(parser: ArgParser = null) extends
      ArgParserParameters(parser) {
    protected val ap =
      if (parser == null) new ArgParser("unknown") else parser

    var language =
      ap.option[String]("language", "lang",
         default = "eng",
         metavar = "LANG",
         aliases = Map("eng" -> Seq("en"), "por" -> Seq("pt"),
                       "deu" -> Seq("de")),
         help = """Name of language of corpus.  Currently used only to
  initialize the value of the stopwords file, if not explicitly set.
  Two- and three-letter ISO-639 codes can be used.  Currently recognized:
  English (en, eng); German (de, deu); Portuguese (pt, por).""")

    //// Input files
    var stopwords_file =
      ap.option[String]("stopwords-file", "sf",
        metavar = "FILE",
        help = """File containing list of stopwords.  If not specified,
  a default list of English stopwords (stored in the TextGrounder distribution)
  is used.""")

    var whitelist_file =
      ap.option[String]("whitelist-file", "wf",
         metavar = "FILE",
         help = """File containing a whitelist of words. If specified, ONLY
  words on the list will be read from any corpora; other words will be ignored.
  If not specified, all words (except those on the stopword list) will be
  read.""")

    var input_corpus =
      ap.multiOption[String]("i", "input-corpus",
        metavar = "DIR",
        help = """Directory containing an input corpus.  Documents in the
  corpus can be Wikipedia articles, individual tweets in Twitter, the set of all
  tweets for a given user, etc.  The corpus generally contains one or more
  "views" on the raw data comprising the corpus, with different views
  corresponding to differing ways of representing the original text of the
  documents -- as raw, word-split text; as unigram word counts; as bigram word
  counts; etc.  Each such view has a schema file and one or more document files.
  The latter contains all the data for describing each document, including
  title, split (training, dev or test) and other metadata, as well as the text
  or word counts that are used to create the textual distribution of the
  document.  The document files are laid out in a very simple database format,
  consisting of one document per line, where each line is composed of a fixed
  number of fields, separated by TAB characters. (E.g. one field would list
  the title, another the split, another all the word counts, etc.) A separate
  schema file lists the name of each expected field.  Some of these names
  (e.g. "title", "split", "text", "coord") have pre-defined meanings, but
  arbitrary names are allowed, so that additional corpus-specific information
  can be provided (e.g. retweet info for tweets that were retweeted from some
  other tweet, redirect info when a Wikipedia article is a redirect to another
  article, etc.).

  Multiple such files can be given by specifying the option multiple
  times.""")
    var eval_file =
      ap.multiOption[String]("e", "eval-file",
        metavar = "FILE",
        help = """File or directory containing files to evaluate on.
  Multiple such files/directories can be given by specifying the option multiple
  times.  If a directory is given, all files in the directory will be
  considered (but if an error occurs upon parsing a file, it will be ignored).
  Each file is read in and then disambiguation is performed.  Not used during
  document geolocation when --eval-format=internal (the default).""")

    var num_nearest_neighbors =
      ap.option[Int]("num-nearest-neighbors", "knn", default = 4,
        help = """Number of nearest neighbors (k in kNN); default is %default.""")

    var num_top_cells_to_output =
      ap.option[Int]("num-top-cells-to-output", "num-top-cells", default = 5,
        help = """Number of nearest neighbor cells to output; default is %default;
  -1 means output all""")

    var output_training_cell_dists =
      ap.flag("output-training-cell-dists", "output-training-cells",
        help = """Output the training cell distributions after they've been trained.""")

    //// Options indicating which documents to train on or evaluate
    var eval_set =
      ap.option[String]("eval-set", "es", metavar = "SET",
        default = "dev",
        choices = Seq("dev", "test"),
        aliases = Map("dev" -> Seq("devel")),
        help = """Set to use for evaluation during document geolocation when
  when --eval-format=internal ('dev' or 'devel' for the development set,
  'test' for the test set).  Default '%default'.""")
    var num_training_docs =
      ap.option[Int]("num-training-docs", "ntrain", metavar = "NUM",
        default = 0,
        help = """Maximum number of training documents to use.
  0 means no limit.  Default 0, i.e. no limit.""")
    var num_test_docs =
      ap.option[Int]("num-test-docs", "ntest", metavar = "NUM",
        default = 0,
        help = """Maximum number of test (evaluation) documents to process.
  0 means no limit.  Default 0, i.e. no limit.""")
    var skip_initial_test_docs =
      ap.option[Int]("skip-initial-test-docs", "skip-initial", metavar = "NUM",
        default = 0,
        help = """Skip this many test docs at beginning.  Default 0, i.e.
  don't skip any documents.""")
    var every_nth_test_doc =
      ap.option[Int]("every-nth-test-doc", "every-nth", metavar = "NUM",
        default = 1,
        help = """Only process every Nth test doc.  Default 1, i.e.
  process all.""")
    //  def skip_every_n_test_docs =
    //    ap.option[Int]("skip-every-n-test-docs", "skip-n", default = 0,
    //      help = """Skip this many after each one processed.  Default 0.""")

    //// Options used when creating word distributions
    var word_dist =
      ap.option[String]("word-dist", "wd",
        default = "pseudo-good-turing",
        choices = Seq("pseudo-good-turing", "pseudo-good-turing-bigram", "dirichlet", "jelinek-mercer"),
        aliases = Map("jelinek-mercer" -> Seq("jelinek"),
                      "pseudo-good-turing" -> Seq("pgt")),
        help = """Type of word distribution to use.  Possibilities are
  'pseudo-good-turing' (a simplified version of Good-Turing over a unigram
  distribution), 'dirichlet' (Dirichlet smoothing over a unigram distribution),
  'jelinek' or 'jelinek-mercer' (Jelinek-Mercer smoothing over a unigram
  distribution), and 'pseudo-good-turing-bigram' (a non-smoothed bigram
  distribution??).  Default '%default'.

  Note that all three involve some type of discounting, i.e. taking away a
  certain amount of probability mass compared with the maximum-likelihood
  distribution (which estimates 0 probability for words unobserved in a
  particular document), so that unobserved words can be assigned positive
  probability, based on their probability across all documents (i.e. their
  global distribution).  The difference is in how the discounting factor is
  computed, as well as the default value for whether to do interpolation
  (always mix the global distribution in) or back-off (use the global
  distribution only for words not seen in the document).  Jelinek-Mercer
  and Dirichlet do interpolation by default, while pseudo-Good-Turing
  does back-off; but this can be overridden using --interpolate.
  Jelinek-Mercer uses a fixed discounting factor; Dirichlet uses a
  discounting factor that gets smaller and smaller the larger the document,
  while pseudo-Good-Turing uses a discounting factor that reserves total
  mass for unobserved words that is equal to the total mass observed
  for words seen once.""")
    var interpolate =
      ap.option[String]("interpolate",
        default = "default",
        choices = Seq("yes", "no", "default"),
        aliases = Map("yes" -> Seq("interpolate"), "no" -> Seq("backoff")),
        help = """Whether to do interpolation rather than back-off.
  Possibilities are 'yes', 'no', and 'default' (which means 'yes' when doing
  Dirichlet or Jelinek-Mercer smoothing, 'no' when doing pseudo-Good-Turing
  smoothing).""")
    var jelinek_factor =
      ap.option[Double]("jelinek-factor", "jf",
        default = 0.3,
        help = """Smoothing factor when doing Jelinek-Mercer smoothing.
  The higher the value, the more relative weight to give to the global
  distribution vis-a-vis the document-specific distribution.  This
  should be a value between 0.0 (no smoothing at all) and 1.0 (total
  smoothing, i.e. use only the global distribution and ignore
  document-specific distributions entirely).  Default %default.""")
    var dirichlet_factor =
      ap.option[Double]("dirichlet-factor", "df",
        default = 500,
        help = """Smoothing factor when doing Dirichlet smoothing.
  The higher the value, the more relative weight to give to the global
  distribution vis-a-vis the document-specific distribution.  Default
  %default.""")
    var preserve_case_words =
      ap.flag("preserve-case-words", "pcw",
        help = """Don't fold the case of words used to compute and
  match against document distributions.  Note that in toponym resolution,
  this applies only to words in documents (currently used only in Naive Bayes
  matching), not to toponyms, which are always matched case-insensitively.""")
    var include_stopwords_in_document_dists =
      ap.flag("include-stopwords-in-document-dists",
        help = """Include stopwords when computing word distributions.""")
    var minimum_word_count =
      ap.option[Int]("minimum-word-count", "mwc", metavar = "NUM",
        default = 1,
        help = """Minimum count of words to consider in word
  distributions.  Words whose count is less than this value are ignored.""")
   var tf_idf =
     ap.flag("tf-idf", "tfidf",
        help = """Adjust word counts according to TF-IDF weighting (i.e.
  downweight words that occur in many documents).""")

    //// Options used when doing Naive Bayes geolocation
    var naive_bayes_weighting =
      ap.option[String]("naive-bayes-weighting", "nbw", metavar = "STRATEGY",
        default = "equal",
        choices = Seq("equal", "equal-words", "distance-weighted"),
        help = """Strategy for weighting the different probabilities
  that go into Naive Bayes.  If 'equal', do pure Naive Bayes, weighting the
  prior probability (baseline) and all word probabilities the same.  If
  'equal-words', weight all the words the same but collectively weight all words
  against the baseline, giving the baseline weight according to --baseline-weight
  and assigning the remainder to the words.  If 'distance-weighted', similar to
  'equal-words' but don't weight each word the same as each other word; instead,
  weight the words according to distance from the toponym.""")
    var naive_bayes_baseline_weight =
      ap.option[Double]("naive-bayes-baseline-weight", "nbbw",
        metavar = "WEIGHT",
        default = 0.5,
        help = """Relative weight to assign to the baseline (prior
  probability) when doing weighted Naive Bayes.  Default %default.""")

    //// Options used when doing ACP geolocation
    var lru_cache_size =
      ap.option[Int]("lru-cache-size", "lru", metavar = "SIZE",
        default = 400,
        help = """Number of entries in the LRU cache.  Default %default.
  Used only when --strategy=average-cell-probability.""")

    //// Miscellaneous options for controlling internal operation
    var no_parallel =
      ap.flag("no-parallel",
        help = """If true, don't do ranking computations in parallel.""")
    var test_kl =
      ap.flag("test-kl",
        help = """If true, run both fast and slow KL-divergence variations and
  test to make sure results are the same.""")

    //// Debugging/output options
    var max_time_per_stage =
      ap.option[Double]("max-time-per-stage", "mts", metavar = "SECONDS",
        default = 0.0,
        help = """Maximum time per stage in seconds.  If 0, no limit.
  Used for testing purposes.  Default 0, i.e. no limit.""")
    var no_individual_results =
      ap.flag("no-individual-results", "no-results",
        help = """Don't show individual results for each test document.""")
    var results_by_range =
      ap.flag("results-by-range",
        help = """Show results by range (of error distances and number of
  documents in true cell).  Not on by default as counters are used for this,
  and setting so many counters breaks some Hadoop installations.""")
    var oracle_results =
      ap.flag("oracle-results",
        help = """Only compute oracle results (much faster).""")
    var debug =
      ap.option[String]("d", "debug", metavar = "FLAGS",
        help = """Output debug info of the given types.  Multiple debug
  parameters can be specified, indicating different types of info to output.
  Separate parameters by spaces, colons or semicolons.  Params can be boolean,
  if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
  list-valued; multiple values are specified by including the parameter
  multiple times, or by separating values by a comma.

  The best way to figure out the possible parameters is by reading the
  source code. (Look for references to debug("foo") for boolean params,
  debugval("foo") for valueful params, or debuglist("foo") for list-valued
  params.) Some known debug flags:

  gridrank: For the given test document number (starting at 1), output
  a grid of the predicted rank for cells around the true cell.
  Multiple documents can have the rank output, e.g. --debug 'gridrank=45,58'
  (This will output info for documents 45 and 58.) This output can be
  postprocessed to generate nice graphs; this is used e.g. in Wing's thesis.

  gridranksize: Size of the grid, in numbers of documents on a side.
  This is a single number, and the grid will be a square centered on the
  true cell. (Default currently 11.)

  kldiv: Print out words contributing most to KL divergence.

  wordcountdocs: Regenerate document file, filtering out documents not
  seen in any counts file.

  some, lots, tons: General info of various sorts. (Document me.)

  cell: Print out info on each cell of the Earth as it's generated.  Also
  triggers some additional info during toponym resolution. (Document me.)

  commontop: Extra info for debugging
   --baseline-strategy=link-most-common-toponym.

  pcl-travel: Extra info for debugging --eval-format=pcl-travel.
  """)

  }

  class DebugSettings {
    // Debug params.  Different params indicate different info to output.
    // Specified using --debug.  Multiple params are separated by spaces,
    // colons or semicolons.  Params can be boolean, if given alone, or
    // valueful, if given as PARAM=VALUE.  Certain params are list-valued;
    // multiple values are specified by including the parameter multiple
    // times, or by separating values by a comma.
    val debug = booleanmap[String]()
    val debugval = stringmap[String]()
    val debuglist = bufmap[String, String]()

    var list_debug_params = Set[String]()

    // Register a list-valued debug param.
    def register_list_debug_param(param: String) {
      list_debug_params += param
    }

    def parse_debug_spec(debugspec: String) {
      val params = """[:;\s]+""".r.split(debugspec)
      // Allow params with values, and allow lists of values to be given
      // by repeating the param
      for (f <- params) {
        if (f contains '=') {
          val Array(param, value) = f.split("=", 2)
          if (list_debug_params contains param) {
            val values = "[,]".split(value)
            debuglist(param) ++= values
          } else
            debugval(param) = value
        } else
          debug(f) = true
      }
    }
  }

  /**
   * Base class for programmatic access to document/etc. geolocation.
   * Subclasses are for particular apps, e.g. GeolocateDocumentDriver for
   * document-level geolocation.
   *
   * NOTE: Currently the code has some values stored in singleton objects,
   * and no clear provided interface for resetting them.  This basically
   * means that there can be only one geolocation instance per JVM.
   * By now, most of the singleton objects have been removed, and it should
   * not be difficult to remove the final limitations so that multiple
   * drivers per JVM (possibly not at the same time) can be done.
   *
   * Basic operation:
   *
   * 1. Create an instance of the appropriate subclass of GeolocateParameters
   * (e.g. GeolocateDocumentParameters for document geolocation) and populate
   * it with the appropriate parameters.  Don't pass in any ArgParser instance,
   * as is the default; that way, the parameters will get initialized to their
   * default values, and you only have to change the ones you want to be
   * non-default.
   * 2. Call run(), passing in the instance you just created.
   *
   * NOTE: Currently, some of the fields of the GeolocateParameters-subclass
   * are changed to more canonical values.  If this is a problem, let me
   * know and I'll fix it.
   *
   * Evaluation output is currently written to standard error, and info is
   * also returned by the run() function.  There are some scripts to parse the
   * console output.  See below.
   */
  abstract class GridLocateDriver extends HadoopableArgParserExperimentDriver {
    type TDoc <: DistDocument[_]
    type TGrid <: CellGrid[_, TDoc, _ <: GeoCell[_, TDoc]]
    type TDocTable <: DistDocumentTable[_, TDoc, TGrid]
    override type TParam <: GridLocateParameters
    var stopwords: Set[String] = _
    var whitelist: Set[String] = _
    var cell_grid: TGrid = _
    var document_table: TDocTable = _
    var word_dist_factory: WordDistFactory = _
    var word_dist_constructor: WordDistConstructor = _
    var document_file_suffix: String = _
    var output_training_cell_dists: Boolean = _

    /**
     * Set the options to those as given.  NOTE: Currently, some of the
     * fields in this structure will be changed (canonicalized).  See above.
     * If options are illegal, an error will be signaled.
     *
     * @param options Object holding options to set
     */
    def handle_parameters() {
      /* FIXME: Eliminate this. */
      GridLocateDriver.Params = params

      if (params.debug != null)
        parse_debug_spec(params.debug)

      need_seq(params.input_corpus, "input-corpus")
    
      if (params.jelinek_factor < 0.0 || params.jelinek_factor > 1.0) {
        param_error("Value for --jelinek-factor must be between 0.0 and 1.0, but is %g" format params.jelinek_factor)
      }

      // Need to have `document_file_suffix` set early on, but factory
      // shouldn't be created till setup_for_run() because factory may
      // depend on auxiliary parameters set during this stage (e.g. during
      // GenerateKML).
      document_file_suffix = initialize_word_dist_suffix()
    }

    protected def initialize_document_table(word_dist_factory: WordDistFactory):
      TDocTable

    protected def initialize_cell_grid(table: TDocTable): TGrid

    protected def num_ngrams = {
      if (params.word_dist == "pseudo-good-turing-bigram") 2
      else 1
    }

    protected def initialize_word_dist_suffix() = {
      if (num_ngrams == 2)
        DistDocument.bigram_counts_suffix
      else
        DistDocument.unigram_counts_suffix
    }

    protected def get_stopwords() = {
      if (params.include_stopwords_in_document_dists) Set[String]()
      else stopwords
    }

    protected def get_whitelist() = {
      whitelist
    }

    protected def initialize_word_dist_constructor(factory: WordDistFactory) = {
      val the_stopwords = get_stopwords()
      val the_whitelist = get_whitelist()
      /* if (num_ngrams == 2)
        new DefaultBigramWordDistConstructor(factory, ...)
      else */
        new DefaultUnigramWordDistConstructor(
          factory,
          ignore_case = !params.preserve_case_words,
          stopwords = the_stopwords,
          whitelist = the_whitelist,
          minimum_word_count = params.minimum_word_count)
    }

    protected def initialize_word_dist_factory() = {
      /* if (params.word_dist == "pseudo-good-turing-bigram")
        new PGTBigramWordDistFactory(params.interpolate,
          params.discount_factor)
      else */ if (params.word_dist == "dirichlet")
      new DirichletUnigramWordDistFactory(params.interpolate,
        params.dirichlet_factor)
    else if (params.word_dist == "jelinek-mercer")
      new JelinekMercerUnigramWordDistFactory(params.interpolate,
        params.jelinek_factor)
    else
      new PseudoGoodTuringUnigramWordDistFactory(params.interpolate)
  }

  protected def read_stopwords() = {
    Stopwords.read_stopwords(get_file_handler, params.stopwords_file,
      params.language)
  }

  protected def read_whitelist() = {
    Whitelist.read_whitelist(get_file_handler, params.whitelist_file)
  }

  protected def read_documents(table: TDocTable) {
    for (fn <- params.input_corpus)
      table.read_training_documents(get_file_handler, fn,
        document_file_suffix, cell_grid)
    table.finish_document_loading()
  }

  def setup_for_run() {
    stopwords = read_stopwords()
    whitelist = read_whitelist()
    word_dist_factory = initialize_word_dist_factory()
    word_dist_constructor = initialize_word_dist_constructor(word_dist_factory)
    word_dist_factory.set_word_dist_constructor(word_dist_constructor)
    document_table = initialize_document_table(word_dist_factory)
    cell_grid = initialize_cell_grid(document_table)
    // This accesses the stopwords and whitelist through the pointer to this in
    // document_table.
    read_documents(document_table)
    if (debug("stop-after-reading-dists")) {
      errprint("Stopping abruptly because debug flag stop-after-reading-dists set")
      output_resource_usage()
      // We throw to top level before exiting because hprof tends to report
      // too much garbage as if it were live.  Unwinding the stack may fix
      // some of that.  If you don't want this unwinding, comment out the
      // throw and uncomment the call to System.exit().
      throw new GridLocateAbruptExit
      // System.exit(0)
    }
    cell_grid.finish()
    if(params.output_training_cell_dists) {
      for(cell <- cell_grid.iter_nonempty_cells(true)) {
        print(cell.shortstr+"\t")
        val word_dist = cell.combined_dist.word_dist
        println(word_dist.toString)        
      }
    }
  }

  /**
   * Given a list of strategies, process each in turn, evaluating all
   * documents using the strategy.
   *
   * @param strategies List of (name, strategy) pairs, giving strategy
   *   names and objects.
   * @param geneval Function to create an evaluator object to evaluate
   *   all documents, given a strategy.
   * @tparam T Supertype of all the strategy objects.
   */
  protected def process_strategies[T](strategies: Seq[(String, T)])(
      geneval: (String, T) => CorpusEvaluator[_,_]) = {
    for ((stratname, strategy) <- strategies) yield {
      val evalobj = geneval(stratname, strategy)
      // For --eval-format=internal, there is no eval file.  To make the
      // evaluation loop work properly, we pretend like there's a single
      // eval file whose value is null.
      val iterfiles =
        if (params.eval_file.length > 0) params.eval_file
        else params.input_corpus
      evalobj.process_files(get_file_handler, iterfiles)
      evalobj.finish()
      (stratname, strategy, evalobj)
    }
  }
}

object GridLocateDriver {
  var Params: GridLocateParameters = _
  val Debug: DebugSettings = new DebugSettings

  // Debug flags (from SphereCellGridEvaluator) -- need to set them
  // here before we parse the command-line debug settings. (FIXME, should
  // be a better way that introduces fewer long-range dependencies like
  // this)
  //
  //  gridrank: For the given test document number (starting at 1), output
  //            a grid of the predicted rank for cells around the true
  //            cell.  Multiple documents can have the rank output, e.g.
  //
  //            --debug 'gridrank=45,58'
  //
  //            (This will output info for documents 45 and 58.)
  //
  //  gridranksize: Size of the grid, in numbers of documents on a side.
  //                This is a single number, and the grid will be a square
  //                centered on the true cell.
  register_list_debug_param("gridrank")
  debugval("gridranksize") = "11"
}

class GridLocateAbruptExit extends Throwable { }

abstract class GridLocateApp(appname: String) extends
    ExperimentDriverApp(appname) {
  type TDriver <: GridLocateDriver

  override def run_program() = {
    try {
      super.run_program()
    } catch {
      case e:GridLocateAbruptExit => {
        errprint("Caught abrupt exit throw, exiting")
        0
      }
    }
  }
}
