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

////////
//////// Geolocate.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import util.matching.Regex
import util.Random
import math._
import collection.mutable

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.textutil._

import WordDist.memoizer._
import GeolocateDriver.Params
import GeolocateDriver.Debug._

/*

This module is the main driver module for the Geolocate subproject.

*/

/////////////////////////////////////////////////////////////////////////////
//                               Structures                                //
/////////////////////////////////////////////////////////////////////////////

//  def print_structure(struct: Any, indent: Int=0) {
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

/////////////////////////////////////////////////////////////////////////////
//                      Wikipedia/Twitter/etc. documents                   //
/////////////////////////////////////////////////////////////////////////////

//////////////////////  DistDocument table

/**
 * Class maintaining tables listing all documents and mapping between
 * names, ID's and documents.  Objects corresponding to redirect articles
 * in Wikipedia should not be present anywhere in this table; instead, the
 * name of the redirect article should point to the document object for the
 * article pointed to by the redirect.
 */
class DistDocumentTable(
  /* FIXME: The point of this parameter is so that we can use the counter
     mechanism instead of our own statistics.  Implement this. */
  val driver_stats: ExperimentDriverStats,
  val word_dist_factory: WordDistFactory
) {
  /**
   * A mechanism for wrapping task counters so that they can be stored
   * in variables and incremented simply using +=.  Note that access to
   * them still needs to go through `value`, unfortunately. (Even marking
   * `value` as implicit isn't enough as the function won't get invoked
   * unless we're in an environment requiring an integral value.  This means
   * it won't get invoked in print statements, variable assignments, etc.)
   *
   * It would be nice to move this elsewhere; we'd have to pass in
   * `driver_stats`, though.
   *
   * NOTE: The counters are task-specific because currently each task
   * reads the entire set of training documents into memory.  We could avoid
   * this by splitting the tasks so that each task is commissioned to
   * run over a specific portion of the Earth rather than a specific
   * set of test documents.  Note that if we further split things so that
   * each task handled both a portion of test documents and a portion of
   * the Earth, it would be somewhat trickier, depending on exactly how
   * we write the code -- for a given set of test documents, different
   * portions of the Earth would be reading in different training documents,
   * so we'd presumably want their counts to add; but we might not want
   * all counts to add.
   */

  def construct_task_counter_name(name: String) =
    "bytask." + driver_stats.get_task_id + "." + name

  def increment_task_counter(name: String, byvalue: Long = 1) {
    driver_stats.increment_local_counter(construct_task_counter_name(name),
      byvalue)
  }

  def get_task_counter(name: String) = {
    driver_stats.get_local_counter(construct_task_counter_name(name))
  }

  class TaskCounterWrapper(name: String) {
    def value = get_task_counter(name)

    def +=(incr: Long) {
      increment_task_counter(name, incr)
    }
  }

  def create_counter_wrapper(prefix: String, split: String) =
    new TaskCounterWrapper(prefix + "." + split)

  def countermap(prefix: String) =
    new SettingDefaultHashMap[String, TaskCounterWrapper](
      create_counter_wrapper(prefix, _))

  /**********************************************************************/
  /*                     Begin DistDocumentTable proper                   */
  /**********************************************************************/

  /**
   * Mapping from document names to DistDocument objects, using the actual case of
   * the document.
   */
  val name_to_document = mutable.Map[String, DistDocument]()

  /**
   * List of documents in each split.
   */
  val documents_by_split = bufmap[String, DistDocument]()

  /**
   * Num of documents with word-count information but not in table.
   */
  val num_documents_with_word_counts_but_not_in_table =
    new TaskCounterWrapper("documents_with_word_counts_but_not_in_table")

  /**
   * Num of documents with word-count information (whether or not in table).
   */
  val num_documents_with_word_counts =
    new TaskCounterWrapper("documents_with_word_counts")

  /** 
   * Num of documents in each split with word-count information seen.
   */
  val num_word_count_documents_by_split =
    countermap("word_count_documents_by_split")

  /**
   * Num of documents in each split with a computed distribution.
   * (Not the same as the previous since we don't compute the distribution of
   * documents in either the test or dev set depending on which one is used.)
   */
  val num_dist_documents_by_split =
    countermap("num_dist_documents_by_split")

  /**
   * Total # of word tokens for all documents in each split.
   */
  val word_tokens_by_split =
    countermap("word_tokens_by_split")

  /**
   * Total # of incoming links for all documents in each split.
   */
  val incoming_links_by_split =
    countermap("incoming_links_by_split")

  /**
   * Map from short name (lowercased) to list of documents.
   * The short name for a document is computed from the document's name.  If
   * the document name has a comma, the short name is the part before the
   * comma, e.g. the short name of "Springfield, Ohio" is "Springfield".
   * If the name has no comma, the short name is the same as the document
   * name.  The idea is that the short name should be the same as one of
   * the toponyms used to refer to the document.
   */
  val short_lower_name_to_documents = bufmap[String, DistDocument]()

  /**
   * Map from tuple (NAME, DIV) for documents of the form "Springfield, Ohio",
   * lowercased.
   */
  val lower_name_div_to_documents = bufmap[(String, String), DistDocument]()

  /**
   * For each toponym, list of documents matching the name.
   */
  val lower_toponym_to_document = bufmap[String, DistDocument]()

  /**
   * Mapping from lowercased document names to DistDocument objects
   */
  val lower_name_to_documents = bufmap[String, DistDocument]()

  /**
   * Look up a document named NAME and return the associated document.
   * Note that document names are case-sensitive but the first letter needs to
   * be capitalized.
   */
  def lookup_document(name: String) = {
    assert(name != null)
    name_to_document.getOrElse(capfirst(name), null)
  }

  /**
   * Record the document as having NAME as one of its names (there may be
   * multiple names, due to redirects).  Also add to related lists mapping
   * lowercased form, short form, etc.
   */ 
  def record_document_name(name: String, doc: DistDocument) {
    // Must pass in properly cased name
    // errprint("name=%s, capfirst=%s", name, capfirst(name))
    // println("length=%s" format name.length)
    // if (name.length > 1) {
    //   println("name(0)=0x%x" format name(0).toInt)
    //   println("name(1)=0x%x" format name(1).toInt)
    //   println("capfirst(0)=0x%x" format capfirst(name)(0).toInt)
    // }
    assert(name == capfirst(name))
    name_to_document(name) = doc
    val loname = name.toLowerCase
    lower_name_to_documents(loname) += doc
    val (short, div) = GeoDocument.compute_short_form(loname)
    if (div != null)
      lower_name_div_to_documents((short, div)) += doc
    short_lower_name_to_documents(short) += doc
    if (!(lower_toponym_to_document(loname) contains doc))
      lower_toponym_to_document(loname) += doc
    if (short != loname && !(lower_toponym_to_document(short) contains doc))
      lower_toponym_to_document(short) += doc
  }

  /**
   * Record either a normal document ('docfrom' same as 'docto') or a
   * redirect ('docfrom' redirects to 'docto').
   */
  def record_document(docfrom: DistDocument, docto: DistDocument) {
    record_document_name(docfrom.title, docto)
    val redir = !(docfrom eq docto)
    val split = docto.split
    val fromlinks = docfrom.adjusted_incoming_links
    incoming_links_by_split(split) += fromlinks
    if (!redir) {
      documents_by_split(split) += docto
    } else if (fromlinks != 0) {
      // Add count of links pointing to a redirect to count of links
      // pointing to the document redirected to, so that the total incoming
      // link count of a document includes any redirects to that document.
      docto.incoming_links = Some(docto.adjusted_incoming_links + fromlinks)
    }
  }

  def create_document(params: Map[String, String]) = new DistDocument(params)

  def would_add_document_to_list(doc: DistDocument) = {
    if (doc.namespace != "Main")
      false
    else if (doc.redir.length > 0)
      false
    else doc.coord != null
  }

  def read_document_data(filehand: FileHandler, filename: String,
      cell_grid: CellGrid) {
    val redirects = mutable.Buffer[DistDocument]()

    def process(params: Map[String, String]) {
      val doc = create_document(params)
      if (doc.namespace != "Main")
        return
      if (doc.redir.length > 0)
        redirects += doc
      else if (doc.coord != null) {
        record_document(doc, doc)
        cell_grid.add_document_to_cell(doc)
      }
    }

    GeoDocumentData.read_document_file(filehand, filename, process,
      maxtime = Params.max_time_per_stage)

    for (x <- redirects) {
      val reddoc = lookup_document(x.redir)
      if (reddoc != null)
        record_document(x, reddoc)
    }
  }

  def finish_document_distributions() {
    // Figure out the value of OVERALL_UNSEEN_MASS for each document.
    for ((split, table) <- documents_by_split) {
      var totaltoks = 0
      var numdocs = 0
      for (doc <- table) {
        if (doc.dist != null) {
          /* FIXME: Move this finish() earlier, and split into
             before/after global. */
          doc.dist.finish(minimum_word_count = Params.minimum_word_count)
          totaltoks += doc.dist.num_word_tokens
          numdocs += 1
        }
      }
      num_dist_documents_by_split(split) += numdocs
      word_tokens_by_split(split) += totaltoks
    }
  }

  def clear_training_document_distributions() {
    for (doc <- documents_by_split("training"))
      doc.dist = null
  }

  def finish_word_counts() {
    word_dist_factory.finish_global_distribution()
    finish_document_distributions()
    errprint("")
    errprint("-------------------------------------------------------------------------")
    errprint("Document count statistics:")
    var total_docs_in_table = 0L
    var total_docs_with_word_counts = 0L
    var total_docs_with_dists = 0L
    for ((split, totaltoks) <- word_tokens_by_split) {
      errprint("For split '%s':", split)
      val docs_in_table = documents_by_split(split).length
      val docs_with_word_counts = num_word_count_documents_by_split(split).value
      val docs_with_dists = num_dist_documents_by_split(split).value
      total_docs_in_table += docs_in_table
      total_docs_with_word_counts += docs_with_word_counts
      total_docs_with_dists += docs_with_dists
      errprint("  %s documents in document table", docs_in_table)
      errprint("  %s documents with word counts seen (and in table)", docs_with_word_counts)
      errprint("  %s documents with distribution computed, %s total tokens, %.2f tokens/document",
        docs_with_dists, totaltoks.value,
        // Avoid division by zero
        totaltoks.value.toDouble / (docs_in_table + 1e-100))
    }
    errprint("Total: %s documents with word counts seen",
      num_documents_with_word_counts.value)
    errprint("Total: %s documents in document table", total_docs_in_table)
    errprint("Total: %s documents with word counts seen but not in document table",
      num_documents_with_word_counts_but_not_in_table.value)
    errprint("Total: %s documents with word counts seen (and in table)",
      total_docs_with_word_counts)
    errprint("Total: %s documents with distribution computed",
      total_docs_with_dists)
  }

  def construct_candidates(toponym: String) = {
    val lotop = toponym.toLowerCase
    lower_toponym_to_document(lotop)
  }

  def word_is_toponym(word: String) = {
    val lw = word.toLowerCase
    lower_toponym_to_document contains lw
  }
}

///////////////////////// DistDocuments

/**
 * A document for geolocation, with a word distribution.  Documents can come
 * from Wikipedia articles, individual tweets, Twitter feeds (all tweets from
 * a user), etc.
 */ 
class DistDocument(params: Map[String, String]) extends GeoDocument(params)
  with EvaluationDocument {
  /**
   * Object containing word distribution of this document.
   */
  var dist: WordDist = null

  override def toString() = {
    var coordstr = if (coord != null) " at %s" format coord else ""
    val redirstr =
      if (redir.length > 0) ", redirect to %s" format redir else ""
    "%s(%s)%s%s" format (title, id, coordstr, redirstr)
  }

  // def __repr__() = "DistDocument(%s)" format toString.encode("utf-8")

  def shortstr() = "%s" format title

  def struct() =
    <DistDocument>
      <title>{ title }</title>
      <id>{ id }</id>
      {
        if (coord != null)
          <location>{ coord }</location>
      }
      {
        if (redir.length > 0)
          <redirectTo>{ redir }</redirectTo>
      }
    </DistDocument>

  def distance_to_coord(coord2: Coord) = spheredist(coord, coord2)
}

/////////////////////////////////////////////////////////////////////////////
//                           Evaluation strategies                         //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for reading documents from a test file and doing
 * document geolocation on them (as opposed e.g. to toponym resolution).
 */
abstract class GeolocateDocumentStrategy(val cell_grid: CellGrid) {
  /**
   * For a given word distribution (describing a test document), return
   * an Iterable of tuples, each listing a particular cell on the Earth
   * and a score of some sort.  The results should be in sorted order,
   * with better cells earlier.  Currently there is no guarantee about
   * the particular scores returned; for some strategies, lower scores
   * are better, while for others, higher scores are better.  Currently,
   * the wrapper code outputs the score but doesn't otherwise use it.
   */
  def return_ranked_cells(word_dist: WordDist): Iterable[(GeoCell, Double)]
}

/**
 * Class that implements the baseline strategies for document geolocation.
 * 'baseline_strategy' specifies the particular strategy to use.
 */
class RandomGeolocateDocumentStrategy(
  cell_grid: CellGrid
) extends GeolocateDocumentStrategy(cell_grid) {
  def return_ranked_cells(word_dist: WordDist) = {
    val cells = cell_grid.iter_nonempty_cells()
    val shuffled = (new Random()).shuffle(cells)
    (for (cell <- shuffled) yield (cell, 0.0))
  }
}

class MostPopularCellGeolocateDocumentStrategy(
  cell_grid: CellGrid,
  internal_link: Boolean
) extends GeolocateDocumentStrategy(cell_grid) {
  var cached_ranked_mps: Iterable[(GeoCell, Double)] = null
  def return_ranked_cells(word_dist: WordDist) = {
    if (cached_ranked_mps == null) {
      cached_ranked_mps = (
        (for (cell <- cell_grid.iter_nonempty_cells())
          yield (cell,
            (if (internal_link)
               cell.word_dist_wrapper.incoming_links
             else
               cell.word_dist_wrapper.num_docs_for_links).toDouble)).
        toArray sortWith (_._2 > _._2))
    }
    cached_ranked_mps
  }
}

class CellDistMostCommonToponymGeolocateDocumentStrategy(
  cell_grid: CellGrid
) extends GeolocateDocumentStrategy(cell_grid) {
  val cdist_factory = new CellDistFactory(Params.lru_cache_size)

  def return_ranked_cells(word_dist: WordDist) = {
    // Look for a toponym, then a proper noun, then any word.
    // FIXME: How can 'word' be null?
    // FIXME: Use invalid_word
    // FIXME: Should predicate be passed an index and have to do its own
    // unmemoizing?
    var maxword = word_dist.find_most_common_word(
      word => word(0).isUpper && cell_grid.table.word_is_toponym(word))
    if (maxword == None) {
      maxword = word_dist.find_most_common_word(
        word => word(0).isUpper)
    }
    if (maxword == None)
      maxword = word_dist.find_most_common_word(word => true)
    cdist_factory.get_cell_dist(cell_grid, maxword.get).get_ranked_cells()
  }
}

class LinkMostCommonToponymGeolocateDocumentStrategy(
  cell_grid: CellGrid
) extends GeolocateDocumentStrategy(cell_grid) {
  def return_ranked_cells(word_dist: WordDist) = {
    var maxword = word_dist.find_most_common_word(
      word => word(0).isUpper && cell_grid.table.word_is_toponym(word))
    if (maxword == None) {
      maxword = word_dist.find_most_common_word(
        word => cell_grid.table.word_is_toponym(word))
    }
    if (debug("commontop"))
      errprint("  maxword = %s", maxword)
    val cands =
      if (maxword != None)
        cell_grid.table.construct_candidates(
          unmemoize_word(maxword.get))
      else Seq[DistDocument]()
    if (debug("commontop"))
      errprint("  candidates = %s", cands)
    // Sort candidate list by number of incoming links
    val candlinks =
      (for (cand <- cands) yield (cand, cand.adjusted_incoming_links.toDouble)).
        // sort by second element of tuple, in reverse order
        sortWith(_._2 > _._2)
    if (debug("commontop"))
      errprint("  sorted candidates = %s", candlinks)

    def find_good_cells_for_coord(cands: Iterable[(DistDocument, Double)]) = {
      for {
        (cand, links) <- candlinks
        val cell = {
          val retval = cell_grid.find_best_cell_for_coord(cand.coord)
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
      new RandomGeolocateDocumentStrategy(cell_grid).return_ranked_cells(word_dist))
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
abstract class MinMaxScoreStrategy(
  cell_grid: CellGrid,
  prefer_minimum: Boolean
) extends GeolocateDocumentStrategy(cell_grid) {
  /**
   * Function to return the score of a document distribution against a
   * cell.
   */
  def score_cell(word_dist: WordDist, cell: GeoCell): Double

  /**
   * Compare a word distribution (for a document, typically) against all
   * cells. Return a sequence of tuples (cell, score) where 'cell'
   * indicates the cell and 'score' the score.
   */
  def return_ranked_cells(word_dist: WordDist) = {
    val cell_buf = mutable.Buffer[(GeoCell, Double)]()
    for (
      cell <- cell_grid.iter_nonempty_cells(nonempty_word_dist = true)
    ) {
      if (debug("lots")) {
        errprint("Nonempty cell at indices %s = location %s, num_documents = %s",
          cell.describe_indices(), cell.describe_location(),
          cell.word_dist_wrapper.num_docs_for_word_dist)
      }

      val score = score_cell(word_dist, cell)
      cell_buf += ((cell, score))
    }

    /* SCALABUG:
       If written simply as 'cell_buf sortWith (_._2 < _._2)',
       return type is mutable.Buffer.  However, if written as an
       if/then as follows, return type is Iterable, even though both
       forks have the same type of mutable.buffer!
     */
    if (prefer_minimum)
      cell_buf sortWith (_._2 < _._2)
    else
      cell_buf sortWith (_._2 > _._2)
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
class KLDivergenceStrategy(
  cell_grid: CellGrid,
  partial: Boolean = true,
  symmetric: Boolean = false
) extends MinMaxScoreStrategy(cell_grid, true) {

  def score_cell(word_dist: WordDist, cell: GeoCell) = {
    var kldiv = word_dist.fast_kl_divergence(cell.word_dist,
      partial = partial)
    //var kldiv = word_dist.test_kl_divergence(cell.word_dist,
    //  partial = partial)
    if (symmetric) {
      val kldiv2 = cell.word_dist.fast_kl_divergence(word_dist,
        partial = partial)
      kldiv = (kldiv + kldiv2) / 2.0
    }
    //kldiv = word_dist.test_kl_divergence(cell.word_dist,
    //                           partial=partial)
    //errprint("For cell %s, KL divergence %.3f", cell, kldiv)
    kldiv
  }

  override def return_ranked_cells(word_dist: WordDist) = {
    val cells = super.return_ranked_cells(word_dist)

    if (debug("kldiv")) {
      // Print out the words that contribute most to the KL divergence, for
      // the top-ranked cells
      val num_contrib_cells = 5
      val num_contrib_words = 25
      errprint("")
      errprint("KL-divergence debugging info:")
      for (((cell, _), i) <- cells.take(num_contrib_cells) zipWithIndex) {
        val (_, contribs) =
          word_dist.slow_kl_divergence_debug(
            cell.word_dist, partial = partial,
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
class CosineSimilarityStrategy(
  cell_grid: CellGrid,
  smoothed: Boolean = false,
  partial: Boolean = false
) extends MinMaxScoreStrategy(cell_grid, true) {

  def score_cell(word_dist: WordDist, cell: GeoCell) = {
    var cossim =
      if (smoothed)
        word_dist.fast_smoothed_cosine_similarity(cell.word_dist,
          partial = partial)
      else
        word_dist.fast_cosine_similarity(cell.word_dist,
          partial = partial)
    assert(cossim >= 0.0)
    // Just in case of round-off problems
    assert(cossim <= 1.002)
    cossim = 1.002 - cossim
    cossim
  }
}

/** Use a Naive Bayes strategy for comparing document and cell. */
class NaiveBayesDocumentStrategy(
  cell_grid: CellGrid,
  use_baseline: Boolean = true
) extends MinMaxScoreStrategy(cell_grid, false) {

  def score_cell(word_dist: WordDist, cell: GeoCell) = {
    // Determine respective weightings
    val (word_weight, baseline_weight) = (
      if (use_baseline) {
        if (Params.naive_bayes_weighting == "equal") (1.0, 1.0)
        else {
          val bw = Params.naive_bayes_baseline_weight.toDouble
          ((1.0 - bw) / word_dist.num_word_tokens, bw)
        }
      } else (1.0, 0.0))

    val word_logprob = cell.word_dist.get_nbayes_logprob(word_dist)
    val baseline_logprob =
      log(cell.word_dist_wrapper.num_docs_for_links.toDouble /
          cell_grid.total_num_docs_for_links)
    val logprob = (word_weight * word_logprob +
      baseline_weight * baseline_logprob)
    logprob
  }
}

class AverageCellProbabilityStrategy(
  cell_grid: CellGrid
) extends GeolocateDocumentStrategy(cell_grid) {
  val cdist_factory = new CellDistFactory(Params.lru_cache_size)

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
  val stopwords_file_in_tg = "data/lists/stopwords.english"

  // Read in the list of stopwords from the given filename.
  def read_stopwords(filehand: FileHandler, stopwords_filename: String) = {
    def compute_stopwords_filename(filename: String) = {
      if (filename != null) filename
      else {
        val tgdir = TextGrounderInfo.get_textgrounder_dir
        // Concatenate directory and rest in most robust way
        filehand.join_filename(tgdir, stopwords_file_in_tg)
      }
    }
    val filename = compute_stopwords_filename(stopwords_filename)
    errprint("Reading stopwords from %s...", filename)
    filehand.openr(filename).toSet
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
class GeolocateParameters(parser: ArgParser = null) extends
    ArgParserParameters(parser) {
  protected val ap =
    if (parser == null) new ArgParser("unknown") else parser

  //// Input files
  var stopwords_file =
    ap.option[String]("stopwords-file",
      metavar = "FILE",
      help = """File containing list of stopwords.  If not specified,
a default list of English stopwords (stored in the TextGrounder distribution)
is used.""")

  var document_file =
    ap.multiOption[String]("a", "document-file",
      metavar = "FILE",
      help = """File containing info about documents.  Documents can be
Wikipedia articles, individual tweets in Twitter, the set of all tweets for
a given user, etc.  This file lists per-document information such as the
document's title, the split (training, dev, or test) that the document is in,
and the document's location.  It does not list the actual word-count
information for the documents; that is held in a separate counts file,
specified using --counts-file.

Multiple such files can be given by specifying the option multiple
times.""")
  var counts_file =
    ap.multiOption[String]("counts-file", "cf",
      metavar = "FILE",
      help = """File containing word counts for documents.  There are scripts
in the 'python' directory for generating counts in the proper format from
Wikipedia articles, Twitter tweets, etc.  Multiple such files can be given
by specifying the option multiple times.""")
  var eval_file =
    ap.multiOption[String]("e", "eval-file",
      metavar = "FILE",
      help = """File or directory containing files to evaluate on.
Multiple such files/directories can be given by specifying the option multiple
times.  If a directory is given, all files in the directory will be
considered (but if an error occurs upon parsing a file, it will be ignored).
Each file is read in and then disambiguation is performed.  Not used during
document geolocation when --eval-format=internal (the default).""")

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
  //    ap.option[Int]("skip-every-n-test-docs", "skip-n", default=0,
  //      help="""Skip this many after each one processed.  Default 0.""")

  //// Options indicating how to generate the cells we compare against
  var degrees_per_cell =
    ap.option[Double]("degrees-per-cell", "dpc", metavar="DEGREES",
      default = 1.0,
      help = """Size (in degrees, a floating-point number) of the tiling
cells that cover the Earth.  Default %default. """)
  var miles_per_cell =
    ap.option[Double]("miles-per-cell", "mpc", metavar="MILES",
      help = """Size (in miles, a floating-point number) of the tiling
cells that cover the Earth.  If given, it overrides the value of
--degrees-per-cell.  No default, as the default of --degrees-per-cell
is used.""")
  var km_per_cell =
    ap.option[Double]("km-per-cell", "kpc", metavar="KM",
      help = """Size (in kilometers, a floating-point number) of the tiling
cells that cover the Earth.  If given, it overrides the value of
--degrees-per-cell.  No default, as the default of --degrees-per-cell
is used.""")
  var width_of_multi_cell =
    ap.option[Int]("width-of-multi-cell", metavar="CELLS", default = 1,
      help = """Width of the cell used to compute a statistical
distribution for geolocation purposes, in terms of number of tiling cells.
NOTE: It's unlikely you want to change this.  It may be removed entirely in
later versions.  In normal circumstances, the value is 1, i.e. use a single
tiling cell to compute each multi cell.  If the value is more than
1, the multi cells overlap.""")

  //// Options for using KD trees, and related parameters
  var kd_tree = 
    ap.option[Boolean]("kd-tree", "kd", "kdtree",
      help = """Specifies we should use a KD tree rather than uniform
grid cell.""")

  var kd_bucket_size =
    ap.option[Int]("kd-bucket-size", "kdbs", "bucket-size", default=200,
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
    ap.option[Boolean]("kd-backoff", "kd-use-backoff", default=false,
      help = """Specifies if we should back off to larger cell distributions.""")


  //// Options used when creating word distributions
  var word_dist =
    ap.option[String]("word-dist", "wd",
      default = "pseudo-good-turing-unigram",
      choices = Seq("pseudo-good-turing-unigram", "pseudo-good-turing-bigram"),
      help = """Type of word distribution to use.  Possibilities are
'pseudo-good-turing-unigram' (a simplified version of Good-Turing over a unigram
distribution) and 'pseudo-good-turing-bigram' (a non-smoothed bigram distribution).
Default '%default'.""")
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

  //// Debugging/output options
  var max_time_per_stage =
    ap.option[Double]("max-time-per-stage", "mts", metavar = "SECONDS",
      default = 0.0,
      help = """Maximum time per stage in seconds.  If 0, no limit.
Used for testing purposes.  Default 0, i.e. no limit.""")
  var no_individual_results =
    ap.flag("no-individual-results", "no-results",
      help = """Don't show individual results for each test document.""")
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
abstract class GeolocateDriver extends
    ArgParserExperimentDriver with ExperimentDriverStats {
  override type ParamType <: GeolocateParameters
  var degrees_per_cell = 0.0
  var stopwords: Set[String] = _
  var cell_grid: CellGrid = _
  var document_table: DistDocumentTable = _
  var word_dist_factory: WordDistFactory = _

  /**
   * FileHandler object for this driver.
   */
  private val local_file_handler = new LocalFileHandler

  def get_file_handler: FileHandler = local_file_handler

  /**
   * Set the options to those as given.  NOTE: Currently, some of the
   * fields in this structure will be changed (canonicalized).  See above.
   * If options are illegal, an error will be signaled.
   *
   * @param options Object holding options to set
   */
  def handle_parameters() {
    GeolocateDriver.Params = params

    if (params.debug != null)
      parse_debug_spec(params.debug)

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

    need_seq(params.document_file, "document-file")
  }

  protected def initialize_document_table(word_dist_factory: WordDistFactory) = {
    new DistDocumentTable(this, word_dist_factory)
  }

  protected def initialize_cell_grid(table: DistDocumentTable) = {
    if (params.kd_tree)
      KdTreeCellGrid(table, params.kd_bucket_size, params.kd_split_method, params.kd_use_backoff)
    else
      new MultiRegularCellGrid(degrees_per_cell,
        params.width_of_multi_cell, table)
  }

  protected def initialize_word_dist_factory() = {
    if (params.word_dist == "pseudo-good-turing-bigram")
     new PGTSmoothedBigramWordDistFactory
    else //(params.word_dist == "pseudo-good-turing-unigram")
      new PseudoGoodTuringSmoothedWordDistFactory 
  }

  protected def read_stopwords() = {
    Stopwords.read_stopwords(get_file_handler, params.stopwords_file)
  }

  protected def read_documents(table: DistDocumentTable, stopwords: Set[String]) {
    for (fn <- Params.document_file)
      table.read_document_data(get_file_handler, fn, cell_grid)

    // Read in the words-counts file
    if (Params.counts_file.length > 0) {
      for (fn <- Params.counts_file)
        word_dist_factory.read_word_counts(table, get_file_handler, fn, stopwords)
      table.finish_word_counts()
    }
  }

  def setup_for_run() {
    word_dist_factory = initialize_word_dist_factory()
    document_table = initialize_document_table(word_dist_factory)
    cell_grid = initialize_cell_grid(document_table)
    stopwords = read_stopwords()
    read_documents(document_table, stopwords)
    cell_grid.finish()
  }

  protected def process_strategies[T](strategies: Seq[(String, T)])(
      geneval: (String, T) => EvaluationOutputter) = {
    for ((stratname, strategy) <- strategies) yield {
      val evalobj = geneval(stratname, strategy)
      // For --eval-format=internal, there is no eval file.  To make the
      // evaluation loop work properly, we pretend like there's a single
      // eval file whose value is null.
      val iterfiles =
        if (Params.eval_file.length > 0) Params.eval_file
        else Seq[String](null)
      evalobj.evaluate_and_output_results(get_file_handler, iterfiles)
      (stratname, strategy, evalobj)
    }
  }
}

object GeolocateDriver {
  var Params: GeolocateParameters = _
  val Debug: DebugSettings = new DebugSettings

  // Debug flags (from InternalGeolocateDocumentEvaluator) -- need to set them
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
      //      choices=Seq(
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
// so that the ParamType can be overridden in HadoopGeolocateDocumentDriver.
abstract class GeolocateDocumentTypeDriver extends GeolocateDriver {
  override type ParamType <: GeolocateDocumentParameters
  type RunReturnType =
    Seq[(String, GeolocateDocumentStrategy, EvaluationOutputter)]

  var strategies: Seq[(String, GeolocateDocumentStrategy)] = _

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

    if (params.counts_file.length == 0)
      param_error("Must specify counts file")

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
                new RandomGeolocateDocumentStrategy(cell_grid)
              case "internal-link" =>
                new MostPopularCellGeolocateDocumentStrategy(cell_grid, true)
              case "num-documents" =>
                new MostPopularCellGeolocateDocumentStrategy(cell_grid, false)
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
              new NaiveBayesDocumentStrategy(cell_grid,
                use_baseline = (stratname == "naive-bayes-with-baseline"))
            else stratname match {
              case "average-cell-probability" =>
                new AverageCellProbabilityStrategy(cell_grid)
              case "cosine-similarity" =>
                new CosineSimilarityStrategy(cell_grid, smoothed = false,
                  partial = false)
              case "partial-cosine-similarity" =>
                new CosineSimilarityStrategy(cell_grid, smoothed = false,
                  partial = true)
              case "smoothed-cosine-similarity" =>
                new CosineSimilarityStrategy(cell_grid, smoothed = true,
                  partial = false)
              case "smoothed-partial-cosine-similarity" =>
                new CosineSimilarityStrategy(cell_grid, smoothed = true,
                  partial = true)
              case "full-kl-divergence" =>
                new KLDivergenceStrategy(cell_grid, symmetric = false,
                  partial = false)
              case "partial-kl-divergence" =>
                new KLDivergenceStrategy(cell_grid, symmetric = false,
                  partial = true)
              case "symmetric-full-kl-divergence" =>
                new KLDivergenceStrategy(cell_grid, symmetric = true,
                  partial = false)
              case "symmetric-partial-kl-divergence" =>
                new KLDivergenceStrategy(cell_grid, symmetric = true,
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
   * Do the actual document geolocation.  Results to stderr (see above), and
   * also returned.
   *
   * The current return type is as follows:
   *
   * Seq[(java.lang.String, GeolocateDocumentStrategy, scala.collection.mutable.Map[evalobj.Document,opennlp.textgrounder.geolocate.EvaluationResult])] where val evalobj: opennlp.textgrounder.geolocate.TestFileEvaluator
   *
   * This means you get a sequence of tuples of
   * (strategyname, strategy, results)
   * where:
   * strategyname = name of strategy as given on command line
   * strategy = strategy object
   * results = map listing results for each document (an abstract type
   * defined in TestFileEvaluator; the result type EvaluationResult
   * is practically an abstract type, too -- the most useful dynamic
   * type in practice is DocumentEvaluationResult)
   */

  def run_after_setup() = {
    process_strategies(strategies)((stratname, strategy) => {
      val evaluator =
        // Generate reader object
        if (params.eval_format == "pcl-travel")
          new PCLTravelGeolocateDocumentEvaluator(strategy, stratname, this)
        else
          new InternalGeolocateDocumentEvaluator(strategy, stratname, this)
      new DefaultEvaluationOutputter(stratname, evaluator)
    })
  }
}

/**
 * Implementation of driver-statistics mix-in that simply stores the
 * counters locally.
 */
trait StandaloneGeolocateDriverStats extends ExperimentDriverStats {
  val counter_values = longmap[String]()

  val local_counter_group = "textgrounder"

  def get_task_id = 0

  protected def do_increment_counter(name: String, incr: Long) {
    counter_values(name) += incr
  }

  protected def do_get_counter(name: String) = counter_values(name)
}

class GeolocateDocumentDriver extends
    GeolocateDocumentTypeDriver with StandaloneGeolocateDriverStats {
  override type ParamType = GeolocateDocumentParameters
}

abstract class GeolocateApp(appname: String) extends
    ExperimentDriverApp(appname) {
  type DriverType <: GeolocateDriver
}

object GeolocateDocumentApp extends GeolocateApp("geolocate-document") {
  type DriverType = GeolocateDocumentDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType()
}

