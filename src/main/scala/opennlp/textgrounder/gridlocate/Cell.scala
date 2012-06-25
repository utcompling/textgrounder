///////////////////////////////////////////////////////////////////////////////
//  Cell.scala
//
//  Copyright (C) 2010, 2011, 2012 Ben Wing, The University of Texas at Austin
//  Copyright (C) 2011, 2012 Stephen Roller, The University of Texas at Austin
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

import collection.mutable

import opennlp.textgrounder.util.collectionutil.{LRUCache, doublemap}
import opennlp.textgrounder.util.printutil.{errprint, warning}
import opennlp.textgrounder.util._
import opennlp.textgrounder.util.experiment._

import opennlp.textgrounder.worddist.{WordDist,WordDistFactory,UnigramWordDist}
import opennlp.textgrounder.worddist.WordDist.memoizer._
/* FIXME: Eliminate this. */
import GridLocateDriver.Params

/////////////////////////////////////////////////////////////////////////////
//                             Word distributions                          //
/////////////////////////////////////////////////////////////////////////////

/**
 * Distribution over words resulting from combining the individual
 * distributions of a number of documents.  We track the number of
 * documents making up the distribution, as well as the total incoming link
 * count for all of these documents.  Note that some documents contribute
 * to the link count but not the word distribution; hence, there are two
 * concepts of "empty", depending on whether all contributing documents or
 * only those that contributed to the word distribution are counted.
 * (The primary reason for documents not contributing to the distribution
 * is that they're not in the training set; see comments below.  However,
 * some documents simply don't have distributions defined for them in the
 * document file -- e.g. if there was a problem extracting the document's
 * words in the preprocessing stage.)
 *
 * Note that we embed the actual object describing the word distribution
 * as a field in this object, rather than extending (subclassing) WordDist.
 * The reason for this is that there are multiple types of WordDists, and
 * so subclassing would require creating a different subclass for every
 * such type, along with extra boilerplate functions to create objects of
 * these subclasses.
 */
class CombinedWordDist(factory: WordDistFactory) {
  /** The combined word distribution itself. */
  val word_dist = factory.create_word_dist()
  /** Number of documents included in incoming-link computation. */
  var num_docs_for_links = 0
  /** Total number of incoming links. */
  var incoming_links = 0
  /** Number of documents included in word distribution.  All such
   * documents also contribute to the incoming link count. */
  var num_docs_for_word_dist = 0

  /** True if no documents have contributed to the word distribution.
   * This should generally be the same as if the distribution is empty
   * (unless documents with an empty distribution were added??). */
  def is_empty_for_word_dist() = num_docs_for_word_dist == 0

  /** True if the object is completely empty.  This means no documents
   * at all have been added using `add_document`. */
  def is_empty() = num_docs_for_links == 0

  /**
   *  Add the given document to the total distribution seen so far.
   *  `partial` is a scaling factor (between 0.0 and 1.0) used for
   *  interpolating multiple distributions.
   */
  def add_document(doc: DistDocument[_], partial: Double = 1.0) {
    /* Formerly, we arranged things so that we were passed in all documents,
       regardless of the split.  The reason for this was that the decision
       was made to accumulate link counts from all documents, even in the
       evaluation set.
       
       Strictly, this is a violation of the "don't train on your evaluation
       set" rule.  The reason motivating this was that

       (1) The links are used only in Naive Bayes, and only in establishing
       a prior probability.  Hence they aren't the main indicator.
       (2) Often, nearly all the link count for a given cell comes from
       a particular document -- e.g. the Wikipedia article for the primary
       city in the cell.  If we pull the link count for this document
       out of the cell because it happens to be in the evaluation set,
       we will totally distort the link count for this cell.  In a "real"
       usage case, we would be testing against an unknown document, not
       against a document in our training set that we've artificially
       removed so as to construct an evaluation set, and this problem
       wouldn't arise, so by doing this we are doing a more realistic
       evaluation.
       
       Note that we do NOT include word counts from dev-set or test-set
       documents in the word distribution for a cell.  This keeps to the
       above rule about only training on your training set, and is OK
       because (1) each document in a cell contributes a similar amount of
       word counts (assuming the documents are somewhat similar in size),
       hence in a cell with multiple documents, each individual document
       only computes a fairly small fraction of the total word counts;
       (2) distributions are normalized in any case, so the exact number
       of documents in a cell does not affect the distribution.
       
       However, once the corpora were separated into sub-corpora based on
       the training/dev/test split, passing in all documents complicated
       things, as it meant having to read all the sub-corpora.  Furthermore,
       passing in non-training documents into the K-d cell grid changes the
       grids in ways that are not easily predictable -- a significantly
       greater effect than simply changing the link counts.  So (for the
       moment at least) we don't do this any more. */
    assert (doc.split == "training")

    /* Add link count of document to cell. */
    doc.incoming_links match {
      // Might be None, for unknown link count
      case Some(x) => incoming_links += x
      case _ =>
    }
    num_docs_for_links += 1

    if (doc.dist == null) {
      if (Params.max_time_per_stage == 0.0 && Params.num_training_docs == 0)
        warning("Saw document %s without distribution", doc)
    } else {
      word_dist.add_word_distribution(doc.dist, partial)
      num_docs_for_word_dist += 1
    }
  }
}


/////////////////////////////////////////////////////////////////////////////
//                             Cell distributions                          //
/////////////////////////////////////////////////////////////////////////////

/**
 * A general distribution over cells, associating a probability with each
 * cell.  The caller needs to provide the probabilities.
 */

class CellDist[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc]
](
  val cell_grid: CellGrid[TCoord, TDoc, TCell]
) {
  val cellprobs: mutable.Map[TCell, Double] =
    mutable.Map[TCell, Double]()

  def set_cell_probabilities(
      probs: collection.Map[TCell, Double]) {
    cellprobs.clear()
    cellprobs ++= probs
  }

  def get_ranked_cells() = {
    // sort by second element of tuple, in reverse order
    cellprobs.toSeq sortWith (_._2 > _._2)
  }
}

/**
 * Distribution over cells that is associated with a word. This class knows
 * how to populate its own probabilities, based on the relative probabilities
 * of the word in the word distributions of the various cells.  That is,
 * if we have a set of cells, each with a word distribution, then we can
 * imagine conceptually inverting the process to generate a cell distribution
 * over words.  Basically, for a given word, look to see what its probability
 * is in all cells; normalize, and we have a cell distribution.
 *
 * Instances of this class are normally generated by a factory, specifically
 * `CellDistFactory` or a subclass.  Currently only used by `SphereWordCellDist`
 * and `SphereCellDistFactory`; see them for info on how they are used.
 *
 * @param word Word for which the cell is computed
 * @param cellprobs Hash table listing probabilities associated with cells
 */

class WordCellDist[TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc]
](
  cell_grid: CellGrid[TCoord, TDoc, TCell],
  val word: Word
) extends CellDist[TCoord, TDoc, TCell](cell_grid) {
  var normalized = false

  protected def init() {
    // It's expensive to compute the value for a given word so we cache word
    // distributions.
    var totalprob = 0.0
    // Compute and store un-normalized probabilities for all cells
    for (cell <- cell_grid.iter_nonempty_cells(nonempty_word_dist = true)) {
      val prob = cell.combined_dist.word_dist.lookup_word(word)
      // Another way of handling zero probabilities.
      /// Zero probabilities are just a bad idea.  They lead to all sorts of
      /// pathologies when trying to do things like "normalize".
      //if (prob == 0.0)
      //  prob = 1e-50
      cellprobs(cell) = prob
      totalprob += prob
    }
    // Normalize the probabilities; but if all probabilities are 0, then
    // we can't normalize, so leave as-is. (FIXME When can this happen?
    // It does happen when you use --mode=generate-kml and specify words
    // that aren't seen.  In other circumstances, the smoothing ought to
    // ensure that 0 probabilities don't exist?  Anything else I missed?)
    if (totalprob != 0) {
      normalized = true
      for ((cell, prob) <- cellprobs)
        cellprobs(cell) /= totalprob
    } else
      normalized = false
  }

  init()
}

/**
 * Factory object for creating CellDists, i.e. objects describing a
 * distribution over cells.  You can create two types of CellDists, one for
 * a single word and one based on a distribution of words.  The former
 * process returns a WordCellDist, which initializes the probability
 * distribution over cells as described for that class.  The latter process
 * returns a basic CellDist.  It works by retrieving WordCellDists for
 * each of the words in the distribution, and then averaging all of these
 * distributions, weighted according to probability of the word in the word
 * distribution.
 *
 * The call to `get_cell_dist` on this class either locates a cached
 * distribution or creates a new one, using `create_word_cell_dist`,
 * which creates the actual `WordCellDist` class.
 *
 * @param lru_cache_size Size of the cache used to avoid creating a new
 *   WordCellDist for a given word when one is already available for that
 *   word.
 */

abstract class CellDistFactory[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc]
](
  val lru_cache_size: Int
) {
  type TCellDist <: WordCellDist[TCoord, TDoc, TCell]
  type TGrid <: CellGrid[TCoord, TDoc, TCell]
  def create_word_cell_dist(cell_grid: TGrid, word: Word): TCellDist

  var cached_dists: LRUCache[Word, TCellDist] = null

  /**
   * Return a cell distribution over a single word, using a least-recently-used
   * cache to optimize access.
   */
  def get_cell_dist(cell_grid: TGrid, word: Word) = {
    if (cached_dists == null)
      cached_dists = new LRUCache(maxsize = lru_cache_size)
    cached_dists.get(word) match {
      case Some(dist) => dist
      case None => {
        val dist = create_word_cell_dist(cell_grid, word)
        cached_dists(word) = dist
        dist
      }
    }
  }

  /**
   * Return a cell distribution over a distribution over words.  This works
   * by adding up the distributions of the individual words, weighting by
   * the count of the each word.
   */
  def get_cell_dist_for_word_dist(cell_grid: TGrid, xword_dist: WordDist) = {
    // FIXME!!! Figure out what to do if distribution is not a unigram dist.
    // Can we break this up into smaller operations?  Or do we have to
    // make it an interface for WordDist?
    val word_dist = xword_dist.asInstanceOf[UnigramWordDist]
    val cellprobs = doublemap[TCell]()
    for ((word, count) <- word_dist.counts) {
      val dist = get_cell_dist(cell_grid, word)
      for ((cell, prob) <- dist.cellprobs)
        cellprobs(cell) += count * prob
    }
    val totalprob = (cellprobs.values sum)
    for ((cell, prob) <- cellprobs)
      cellprobs(cell) /= totalprob
    val retval = new CellDist[TCoord, TDoc, TCell](cell_grid)
    retval.set_cell_probabilities(cellprobs)
    retval
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Cells in a grid                             //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for a general cell in a cell grid.
 * 
 * @param cell_grid The CellGrid object for the grid this cell is in.
 * @tparam TCoord The type of the coordinate object used to specify a
 *   a point somewhere in the grid.
 * @tparam TDoc The type of documents stored in a cell in the grid.
 */
abstract class GeoCell[TCoord, TDoc <: DistDocument[TCoord]](
    val cell_grid: CellGrid[TCoord, TDoc,
      _ <: GeoCell[TCoord, TDoc]]
) {
  val combined_dist =
    new CombinedWordDist(cell_grid.table.word_dist_factory)
  var most_popular_document: TDoc = _
  var mostpopdoc_links = 0

  /**
   * Return a string describing the location of the cell in its grid,
   * e.g. by its boundaries or similar.
   */
  def describe_location(): String

  /**
   * Return a string describing the indices of the cell in its grid.
   * Only used for debugging.
   */
  def describe_indices(): String

  /**
   * Return the coordinate of the "center" of the cell.  This is the
   * coordinate used in computing distances between arbitary points and
   * given cells, for evaluation and such.  For odd-shaped cells, the
   * center can be more or less arbitrarily placed as long as it's somewhere
   * central.
   */
  def get_center_coord(): TCoord

  /**
   * Return true if we have finished creating and populating the cell.
   */
  def finished = combined_dist.word_dist.finished
  /**
   * Return a string representation of the cell.  Generally does not need
   * to be overridden.
   */
  override def toString = {
    val unfinished = if (finished) "" else ", unfinished"
    val contains =
      if (most_popular_document != null)
        ", most-pop-doc %s(%d links)" format (
          most_popular_document, mostpopdoc_links)
      else ""

    "GeoCell(%s%s%s, %d documents(dist), %d documents(links), %s types, %s tokens, %d links)" format (
      describe_location(), unfinished, contains,
      combined_dist.num_docs_for_word_dist,
      combined_dist.num_docs_for_links,
      combined_dist.word_dist.num_word_types,
      combined_dist.word_dist.num_word_tokens,
      combined_dist.incoming_links)
  }

  // def __repr__() = {
  //   toString.encode("utf-8")
  // }

  /**
   * Return a shorter string representation of the cell, for
   * logging purposes.
   */
  def shortstr = {
    var str = "Cell %s" format describe_location()
    val mostpop = most_popular_document
    if (mostpop != null)
      str += ", most-popular %s" format mostpop.shortstr
    str
  }

  /**
   * Return an XML representation of the cell.  Currently used only for
   * debugging-output purposes, so the exact representation isn't too important.
   */
  def struct() =
    <GeoCell>
      <bounds>{ describe_location() }</bounds>
      <finished>{ finished }</finished>
      {
        if (most_popular_document != null)
          (<mostPopularDocument>most_popular_document.struct()</mostPopularDocument>
           <mostPopularDocumentLinks>mostpopdoc_links</mostPopularDocumentLinks>)
      }
      <numDocumentsDist>{ combined_dist.num_docs_for_word_dist }</numDocumentsDist>
      <numDocumentsLink>{ combined_dist.num_docs_for_links }</numDocumentsLink>
      <incomingLinks>{ combined_dist.incoming_links }</incomingLinks>
    </GeoCell>

  /**
   * Add a document to the distribution for the cell.
   */
  def add_document(doc: TDoc) {
    assert(!finished)
    combined_dist.add_document(doc)
    if (doc.incoming_links != None &&
      doc.incoming_links.get > mostpopdoc_links) {
      mostpopdoc_links = doc.incoming_links.get
      most_popular_document = doc
    }
  }

  /**
   * Finish any computations related to the cell's word distribution.
   */
  def finish() {
    assert(!finished)
    combined_dist.word_dist.finish_before_global()
    combined_dist.word_dist.finish_after_global()
  }
}

/**
 * A mix-in trait for GeoCells that create their distribution by remembering
 * all the documents that go into the distribution, and then generating
 * the distribution from them at the end.
 *
 * NOTE: This is *not* the ideal way of doing things!  It can cause
 * out-of-memory errors for large corpora.  It is better to create the
 * distributions on the fly.  Note that for K-d cells this may require
 * two passes over the input corpus: One to note the documents that go into
 * the cells and create the cells appropriately, and another to add the
 * document distributions to those cells.  If so, we should add a function
 * to cell grids indicating whether they want the documents given to them
 * in two passes, and modify the code in DistDocumentTable (DistDocument.scala)
 * so that it does two passes over the documents if so requested.
 */
trait DocumentRememberingCell[TCoord, TDoc <: DistDocument[TCoord]] {
  this: GeoCell[TCoord, TDoc] =>

  /**
   * Return an Iterable over documents, listing the documents in the cell.
   */
  def iterate_documents(): Iterable[TDoc]

  /**
   * Generate the distribution for the cell from the documents in it.
   */
  def generate_dist() {
    assert(!finished)
    for (doc <- iterate_documents())
      add_document(doc)
    finish()
  }
}

/**
 * Abstract class for a general grid of cells.  The grid is defined over
 * a continuous space (e.g. the surface of the Earth).  The space is indexed
 * by coordinates (of type TCoord).  Each cell (of type TCell) covers
 * some portion of the space.  There is also a set of documents (of type
 * TDoc), each of which is indexed by a coordinate and which has a
 * distribution describing the contents of the document.  The distributions
 * of all the documents in a cell (i.e. whose coordinate is within the cell)
 * are amalgamated to form the distribution of the cell.
 *
 * One example is the SphereCellGrid -- a grid of cells covering the Earth.
 * ("Sphere" is used here in its mathematical meaning of the surface of a
 * round ball.) Coordinates, of type SphereCoord, are pairs of latitude and
 * longitude.  Documents are of type SphereDocument and have a SphereCoord
 * as their coordinate.  Cells are of type SphereCell.  Subclasses of
 * SphereCellGrid refer to particular grid cell shapes.  For example, the
 * MultiRegularCellGrid consists of a regular tiling of the surface of the
 * Earth into "rectangles" defined by minimum and maximum latitudes and
 * longitudes.  Most commonly, each tile is a cell, but it is possible for
 * a cell to consist of an NxN square of tiles, in which case the cells
 * overlap.  Another subclass is KDTreeCellGrid, with rectangular cells of
 * variable size so that the number of documents in a given cell stays more
 * or less constant.
 *
 * Another possibility would be a grid indexed by years, where each cell
 * corresponds to a particular range of years.
 *
 * In general, no assumptions are made about the shapes of cells in the grid,
 * the number of dimensions in the grid, or whether the cells are overlapping.
 *
 * The following operations are used to populate a cell grid:
 *
 * (1) Documents are added one-by-one to a grid by calling
 *     `add_document_to_cell`.
 * (2) After all documents have been added, `initialize_cells` is called
 *     to generate the cells and create their distribution.
 * (3) After this, it should be possible to list the cells by calling
 *     `iter_nonempty_cells`.
 */
abstract class CellGrid[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc]
](
    val table: DistDocumentTable[TCoord, TDoc, _ <: CellGrid[TCoord, TDoc, TCell]]
) {

  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int

  /*
   * Number of times to pass over the training corpus
   * and call add_document()
   */
  val num_training_passes: Int = 1

  /*
   * Called before each new pass of the training. Usually not
   * needed, but needed for KDCellGrid and possibly future CellGrids.
   */
  def begin_training_pass(pass: Int) = {}

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null if `create` is false.  Else, create an empty
   * cell to hold the coordinates -- but do *NOT* record the cell or
   * otherwise alter the existing cell configuration.  This situation
   * where such a cell is needed is during evaluation.  The cell is
   * needed purely for comparing it against existing cells and determining
   * its center.  The reason for not recording such cells is to make
   * sure that future evaluation results aren't affected.
   */
  def find_best_cell_for_coord(coord: TCoord, create_non_recorded: Boolean):
    TCell

  /**
   * Add the given document to the cell grid.
   */
  def add_document_to_cell(document: TDoc): Unit

  /**
   * Generate all non-empty cells.  This will be called once (and only once),
   * after all documents have been added to the cell grid by calling
   * `add_document_to_cell`.  The generation happens internally; but after
   * this, `iter_nonempty_cells` should work properly.  This is not meant
   * to be called externally.
   */
  protected def initialize_cells(): Unit

  /**
   * Iterate over all non-empty cells.
   * 
   * @param nonempty_word_dist If given, returned cells must also have a
   *   non-empty word distribution; otherwise, they just need to have at least
   *   one document in them. (Not all documents have word distributions, esp.
   *   when --max-time-per-stage has been set to a non-zero value so that we
   *   only load some subset of the word distributions for all documents.  But
   *   even when not set, some documents may be listed in the document-data file
   *   but have no corresponding word counts given in the counts file.)
   */
  def iter_nonempty_cells(nonempty_word_dist: Boolean = false):
    Iterable[TCell]
  
  /*********************** Not meant to be overridden *********************/
  
  /* These are simply the sum of the corresponding counts
     `num_docs_for_word_dist` and `num_docs_for_links` of each individual
     cell. */
  var total_num_docs_for_word_dist = 0
  var total_num_docs_for_links = 0
  /* Set once finish() is called. */
  var all_cells_computed = false
  /* Number of non-empty cells. */
  var num_non_empty_cells = 0

  /**
   * This function is called externally to initialize the cells.  It is a
   * wrapper around `initialize_cells()`, which is not meant to be called
   * externally.  Normally this does not need to be overridden.
   */
  def finish() {
    assert(!all_cells_computed)

    initialize_cells()

    all_cells_computed = true

    total_num_docs_for_links = 0
    total_num_docs_for_word_dist = 0

    { // Put in a block to control scope of 'task'
      val task = new ExperimentMeteredTask(table.driver, "non-empty cell",
        "computing statistics of")
      for (cell <- iter_nonempty_cells()) {
        total_num_docs_for_word_dist +=
          cell.combined_dist.num_docs_for_word_dist
        total_num_docs_for_links +=
          cell.combined_dist.num_docs_for_links
        task.item_processed()
      }
      task.finish()
    }

    errprint("Number of non-empty cells: %s", num_non_empty_cells)
    errprint("Total number of cells: %s", total_num_cells)
    errprint("Percent non-empty cells: %g",
      num_non_empty_cells.toDouble / total_num_cells)
    val recorded_training_docs_with_coordinates =
      table.num_recorded_documents_with_coordinates_by_split("training").value
    errprint("Training documents per non-empty cell: %g",
      recorded_training_docs_with_coordinates.toDouble / num_non_empty_cells)
    // Clear out the document distributions of the training set, since
    // only needed when computing cells.
    //
    // FIXME: Could perhaps save more memory, or at least total memory used,
    // by never creating these distributions at all, but directly adding
    // them to the cells.  Would require a bit of thinking when reading
    // in the counts.
    table.driver.heartbeat
    table.clear_training_document_distributions()
    table.driver.heartbeat
  }
}
