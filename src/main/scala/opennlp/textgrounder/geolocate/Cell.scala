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
//////// Cell.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import collection.mutable

import opennlp.textgrounder.util.collectionutil.{LRUCache, doublemap}
import opennlp.textgrounder.util.printutil.{errprint, warning}

import WordDist.memoizer._
import GeolocateDriver.Params

/////////////////////////////////////////////////////////////////////////////
//                             Word distributions                          //
/////////////////////////////////////////////////////////////////////////////

/**
 * Distribution over words corresponding to a cell.
 */

class CellWordDist(val word_dist: WordDist) {
  /** Number of documents included in incoming-link computation. */
  var num_docs_for_links = 0
  /** Total number of incoming links. */
  var incoming_links = 0
  /** Number of documents included in word distribution. */
  var num_docs_for_word_dist = 0

  def is_empty_for_word_dist() = num_docs_for_word_dist == 0

  def is_empty() = num_docs_for_links == 0

  /**
   *  Add the given document to the total distribution seen so far
   */
  def add_document(doc: DistDocument[_]) {
    /* We are passed in all documents, regardless of the split.
       The decision was made to accumulate link counts from all documents,
       even in the evaluation set.  Strictly, this is a violation of the
       "don't train on your evaluation set" rule.  The reason we do this
       is that

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
       of documents in a cell does not affect the distribution. */
    /* Add link count of document to cell. */
    doc.incoming_links match {
      // Might be None, for unknown link count
      case Some(x) => incoming_links += x
      case _ =>
    }
    num_docs_for_links += 1

    /* Add word counts of document to cell, but only if in the
       training set. */
    if (doc.split == "training") {
      if (doc.dist == null) {
        if (Params.max_time_per_stage == 0.0 && Params.num_training_docs == 0)
          warning("Saw document %s without distribution", doc)
      } else {
        assert(doc.dist.finished)
        word_dist.add_word_distribution(doc.dist)
        num_docs_for_word_dist += 1
      }
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Cell distributions                          //
/////////////////////////////////////////////////////////////////////////////

/** A simple distribution associating a probability with each cell. */

class CellDist[CoordType, DocumentType <: DistDocument[CoordType],
  CellType <: GeoCell[CoordType, DocumentType]](
  val cell_grid: CellGrid[CoordType, DocumentType, CellType]
) {
  val cellprobs: mutable.Map[CellType, Double] =
    mutable.Map[CellType, Double]()

  def set_cell_probabilities(
      probs: collection.Map[CellType, Double]) {
    cellprobs.clear()
    cellprobs ++= probs
  }

  def get_ranked_cells() = {
    // sort by second element of tuple, in reverse order
    cellprobs.toSeq sortWith (_._2 > _._2)
  }
}

/**
 * Distribution over cells, as might be attached to a word.  If we have a
 *  set of cells, each with a word distribution, then we can imagine
 *  conceptually inverting the process to generate a cell distribution over
 *  words.  Basically, for a given word, look to see what its probability is
 *  in all cells; normalize, and we have a cell distribution.
 *
 *  @param word Word for which the cell is computed
 *  @param cellprobs Hash table listing probabilities associated with cells
 */

class WordCellDist[CoordType, DocumentType <: DistDocument[CoordType],
  CellType <: GeoCell[CoordType, DocumentType]](
  cell_grid: CellGrid[CoordType, DocumentType, CellType],
  val word: Word
) extends CellDist[CoordType, DocumentType, CellType](cell_grid) {
  var normalized = false

  protected def init() {
    // It's expensive to compute the value for a given word so we cache word
    // distributions.
    var totalprob = 0.0
    // Compute and store un-normalized probabilities for all cells
    for (cell <- cell_grid.iter_nonempty_cells(nonempty_word_dist = true)) {
      val prob = cell.word_dist.lookup_word(word)
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

abstract class CellDistFactory[
  CoordType, DocumentType <: DistDocument[CoordType],
  CellType <: GeoCell[CoordType, DocumentType]](
  val lru_cache_size: Int
) {
  type WordCellDistType <: WordCellDist[CoordType, DocumentType, CellType]
  type GridType <: CellGrid[CoordType, DocumentType, CellType]
  def create_word_cell_dist(cell_grid: GridType, word: Word): WordCellDistType

  var cached_dists: LRUCache[Word, WordCellDistType] = null

  // Return a cell distribution over a given word, using a least-recently-used
  // cache to optimize access.
  def get_cell_dist(cell_grid: GridType, word: Word) = {
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
  def get_cell_dist_for_word_dist(cell_grid: GridType, xword_dist: WordDist) = {
    // FIXME!!! Figure out what to do if distribution is not a unigram dist.
    // Can we break this up into smaller operations?  Or do we have to
    // make it an interface for WordDist?
    val word_dist = xword_dist.asInstanceOf[UnigramWordDist]
    val cellprobs = doublemap[CellType]()
    for ((word, count) <- word_dist.counts) {
      val dist = get_cell_dist(cell_grid, word)
      for ((cell, prob) <- dist.cellprobs)
        cellprobs(cell) += count * prob
    }
    val totalprob = (cellprobs.values sum)
    for ((cell, prob) <- cellprobs)
      cellprobs(cell) /= totalprob
    val retval = new CellDist[CoordType, DocumentType, CellType](cell_grid)
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
 * @tparam CoordType The type of the coordinate object used to specify a
 *   a point somewhere in the grid.
 * @tparam DocumentType The type of documents stored in a cell in the grid.
 */
abstract class GeoCell[CoordType, DocumentType <: DistDocument[CoordType]](
    val cell_grid: CellGrid[CoordType, DocumentType,
      _ <: GeoCell[CoordType, DocumentType]]
) {
  val word_dist_wrapper =
    new CellWordDist(cell_grid.table.word_dist_factory.create_word_dist())
  var most_popular_document: DocumentType = _
  var mostpopdoc_links = 0

  def word_dist = word_dist_wrapper.word_dist

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
   * Return an Iterable over documents, listing the documents in the cell.
   */
  def iterate_documents(): Iterable[DocumentType]

  /**
   * Return the coordinate of the "center" of the cell.  This is the
   * coordinate used in computing distances between arbitary points and
   * given cells, for evaluation and such.  For odd-shaped cells, the
   * center can be more or less arbitrarily placed as long as it's somewhere
   * central.
   */
  def get_center_coord(): CoordType

  /**
   * Return a string representation of the cell.  Generally does not need
   * to be overridden.
   */
  override def toString = {
    val unfinished = if (word_dist.finished) "" else ", unfinished"
    val contains =
      if (most_popular_document != null)
        ", most-pop-doc %s(%d links)" format (
          most_popular_document, mostpopdoc_links)
      else ""

    "GeoCell(%s%s%s, %d documents(dist), %d documents(links), %d links)" format (
      describe_location(), unfinished, contains,
      word_dist_wrapper.num_docs_for_word_dist,
      word_dist_wrapper.num_docs_for_links,
      word_dist_wrapper.incoming_links)
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
      <finished>{ word_dist.finished }</finished>
      {
        if (most_popular_document != null)
          (<mostPopularDocument>most_popular_document.struct()</mostPopularDocument>
           <mostPopularDocumentLinks>mostpopdoc_links</mostPopularDocumentLinks>)
      }
      <numDocumentsDist>{ word_dist_wrapper.num_docs_for_word_dist }</numDocumentsDist>
      <numDocumentsLink>{ word_dist_wrapper.num_docs_for_links }</numDocumentsLink>
      <incomingLinks>{ word_dist_wrapper.incoming_links }</incomingLinks>
    </GeoCell>

  /**
   * Generate the distribution for a cell from the documents in it.
   */
  def generate_dist() {
    for (doc <- iterate_documents()) {
      word_dist_wrapper.add_document(doc)
      if (doc.incoming_links != None &&
        doc.incoming_links.get > mostpopdoc_links) {
        mostpopdoc_links = doc.incoming_links.get
        most_popular_document = doc
      }
    }
    word_dist.finish(minimum_word_count = Params.minimum_word_count)
  }
}

/**
 * Abstract class for a general grid of cells.  No assumptions are
 * made about the shapes of cells in the grid, the number of dimensions in
 * the grid, or whether the cells are overlapping.
 */
abstract class CellGrid[CoordType, DocumentType <: DistDocument[CoordType],
    CellType <: GeoCell[CoordType, DocumentType]](
    val table: DistDocumentTable[CoordType, DocumentType]
) {

  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null.
   */
  def find_best_cell_for_coord(coord: CoordType): CellType

  /**
   * Add the given document to the cell grid.
   */
  def add_document_to_cell(document: DocumentType): Unit

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
    Iterable[CellType]
  
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
    for (cell <- iter_nonempty_cells()) {
      total_num_docs_for_word_dist +=
        cell.word_dist_wrapper.num_docs_for_word_dist
      total_num_docs_for_links +=
        cell.word_dist_wrapper.num_docs_for_links
    }

    errprint("Number of non-empty cells: %s", num_non_empty_cells)
    errprint("Total number of cells: %s", total_num_cells)
    errprint("Percent non-empty cells: %g",
      num_non_empty_cells.toDouble / total_num_cells)
    val training_docs_with_word_counts =
      table.num_word_count_documents_by_split("training").value
    errprint("Training documents per non-empty cell: %g",
      training_docs_with_word_counts.toDouble / num_non_empty_cells)
    // Clear out the document distributions of the training set, since
    // only needed when computing cells.
    //
    // FIXME: Could perhaps save more memory, or at least total memory used,
    // by never creating these distributions at all, but directly adding
    // them to the cells.  Would require a bit of thinking when reading
    // in the counts.
    table.clear_training_document_distributions()
  }
}
