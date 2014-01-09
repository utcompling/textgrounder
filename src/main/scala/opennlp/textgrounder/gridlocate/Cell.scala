///////////////////////////////////////////////////////////////////////////////
//  Cell.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package gridlocate

import collection.mutable

import util.error.warning
import util.experiment._
import util.print.errprint
import util.numeric.pretty_double
import util.textdb.{Encoder, Row}

import langmodel.LangModelFactory

/////////////////////////////////////////////////////////////////////////////
//                             Cells in a grid                             //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for a general cell in a cell grid.
 *
 * This combines a number of documents into a combined language model.
 * We track the number of documents making up the language model, as well as
 * the combined salience for all of these documents.
 *
 * @param grid The Grid object for the grid this cell is in.
 * @tparam Co The type of the coordinate object used to specify a
 *   a point somewhere in the grid.
 */
abstract class GridCell[Co](
    val grid: Grid[Co]
) {
  /**************************** Basic properties ******************************/

  /** The combined language model. */
  val lang_model = grid.docfact.lang_model_factory.create_lang_model
  /** Number of documents used to create language model. */
  var num_docs = 0
  /** Combined salience score (computed by adding individual
    * scores of documents). */
  var salience = 0.0
  var most_salient_document: String = ""
  var most_salient_doc_salience = 0.0

  /**
   * True if the object is empty.  This means no documents have been
   * added using `add_document`. */
  def is_empty = num_docs == 0

  /** Normal language model of cell. */
  def grid_lm = lang_model.grid_lm
  /** Language model of cell used during reranking. */
  def rerank_lm = lang_model.rerank_lm

  /**
   * Return true if we have finished creating and populating the cell.
   */
  def finished = lang_model.finished

  /***************************** Central point *****************************/

  /**
   * Return the coordinate of the true center of the cell.  This is sometimes
   * used in computing certain measures.  If this cannot clearly be defined,
   * then a more or less arbitrarily-placed location can be used as long as
   * it's somewhere central.
   */
  def get_true_center: Co

  /**
   * Return the coordinate of the centroid of the cell. If the cell has no
   * documents in it, return the true center.
   */
  def get_centroid: Co

  /**
   * Return the coordinate of the central point of the cell.  This is the
   * coordinate used in computing distances between arbitrary points and
   * given cells, for evaluation and such.  This may be the true center,
   * or some other measure of central tendency (e.g. the centroid).
   */
  def get_central_point = {
    if (grid.driver.params.center_method == "center")
      get_true_center
    else
      get_centroid
  }

  /************************* External representation *************************/

  /**
   * Return a string describing the location of the cell in its grid,
   * e.g. by its boundaries or similar.
   */
  def format_location: String

  /**
   * Return a string describing the indices of the cell in its grid.
   * Only used for debugging.
   */
  def format_indices: String

  /**
   * Return a string representation of the cell.  Generally does not need
   * to be overridden.
   */
  override def toString = {
    val unfinished = if (finished) "" else ", unfinished"
    val contains =
      if (most_salient_document != "")
        ", most-salient-doc %s(%s salience)" format (
          most_salient_document, most_salient_doc_salience)
      else ""

    "GridCell(%s%s%s, %s documents, %s grid types, %s grid tokens, %s rerank types, %s rerank tokens, %s salience)" format (
      format_location, unfinished, contains,
      num_docs,
      grid_lm.num_types,
      grid_lm.num_tokens,
      rerank_lm.num_types,
      rerank_lm.num_tokens,
      salience)
  }

  def to_row = Seq(
    "location" -> format_location,
    "true-center" -> get_true_center,
    "centroid" -> get_centroid,
    "central-point" -> get_central_point,
    "num-documents" -> num_docs,
    "grid-num-word-types" ->
      grid_lm.num_types,
    "grid-num-word-tokens" ->
      grid_lm.num_tokens,
    "rerank-num-word-types" ->
      rerank_lm.num_types,
    "rerank-num-word-tokens" ->
      rerank_lm.num_tokens,
    "salience" -> salience,
    "most-salient-document" ->
      Encoder.string(most_salient_document),
    "most-salient-document-salience" -> most_salient_doc_salience
  )

  /************************* Building up the cell *************************/

  def add_document(doc: GridDoc[Co]) {
    assert(!finished)

    /* Formerly, we arranged things so that we were passed in all documents,
       regardless of the split.  The reason for this was that the decision
       was made to accumulate saliences from all documents, even in the
       evaluation set.

       Strictly, this is a violation of the "don't train on your evaluation
       set" rule.  The reason motivating this was that

       (1) The salience isused only in Naive Bayes, and only in establishing
       a prior probability.  Hence it isn' the main indicator.
       (2) Often, nearly all the salience for a given cell comes from
       a particular document -- e.g. the Wikipedia article for the primary
       city in the cell.  If we pull the salience for this document
       out of the cell because it happens to be in the evaluation set,
       we will totally distort the salience for this cell.  In a "real"
       usage case, we would be testing against an unknown document, not
       against a document in our training set that we've artificially
       removed so as to construct an evaluation set, and this problem
       wouldn't arise, so by doing this we are doing a more realistic
       evaluation.

       Note that we do NOT include word counts from dev-set or test-set
       documents in the language model for a cell.  This keeps to the
       above rule about only training on your training set, and is OK
       because (1) each document in a cell contributes a similar amount of
       word counts (assuming the documents are somewhat similar in size),
       hence in a cell with multiple documents, each individual document
       only computes a fairly small fraction of the total word counts;
       (2) distributions are normalized in any case, so the exact number
       of documents in a cell does not affect the language model.

       However, once the corpora were separated into sub-corpora based on
       the training/dev/test split, passing in all documents complicated
       things, as it meant having to read all the sub-corpora.  Furthermore,
       passing in non-training documents into the K-d cell grid changes the
       grids in ways that are not easily predictable -- a significantly
       greater effect than simply changing the salience.  So (for the
       moment at least) we don't do this any more. */
    assert (doc.split == "training")

    /* Add salience of document to cell. */
    doc.salience.foreach { sal =>
      salience += sal
      add_salient_point(doc.title, sal)
    }

    /* Accumulate language model. `partial` is a scaling factor (between
       0.0 and 1.0) used for interpolating multiple language models.
       Not currently implemented completely. */
    lang_model.add_language_model(doc.lang_model, partial = 1.0)
    num_docs += 1
  }

  /**
   * Include a point (e.g. coordinate) with a given name and salience in
   * a cell, if its salience is greater than any value seen so far.
   * This allows a cell to be identified by e.g. the most populous city
   * in the cell, even if there are no documents corresponding to a city.
   */
  def add_salient_point(name: String, salience: Double) {
    if (salience > most_salient_doc_salience) {
      most_salient_doc_salience = salience
      most_salient_document = name
    }
  }

  /**
   * Finish any computations related to the cell's language model.
   */
  def finish() {
    assert(!finished)
    lang_model.finish_before_global()
    lang_model.finish_after_global()
  }
}

/////////////////////////////////////////////////////////////////////////////
//                               Grid of cells                             //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for a general grid of cells.  The grid is defined over
 * a continuous space (e.g. the surface of the Earth).  The space is indexed
 * by coordinates (of type Co).  Each cell (of type GridCell[Co]) covers
 * some portion of the space.  There is also a set of documents (of type
 * TDoc), each of which is indexed by a coordinate and which has a
 * language model describing the contents of the document.  The language models
 * of all the documents in a cell (i.e. whose coordinate is within the cell)
 * are amalgamated to form the language model of the cell.
 *
 * One example is the SphereGrid -- a grid of cells covering the Earth.
 * ("Sphere" is used here in its mathematical meaning of the surface of a
 * round ball.) Coordinates, of type SphereCoord, are pairs of latitude and
 * longitude.  Documents are of type SphereDoc and have a SphereCoord
 * as their coordinate.  Cells are of type SphereCell.  Subclasses of
 * SphereGrid refer to particular grid cell shapes.  For example, the
 * MultiRegularGrid consists of a regular tiling of the surface of the
 * Earth into "rectangles" defined by minimum and maximum latitudes and
 * longitudes.  Most commonly, each tile is a cell, but it is possible for
 * a cell to consist of an NxN square of tiles, in which case the cells
 * overlap.  Another subclass is KDTreeGrid, with rectangular cells of
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
 * (1) Documents are added by calling `add_training_documents_to_grid`.
 * (2) After all documents have been added, `initialize_cells` is called
 *     to generate the cells and create their language model.
 * (3) After this, it should be possible to list the cells by calling
 *     `iter_nonempty_cells`.
 */
abstract class Grid[Co](
    val docfact: GridDocFactory[Co]
) {
  def driver = docfact.driver

  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int

  /**
   * Find the correct cell for the given coordinate. If no such cell exists,
   * return None if `create_non_recorded` is false.  Else, create an empty
   * cell to hold the coordinates -- but do *NOT* record the cell or otherwise
   * alter the existing cell configuration.  This situation where such a cell
   * is needed is during evaluation.  The cell is needed purely for comparing
   * it against existing cells and determining its center.  The reason for not
   * recording such cells is to make sure that future evaluation results
   * aren't affected.
   *
   * This is used by `find_best_cell_for_document` and `add_salient_point`.
   * If this operation doesn't make sense directly, because other properties
   * of a document are needed to locate a cell in addition to the coordinate,
   * then it can be given a null implementation using `???`, provided that
   * both of the above functions are overridden.
   */
  def find_best_cell_for_coord(coord: Co, create_non_recorded: Boolean):
    Option[GridCell[Co]]

  /**
   * Find the correct cell for the given document, based on the document's
   * coordinates and other properties.  If no such cell exists, return None
   * if `create_non_recorded` is false.  Else, create an empty cell to hold the
   * coordinates -- but do *NOT* record the cell or otherwise alter the
   * existing cell configuration.  This situation where such a cell is needed
   * is during evaluation.  The cell is needed purely for comparing it against
   * existing cells and determining its center.  The reason for not recording
   * such cells is to make sure that future evaluation results aren't affected.
   */
  def find_best_cell_for_document(doc: GridDoc[Co],
      create_non_recorded: Boolean) =
    find_best_cell_for_coord(doc.coord, create_non_recorded)

  /**
   * Add a point (coordinate) with a given name and salience to the
   * appropriate cell of the grid, if its salience is greater than any
   * value seen so far for the cell. This allows a cell to be identified by
   * e.g. the most populous city in the cell, even if there are no documents
   * corresponding to a city.
   */
  def add_salient_point(coord: Co, name: String, salience: Double) {
    find_best_cell_for_coord(coord,
      create_non_recorded = false) map { cell =>
      cell.add_salient_point(name, salience)
    }
  }

  /**
   * Add the given training documents to the cell grid.
   *
   * @param get_rawdocs Function to read raw documents, given a string
   *   indicating the nature of the operation (displayed in status updates
   *   during reading).
   */
  def add_training_documents_to_grid(
      get_rawdocs: String => Iterator[DocStatus[Row]])

  /**
   * Generate all non-empty cells.  This will be called once (and only once),
   * after all documents have been added to the cell grid by calling
   * `add_training_documents_to_grid`.  The generation happens internally;
   * but after this, `iter_nonempty_cells` should work properly.  This is
   * not meant to be called externally.
   */
  protected def initialize_cells()

  /**
   * Iterate over all non-empty cells.
   */
  def iter_nonempty_cells: Iterable[GridCell[Co]]

  /*********************** Not meant to be overridden *********************/

  /* Sum of `num_docs` for each cell. */
  var total_num_docs = 0
  /* Set once finish() is called. */
  var all_cells_computed = false
  /* Number of non-empty cells. */
  var num_non_empty_cells = 0

  /**
   * Iterate over all non-empty cells, making sure to include the given cells
   *  even if empty.
   */
  def iter_nonempty_cells_including(include: Iterable[GridCell[Co]]
  ) = {
    val cells = iter_nonempty_cells
    if (include.size == 0)
      cells
   else {
      val not_included = include.filter(cell => cells.find(_ == cell) == None)
      cells ++ not_included
    }
  }

  /**
   * Iterate over all non-empty cells, making sure to include the given cell
   *  even if empty, if `doinc` is true.
   */
  def iter_nonempty_cells_including(maybeinc: GridCell[Co], doinc: Boolean
  ): Iterable[GridCell[Co]] = {
    val include = if (doinc) Seq(maybeinc) else Seq()
    iter_nonempty_cells_including(include)
  }

  /**
   * Standard implementation of `add_training_documents_to_grid`.
   *
   * @param add_document_to_grid Function to add a document to the grid.
   */
  protected def default_add_training_documents_to_grid(
    get_rawdocs: String => Iterator[DocStatus[Row]],
    add_document_to_grid: GridDoc[Co] => Unit
  ) {
    // FIXME: The "finish_globally" flag needs to be tied into the
    // recording of global statistics in the language models.
    // In reality, all the glop handled by finish_before_global() and
    // note_lang_model_globally() and such should be handled by separate
    // mapping stages onto the documents. See `raw_documents_to_documents`.
    val docstats = docfact.raw_documents_to_document_statuses(
        get_rawdocs("reading"),
        note_globally = true,
        finish_globally = false
      ) map { stat =>
        stat foreach_all {(_, _) => docfact.record_training_document_in_factory(stat)}
        stat
      }

    val docs = docfact.document_statuses_to_documents(docstats)
    docs foreach { doc => add_document_to_grid(doc) }

    // Compute overall backoff statistics.
    if (driver.params.verbose)
      errprint("Finishing global backoff stats...")
    docfact.lang_model_factory.finish_global_backoff_stats()
    docfact.finish_document_loading()
  }

  /**
   * This function is called externally to initialize the cells.  It is a
   * wrapper around `initialize_cells()`, which is not meant to be called
   * externally.  Normally this does not need to be overridden.
   */
  def finish() {
    assert(!all_cells_computed)

    initialize_cells()

    all_cells_computed = true

    total_num_docs = 0

    iter_nonempty_cells foreach { cell =>
      total_num_docs +=
        cell.num_docs
    }

    driver.note_result("number-of-non-empty-cells", num_non_empty_cells,
      "Number of non-empty cells")
    driver.note_result("total-number-of-cells", total_num_cells,
      "Total number of cells")
    val pct_non_empty = num_non_empty_cells.toDouble / total_num_cells * 100
    driver.note_result("percent-non-empty-cells", pct_non_empty,
      "Percent non-empty cells")
    val training_docs_with_coordinates =
      docfact.num_training_documents_with_coordinates_by_split("training").value
    val training_docs_per_non_empty_cell =
      training_docs_with_coordinates.toDouble / num_non_empty_cells
    driver.note_result("training-documents-per-non-empty-cell",
      training_docs_per_non_empty_cell,
      "Training documents per non-emtpy cell")
    if (driver.params.verbose) {
      errprint(
        "%d cells, %d (%.2f%%) non-empty, %s training docs/non-empty cell",
        total_num_cells, num_non_empty_cells, pct_non_empty,
        pretty_double(training_docs_per_non_empty_cell))
    }
    driver.heartbeat
  }
}
