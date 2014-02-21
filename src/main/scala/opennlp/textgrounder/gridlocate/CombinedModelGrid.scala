///////////////////////////////////////////////////////////////////////////////
//  CombinedModelGrid.scala
//
//  Copyright (C) 2012 Stephen Roller, The University of Texas at Austin
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

import util.experiment._
import util.error.warning
import util.print.errprint
import util.textdb.Row

/**
 * A combined grid composed of sub-grids, with the cells of the grid consisting
 * of the union of all cells of all the sub-grids.
 */
abstract class CombinedGrid[Co](
  docfact: GridDocFactory[Co],
  id: String,
  val grids: Iterable[Grid[Co]]
) extends Grid[Co](docfact, id) {
  def short_type = "comb"

  def total_num_cells = grids.map(_.total_num_cells).sum

  def find_best_cell_for_coord(coord: Co, create_non_recorded: Boolean) = {
    val candidates =
      grids.flatMap(_.find_best_cell_for_coord(coord, create_non_recorded))
    if (candidates.size == 0)
      None
    else
      Some(candidates.minBy(
        cell => cell.grid.distance_between_coords(cell.get_central_point, coord)))
  }

  protected def initialize_cells() { }

  def iter_nonempty_cells = grids.flatMap(_.iter_nonempty_cells)
}

/**
 * A combined grid which is created before the sub-grids are initialized
 * with documents, so that the same documents are used to initialize the
 * cells of each sub-grid. The document factory should be the same as
 * one of the sub-grid's document factories -- this is possible because
 * all sub-grids have the same documents in them.
 */
abstract class UninitializedCombinedGrid[Co](
  docfact: GridDocFactory[Co],
  id: String,
  grids: Iterable[Grid[Co]]
) extends CombinedGrid[Co](docfact, id, grids) {

  def add_training_documents_to_grid(
      get_rawdocs: String => Iterator[DocStatus[Row]]) {
    for (grid <- grids) grid.add_training_documents_to_grid(get_rawdocs)
  }

  // We don't want this to do anything because we assume that our document
  // factory is the same as that of one of the sub-grids: See above.
  override protected def finish_document_factory() { }

  override def finish_adding_documents() {
    for (grid <- grids) grid.finish_adding_documents()
    super.finish_adding_documents()
  }

  override def finish() {
    for (grid <- grids) grid.finish()
    super.finish()
  }
}

/**
 * A combined grid which is created after the sub-grids are initialized
 * with documents. The functions to initialize the grid should never be
 * called, so they're stubbed out.
 */
abstract class InitializedCombinedGrid[Co](
  docfact: GridDocFactory[Co],
  id: String,
  grids: Iterable[Grid[Co]]
) extends CombinedGrid[Co](docfact, id, grids) {

  for (grid <- grids) assert(grid.all_cells_computed)

  def add_training_documents_to_grid(
    get_rawdocs: String => Iterator[DocStatus[Row]]
  ) {
    // We need to process the documents again to compute combined global
    // backoff stats for the test documents that we read in. (The alternative
    // of trying to directly combine the backoff stats of the individual
    // sub-grids is messier to implement.)
    docfact.raw_documents_to_documents(
      get_rawdocs("reading %s for combined global backoff stats"),
      note_globally = true,
      finish_globally = false
    ).foreach { doc => () }
  }
}
