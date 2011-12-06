///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 The University of Texas at Austin
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
//////// KDTreeCellGrid.scala
////////
//////// Copyright (c) 2011.
////////

package opennlp.textgrounder.geolocate

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import ags.utils.KdTree

import opennlp.textgrounder.util.distances.SphereCoord

class KdTreeCell(
  cellgrid: KdTreeCellGrid,
  val kdleaf : KdTree[SphereDocument]) extends RectangularCell(cellgrid) {

  def get_northeast_coord () : SphereCoord = {
    new SphereCoord(kdleaf.minLimit(0), kdleaf.minLimit(1))
  }
  
  def get_southwest_coord () : SphereCoord = {
    new SphereCoord(kdleaf.maxLimit(0), kdleaf.maxLimit(1))
  }
  
  def iterate_documents () : Iterable[SphereDocument] = {
    kdleaf.getData()
  }

  override def get_center_coord () = {
    if (cellgrid.table.driver.params.center_method == "center") {
      // center method
      super.get_center_coord
    } else {
      // centroid method
      var sum_lat = 0.0
      var sum_long = 0.0
      for (art <- kdleaf.getData) {
        sum_lat += art.coord.lat
        sum_long += art.coord.long
      }
      SphereCoord(sum_lat / kdleaf.size, sum_long / kdleaf.size)
    }
  }
  
  def describe_indices () : String = {
    "Placeholder"
  }
  
  def describe_location () : String = {
    get_boundary.toString
  }
}

object KdTreeCellGrid {
  def apply(table: SphereDocumentTable, bucketSize: Int, splitMethod: String,
            useBackoff: Boolean) : KdTreeCellGrid = {
    new KdTreeCellGrid(table, bucketSize, splitMethod match {
      case "halfway" => KdTree.SplitMethod.HALFWAY
      case "median" => KdTree.SplitMethod.MEDIAN
      case "maxmargin" => KdTree.SplitMethod.MAX_MARGIN
    }, useBackoff)
  }
}

class KdTreeCellGrid(table: SphereDocumentTable, 
                     bucketSize: Int,
                     splitMethod: KdTree.SplitMethod,
                     useBackoff: Boolean)
    extends SphereCellGrid(table) {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int = 0
  var kdtree : KdTree[SphereDocument] = new KdTree[SphereDocument](2, bucketSize, splitMethod);
  val leaves_to_cell : Map[KdTree[SphereDocument], KdTreeCell] = Map();

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null.
   */
  def find_best_cell_for_coord(coord: SphereCoord): KdTreeCell = {
    leaves_to_cell(kdtree.getLeaf(Array(coord.lat, coord.long)))
  }

  /**
   * Add the given document to the cell grid.
   */
  def add_document_to_cell(document: SphereDocument): Unit = {
    kdtree.addPoint(Array(document.coord.lat, document.coord.long), document)
  }

  /**
   * Generate all non-empty cells.  This will be called once (and only once),
   * after all documents have been added to the cell grid by calling
   * `add_document_to_cell`.  The generation happens internally; but after
   * this, `iter_nonempty_cells` should work properly.
   */
  def initialize_cells: Unit = {
    total_num_cells = kdtree.getLeaves.size
    num_non_empty_cells = total_num_cells

    val nodes = if (useBackoff) kdtree.getNodes else kdtree.getLeaves
    for (leaf <- nodes) {
      val c = new KdTreeCell(this, leaf)
      c.generate_dist
      leaves_to_cell.update(leaf, c)
    }
  }

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
  def iter_nonempty_cells(nonempty_word_dist: Boolean = false): Iterable[SphereCell] = {
    val nodes = if (useBackoff) kdtree.getNodes else kdtree.getLeaves
    for (leaf <- nodes
      if (leaf.size() > 0 || !nonempty_word_dist))
        yield leaves_to_cell(leaf)
  }
}

