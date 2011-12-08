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
import opennlp.textgrounder.util.experiment._

class KdTreeCell(
  cellgrid: KdTreeCellGrid,
  val kdleaf : KdTree[SphereDocument]
) extends RectangularCell(cellgrid) with
    DocumentRememberingCell[SphereCoord, SphereDocument] {

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
            useBackoff: Boolean, interpolateWeight: Double = 0.0) : KdTreeCellGrid = {
    new KdTreeCellGrid(table, bucketSize, splitMethod match {
      case "halfway" => KdTree.SplitMethod.HALFWAY
      case "median" => KdTree.SplitMethod.MEDIAN
      case "maxmargin" => KdTree.SplitMethod.MAX_MARGIN
    }, useBackoff, interpolateWeight)
  }
}

class KdTreeCellGrid(table: SphereDocumentTable, 
                     bucketSize: Int,
                     splitMethod: KdTree.SplitMethod,
                     useBackoff: Boolean,
                     interpolateWeight: Double)
    extends SphereCellGrid(table) {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int = 0
  var kdtree : KdTree[SphereDocument] = new KdTree[SphereDocument](2, bucketSize, splitMethod)
  val leaves_to_cell : Map[KdTree[SphereDocument], KdTreeCell] = Map()

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
  def initialize_cells() {
    total_num_cells = kdtree.getLeaves.size
    num_non_empty_cells = total_num_cells

    val nodes_to_cell : Map[KdTree[SphereDocument], KdTreeCell] = Map()

    for (node <- kdtree.getNodes) {
      val c = new KdTreeCell(this, node)
      c.generate_dist
      nodes_to_cell.update(node, c)
      table.driver.heartbeat
    }

    if (interpolateWeight > 0) {
      // this is really gross. we need to interpolate all the nodes
      // by modifying their worddist.count map. We are breaking
      // so many levels of abstraction by doing this AND preventing
      // us from using interpolation with bigrams :(
      //

      // We'll do it top-down so dependencies are met.

      val iwtopdown = true
      val nodes = 
        if (iwtopdown) kdtree.getNodes.toList
        else kdtree.getNodes.reverse

      for (node <- nodes if node.parent != null) {
        val cell = nodes_to_cell(node)
        val wd = cell.combined_dist.word_dist
        val uwd = wd.asInstanceOf[UnigramWordDist]

        for ((k,v) <- uwd.counts) {
          uwd.counts.put(k, (1 - interpolateWeight) * v)
        }

        val pcell = nodes_to_cell(node.parent)
        val pwd = pcell.combined_dist.word_dist
        val puwd = pwd.asInstanceOf[UnigramWordDist]

        for ((k,v) <- puwd.counts) {
          val oldv = uwd.counts.getOrElse(k, 0.0)
          val newv = oldv + interpolateWeight * v
          if (newv > interpolateWeight)
            uwd.counts.put(k, newv)
        }

        // gotta update these variables
        uwd.num_word_tokens = uwd.counts.values.sum
        driver.heartbeat
      }
    }

    val nodes = if (useBackoff) kdtree.getNodes else kdtree.getLeaves
    for (node <- nodes) {
      leaves_to_cell.update(node, nodes_to_cell(node))
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

