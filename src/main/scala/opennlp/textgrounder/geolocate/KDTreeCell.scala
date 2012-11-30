///////////////////////////////////////////////////////////////////////////////
//  KDTreeGrid.scala
//
//  Copyright (C) 2011, 2012 Stephen Roller, The University of Texas at Austin
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
//  Copyright (C) 2011 Mike Speriosu, The University of Texas at Austin
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

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import ags.utils.KdTree

import opennlp.textgrounder.{util => tgutil}
import tgutil.distances.SphereCoord
import tgutil.experiment._
import tgutil.printutil.{errprint, warning}

import opennlp.textgrounder.worddist.UnigramWordDist

class KdTreeCell(
  cellgrid: KdTreeGrid,
  val kdleaf : KdTree
) extends RectangularCell(cellgrid) {

  def get_northeast_coord =
    new SphereCoord(kdleaf.minLimit(0), kdleaf.minLimit(1))

  def get_southwest_coord =
    new SphereCoord(kdleaf.maxLimit(0), kdleaf.maxLimit(1))

  def describe_indices = "Placeholder"

  def describe_location =
    get_boundary.toString + " (Center: " + get_center_coord + ")"
}

object KdTreeGrid {
  def apply(docfact: SphereDocFactory, bucketSize: Int,
            splitMethod: String, useBackoff: Boolean,
            interpolateWeight: Double = 0.0) : KdTreeGrid = {
    new KdTreeGrid(docfact, bucketSize, splitMethod match {
      case "halfway" => KdTree.SplitMethod.HALFWAY
      case "median" => KdTree.SplitMethod.MEDIAN
      case "maxmargin" => KdTree.SplitMethod.MAX_MARGIN
    }, useBackoff, interpolateWeight)
  }
}

class KdTreeGrid(
  docfact: SphereDocFactory,
  bucketSize: Int,
  splitMethod: KdTree.SplitMethod,
  useBackoff: Boolean,
  interpolateWeight: Double
) extends SphereGrid(docfact) {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int = 0
  var kdtree: KdTree = new KdTree(2, bucketSize, splitMethod)

  val nodes_to_cell: Map[KdTree, KdTreeCell] = Map()
  val leaves_to_cell: Map[KdTree, KdTreeCell] = Map()

  override val num_training_passes: Int = 2
  var current_training_pass: Int = 0

  override def begin_training_pass(pass: Int) = {
    current_training_pass = pass

    if (pass == 1) {
      // do nothing
    } else if (pass == 2) {
      // we've seen all the coordinates. we need to build up
      // the entire kd-tree structure now, the centroids, and
      // clean out the data.

      val task =
        docfact.driver.show_progress("K-d tree structure", "generating").start()

      // build the full kd-tree structure.
      kdtree.balance

      for (node <- kdtree.getNodes) {
        val c = new KdTreeCell(this, node)
        nodes_to_cell.update(node, c)
        task.item_processed()
      }
      task.finish()

      // no longer need to keep all our locations in memory. destroy
      // them. to free up memory.
      kdtree.annihilateData
    } else {
      // definitely should not get here
      assert(false);
    }
  }

  def find_best_cell_for_document(doc: SphereDoc,
      create_non_recorded: Boolean) = {
    // FIXME: implementation note: the KD tree should tile the entire earth's surface,
    // but there's a possibility of something going awry here if we've never
    // seen a evaluation point before.
    Option(leaves_to_cell(kdtree.getLeaf(Array(doc.coord.lat, doc.coord.long))))
  }

  /**
   * Add the given document to the cell grid.
   */
  def add_document_to_cell(document: SphereDoc) {
    if (current_training_pass == 1) {
      kdtree.addPoint(Array(document.coord.lat, document.coord.long))
    } else if (current_training_pass == 2) {
      val leaf = kdtree.getLeaf(Array(document.coord.lat, document.coord.long))
      var n = leaf
      while (n != null) {
        nodes_to_cell(n).add_document(document)
        n = n.parent;
      }
    } else {
      assert(false)
    }
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

    // need to finish generating all the word distributions
    for (c <- nodes_to_cell.valuesIterator) {
      c.finish()
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

      docfact.driver.show_progress("K-d tree cell", "interpolating").
      foreach(nodes.filter(_.parent != null)) { node =>
        val cell = nodes_to_cell(node)
        val wd = cell.combined_dist.word_dist
        val model = wd.asInstanceOf[UnigramWordDist].model

        for ((k,v) <- model.iter_items) {
          model.set_item(k, (1 - interpolateWeight) * v)
        }

        val pcell = nodes_to_cell(node.parent)
        val pwd = pcell.combined_dist.word_dist
        val pmodel = pwd.asInstanceOf[UnigramWordDist].model

        for ((k,v) <- pmodel.iter_items) {
          val oldv = if (model contains k) model.get_item(k) else 0.0
          val newv = oldv + interpolateWeight * v
          if (newv > interpolateWeight)
            model.set_item(k, newv)
        }
      }
    }

    // here we need to drop nonleaf nodes unless backoff is enabled.
    val nodes = if (useBackoff) kdtree.getNodes else kdtree.getLeaves
    for (node <- nodes) {
      leaves_to_cell.update(node, nodes_to_cell(node))
    }
  }

  /**
   * Iterate over all non-empty cells.
   */
  def iter_nonempty_cells: Iterable[SphereCell] = {
    val nodes = if (useBackoff) kdtree.getNodes else kdtree.getLeaves
    for (leaf <- nodes if leaf.size > 0)
        yield leaves_to_cell(leaf)
  }
}

