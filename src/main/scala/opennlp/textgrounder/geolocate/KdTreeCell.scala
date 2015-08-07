///////////////////////////////////////////////////////////////////////////////
//  KdTreeCell.scala
//
//  Copyright (C) 2011, 2012 Stephen Roller, The University of Texas at Austin
//  Copyright (C) 2011-2014 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package geolocate

import scala.collection.JavaConversions._
import scala.collection.mutable

import ags.utils.KdTree
import KdTree.SplitMethod

import util.debug._
import util.experiment._
import util.error._
import util.print.errprint
import util.spherical.SphereCoord

import langmodel.UnigramLangModel

import gridlocate.{RawDoc, DocStatus, GridDocFactory}

class KdTreeCell(
  cellgrid: KdTreeGrid,
  val kdnode: KdTree
) extends RectangularCell(cellgrid) {

  def get_southwest_coord =
    new SphereCoord(kdnode.minBoundary(0), kdnode.minBoundary(1))

  def get_northeast_coord =
    new SphereCoord(kdnode.maxBoundary(0), kdnode.maxBoundary(1))

  // FIXME! Describe path down to this node.
  def format_indices = {
    def describe_node(node: KdTree) = s"${node.size}"
    var path: List[String] = Nil
    var node = kdnode
    while (node.parent != null) {
      val childindic = if (node.parent.getLeft == node) "<" else {
        assert_==(node.parent.getRight, node, "node")
        ">"
      }
      path = "%s%s".format(childindic, describe_node(node)) :: path
    }
    path = describe_node(node) :: path
    path mkString ""
  }
}

object KdTreeGrid {
  def kd_split_method_to_enum(split_method: String) = {
    split_method match {
      case "halfway" => SplitMethod.HALFWAY
      case "median" => SplitMethod.MEDIAN
      case "maxmargin" => SplitMethod.MAX_MARGIN
    }
  }
}

/**
 * Class for K-d tree grid. When not doing hierarchical classification,
 * `cutoffBucketSize` should be 0, `existingGrid` should be None, and
 * `existingGridNewLeaves` should be empty. For hierarchical classification
 * at level 1 (the coarsest level), pass in the same parameters except that
 * `cutoffBucketSize` should specify a minimum bucket size for which we
 * continue to split the cells, which should be greater than the 'bucketSize'
 * used to create the actual K-d grid. The cells returned by
 * `iter_nonempty_cells` are determined by only dividing nodes with at least
 * the specified bucket size. For hierarchical classification at level &gt; 1,
 * `cutoffBucketSize` should be as for level 1, `existingGrid` should be one
 * of the grids at a higher-up level and `existingGridNewLeaves` should be the
 * list of leaves to be returned by `iter_nonempty_cells` at this level.
 * Note that the list of cells returned by `iter_nonempty_cells`, which comes
 * from the `leaf_node` field, is also used by `find_best_cell_for_coord`.
 */
class KdTreeGrid(
  docfact: SphereDocFactory,
  id: String,
  bucketSize: Int,
  splitMethod: SplitMethod,
  useBackoff: Boolean,
  interpolateWeight: Double,
  existingGrid: Option[KdTreeGrid] = None,
  existingGridNewLeaves: Iterable[KdTree] = Iterable(),
  cutoffBucketSize: Int = 0
) extends RectangularGrid(docfact, id) {
  def short_type = "kd"

  if (useBackoff && cutoffBucketSize > 0)
    warning("--kd-use-backoff probably incompatible with hierarchical classification")
  assert((existingGrid == None) == (existingGridNewLeaves.size == 0),
    s"existingGrid: $existingGrid, existingGridNewLeaves: $existingGridNewLeaves")
  if (existingGrid != None)
    assert_>(cutoffBucketSize, 0)
  if (cutoffBucketSize > 0)
    assert_>=(cutoffBucketSize, bucketSize)
  if (debug("kd-tree-grid")) {
    errprint("Created K-d tree grid with id %s", id)
    errprint("  bucket size %s, cutoffBucketSize %s", bucketSize, cutoffBucketSize)
    errprint("  has existing grid: %s", if (existingGrid == None) "no" else "yes")
    val sizes = existingGridNewLeaves.map(_.size)
    if (existingGridNewLeaves.size == 0)
      errprint("  existingGridNewLeaves: len 0")
    else
      errprint("  existingGridNewLeaves: len %s, min #docs %s, max #docs %s",
        existingGridNewLeaves.size, sizes.min, sizes.max)
  }

  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int = 0
  /** Underlying K-d tree object. */
  val kdtree: KdTree = existingGrid match {
    case Some(grid) => grid.kdtree
    case None => new KdTree(2, bucketSize, splitMethod)
  }
  /** Map from nodes in the K-d tree to cells. */
  val nodes_to_cell: mutable.Map[KdTree, KdTreeCell] = existingGrid match {
    case Some(grid) => grid.nodes_to_cell
    case None => mutable.Map[KdTree, KdTreeCell]()
  }
  /** Set of leaf nodes as returned by iter_nonempty_cells. Will be the same
    * as the actual leaf nodes of the tree except when we have one of the
    * higher levels during hierarchical classification.
    */
  var leaf_nodes: Set[KdTree] = _

  // Fetch the child nodes, but if none, return the same node instead
  def get_node_children(node: KdTree) = {
    val left = node.getLeft
    val right = node.getRight
    if (left == null && right == null)
      Seq(node)
    else {
      assert(left != null)
      assert(right != null)
      Seq(left, right)
    }
  }

  // Fetch the leaf nodes underneath the given node.
  def get_leaf_nodes(node: KdTree): Seq[KdTree] = {
    val left = node.getLeft
    val right = node.getRight
    if (left == null && right == null)
      Seq(node)
    else {
      assert(left != null)
      assert(right != null)
      get_leaf_nodes(left) ++ get_leaf_nodes(right)
    }
  }

  // Fetch the nodes down to the cutoff given by `cutoffBucketSize`
  def get_nodes_to_cutoff(nodes: Iterable[KdTree], cutoff: Int
      ): Iterable[KdTree] = {
    // Do breadth-wise descent of tree, dividing nodes above the cutoff
    // and repeating till we divide no nodes
    def get_next_cutoff_level(nodes: Iterable[KdTree]) = {
      nodes.flatMap { node =>
        // We do > not >= because the underlying K-d code uses > to
        // decide when to split a node
        if (node.size > cutoff)
          get_node_children(node)
        else
          Seq(node)
      }
    }
    val next_level = get_next_cutoff_level(nodes)
    // This is tail-recursive
    if (next_level.size == nodes.size)
      nodes
    else get_nodes_to_cutoff(next_level, cutoff)
  }

  // // Fetch the nodes down to a specified depth, doing breadth-wise descent
  // def get_nodes_to_depth(nodes: Iterable[KdTree], depth: Int
  //     ): Iterable[KdTree] = {
  //   // This is tail-recursive
  //   if (depth == 0) nodes
  //   else get_nodes_to_depth(nodes.flatMap(get_node_children), depth - 1)
  // }

  def get_nodes_to_subdivide_depth(nodes: Iterable[KdTree],
      new_cutoff_size: Int) = {
    if (debug("describe-kd-tree"))
      errprint("get_nodes_to_subdivide_depth: cutoff=%s", new_cutoff_size)
    val new_nodes = get_nodes_to_cutoff(nodes, new_cutoff_size)
    if (debug("describe-kd-tree")) {
      for (node <- new_nodes)
        errprint("  %s", describe_this_node(node, 0))
    }
    new_nodes
  }

  // In MultiRegularGrid, we need to create and populate a new grid at a
  // finer scale, and then map a cell in the coarser grid to the overlapping
  // cells in the finer grid. In this case, the K-d tree already holds
  // cells at all levels that were initialized with their language models at
  // the time we created the initial grid, so all we need to do is create
  // a copy of the existing grid with the leaf nodes specified (which may
  // be nodes higher up than the actual leaf nodes in the underlying K-d tree),
  // which should be lower down than the leaf nodes in the existing grid.
  // I think we need to ensure that the leaf nodes in the new grid are the
  // same ones returned by imp_get_subdivided_cells. FIXME: When are
  // iter_nonempty_cells and find_best_cell_for_coord called during
  // hierarchical classification?
  def create_subdivided_grid(create_docfact: => GridDocFactory[SphereCoord],
      id: String) = {
    // Fetch the nodes down to the new cutoff size
    val new_cutoff_size = cutoffBucketSize /
      driver.asInstanceOf[GeolocateDriver].params.subdivide_factor
    val new_nodes = get_nodes_to_subdivide_depth(leaf_nodes, new_cutoff_size)
    val ret = new KdTreeGrid(docfact, id, bucketSize, splitMethod, useBackoff,
      interpolateWeight, Some(this), new_nodes, new_cutoff_size)
    if (debug("describe-kd-tree")) {
      errprint(s"For grid $id:")
      for (node <- new_nodes)
        errprint("  %s", ret.describe_this_node(node, 0))
    }
    ret
  }

  def imp_get_subdivided_cells(cell: SphereCell) = {
    val kdcell = cell.asInstanceOf[KdTreeCell]
    val new_nodes =
      get_nodes_to_subdivide_depth(Seq(kdcell.kdnode), cutoffBucketSize)
    if (debug("assert-leaf")) {
      // WARNING!!! This appears to only make sense for 2-level hierarchies.
      // For 3-level hierarchies, you will expect to get non-leaves
      // returned on level 2.
      errprint("Asserting leaf in imp_get_subdivided_cells")
      for (node <- new_nodes)
        assert(node.getLeft == null && node.getRight == null, {
          errprint("Saw non-leaf node:")
          describe_node(node, 1)
          s"Saw non-leaf node $node"
        })
      val leaf_nodes = get_leaf_nodes(kdcell.kdnode)
      assert_==(new_nodes.size, leaf_nodes.size)
      errprint("For cell %s, %s subdividing cells", cell, new_nodes.size)
    }
    new_nodes.map(nodes_to_cell(_))
  }

  def describe_this_node(node: KdTree, depth: Int,
      fn: KdTree => String = x => "") {
    errprint("%s%s#: (%s,%s) - (%s,%s): %s%s",
      "  " * depth,
      node.size,
      node.minBoundary(0), node.minBoundary(1),
      node.maxBoundary(0), node.maxBoundary(1),
      nodes_to_cell(node), {
        val str = fn(node)
        if (str == "") ""
        else s": $str"
      })
  }

  def describe_node(node: KdTree, depth: Int,
      fn: KdTree => String = x => "") {
    describe_this_node(node, depth, fn)
    val left = node.getLeft
    if (left != null)
      describe_node(left, depth + 1, fn)
    val right = node.getRight
    if (right != null)
      describe_node(right, depth + 1, fn)
  }

  def describe_kd_tree() {
    describe_node(kdtree, 0)
  }

  def add_training_documents_to_grid(
      get_rawdocs: String => Iterator[DocStatus[RawDoc]]) {
    if (existingGrid != None)
      return

    for (doc <- docfact.raw_documents_to_documents(
           get_rawdocs("preliminary pass to generate K-d tree: reading"),
           skip_no_coord = true,
           note_globally = false,
           finish_globally = false)) {
      kdtree.addPoint(Array(doc.coord.lat, doc.coord.long))
    }

    // we've seen all the coordinates. we need to build up
    // the entire kd-tree structure now, the centroids, and
    // clean out the data.

    val task =
      driver.show_progress("generating", "K-d tree structure").start()

    // build the full kd-tree structure.
    kdtree.balance

    for (node <- kdtree.getNodes) {
      val c = new KdTreeCell(this, node)
      nodes_to_cell(node) = c
      task.item_processed()
    }
    task.finish()

    // no longer need to keep all our locations in memory. destroy
    // them. to free up memory.
    kdtree.annihilateData

    // Now read normally.
    default_add_training_documents_to_grid(get_rawdocs, doc => {
      val leaf = kdtree.getLeaf(Array(doc.coord.lat, doc.coord.long))
      var n = leaf
      while (n != null) {
        nodes_to_cell(n).add_document(doc)
        n = n.parent
      }
    })

    if (debug("describe-kd-tree"))
      describe_kd_tree()
  }

  def find_best_cell_for_coord(coord: SphereCoord,
      create_non_recorded: Boolean) = {
    var leaf = kdtree.getLeaf(Array(coord.lat, coord.long))
    while (!(leaf_nodes contains leaf)) {
      leaf = leaf.parent
      assert(leaf != null)
    }

    // FIXME: implementation note: the KD tree should tile the entire
    // earth's surface, but there's a possibility of something going awry
    // here if we've never seen an evaluation point before.
    Some(nodes_to_cell(leaf))
  }

  override def finish_document_factory() {
    if (existingGrid == None)
      super.finish_document_factory()
  }

  /**
   * Generate all non-empty cells.  This will be called once (and only once),
   * after all documents have been added to the cell grid by calling
   * `add_document_to_cell`.  The generation happens internally; but after
   * this, `iter_nonempty_cells` should work properly.
   */
  def initialize_cells() {
    // here we need to drop nonleaf nodes unless backoff is enabled.
    leaf_nodes =
      if (useBackoff) kdtree.getNodes.toSet
      else if (existingGridNewLeaves.size > 0)
        existingGridNewLeaves.toSet
      else if (cutoffBucketSize == 0) kdtree.getLeaves.toSet
      else get_nodes_to_cutoff(Seq(kdtree), cutoffBucketSize).toSet

    total_num_cells = leaf_nodes.size

    if (existingGrid == None) {
      // need to finish generating all the language models
      for (c <- nodes_to_cell.valuesIterator) {
        c.finish()
      }

      if (interpolateWeight > 0) {
        // this is really gross. we need to interpolate all the nodes
        // by modifying their langmodel.count map. We are breaking
        // so many levels of abstraction by doing this AND preventing
        // us from using interpolation with bigrams :(
        //

        // We'll do it top-down so dependencies are met.

        val iwtopdown = true
        val nodes =
          if (iwtopdown) kdtree.getNodes.toList
          else kdtree.getNodes.reverse

        driver.show_progress("interpolating", "K-d tree cell").
        foreach(nodes.filter(_.parent != null)) { node =>
          val cell = nodes_to_cell(node)
          val pcell = nodes_to_cell(node.parent)
          // There may be a separate grid_lm and rerank_lm; do both if needed
          (cell.lang_model.iterator zip pcell.lang_model.iterator).foreach {
            case (wd, pwd) =>
              for ((k,v) <- wd.iter_grams_for_modify) {
                wd.set_gram(k, (1 - interpolateWeight) * v)
              }

              for ((k,v) <- pwd.iter_grams) {
                val oldv = if (wd contains k) wd.get_gram(k) else 0.0
                val newv = oldv + interpolateWeight * v
                if (newv > interpolateWeight)
                  wd.set_gram(k, newv)
              }
          }
        }
      }
    }
  }

  /**
   * Iterate over all non-empty cells.
   */
  def imp_iter_nonempty_cells = {
    // Converting to IndexedSeq is very important because otherwise it's
    // a set and all sorts of unexpected things result from mapping a
    // set to something else with duplicate items. Since mapping a set
    // to something else yields a set, the duplicate items are erased,
    // which is almost never what's desired. So we should never return
    // a Set when an Iterable is called for.
    (for {
      leaf <- leaf_nodes
      cell = {
        assert_>(leaf.size, 0)
        nodes_to_cell(leaf)
      }
      if !cell.is_empty // can be empty if there is a bounding-box restriction
    } yield cell).toIndexedSeq
  }

  override def output_ranking_data(docid: String,
      xpred_cells: Iterable[(SphereCell, Double)],
      xparent_cell: Option[SphereCell],
      xcorrect_cell: Option[SphereCell]) {
    super.output_ranking_data(docid, xpred_cells, xparent_cell, xcorrect_cell)
    if (xparent_cell == None || !debug("assert-leaf"))
      return
    // WARNING!!! assert-leaf appears to only make sense for 2-level
    // hierarchies. For 3-level hierarchies, you will expect to get non-leaves
    // returned on level 2.
    val parent_cell = xparent_cell.get.asInstanceOf[KdTreeCell]
    val pred_cells = xpred_cells.asInstanceOf[Iterable[(KdTreeCell, Double)]]
    errprint("Asserting leaf in output_ranking_data")
    errprint(s"cutoffBucketSize is $cutoffBucketSize")
    for ((cell, score) <- pred_cells) {
      val node = cell.kdnode
      assert(node.getLeft == null && node.getRight == null, {
        errprint("Saw non-leaf node:")
        describe_node(node, 1)
        s"Saw non-leaf node $node"
      })
    }
    val parent_node = parent_cell.kdnode
    val pred_score_rank_map =
      pred_cells.zip(Stream.from(1)).map {
        case ((cell, score), rank) => (cell.kdnode, (score, rank))
      }.toMap
    describe_node(parent_node, 0, node => {
      if (pred_score_rank_map contains node) {
        val (score, rank) = pred_score_rank_map(node)
        val str = s"rank: $rank, score: $score"
        if (node.getLeft == null && node.getRight == null) str
        else s"WARNING: NON-LEAF NODE IN PRED_CELLS: $str"
      } else if (node.getLeft == null && node.getRight == null)
        "WARNING: LEAF NODE NOT IN PRED_CELLS"
        else
        "(non-leaf node)"
    })
    val leaf_nodes = get_leaf_nodes(parent_node)
    assert_==(pred_cells.size, leaf_nodes.size)
    val pred_nodes = pred_cells.map { case (cell, score) => cell.kdnode }
    assert_==(pred_nodes.toSet, leaf_nodes.toSet)
  }
}

