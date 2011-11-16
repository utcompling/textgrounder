package opennlp.textgrounder.geolocate

import scala.collection.JavaConversions._
import scala.collection.mutable.Map

import ags.utils.KdTree

import opennlp.textgrounder.util.distances.Coord

import GeolocateDriver.Params

class KdTreeCell(
  cellgrid: KdTreeCellGrid,
  val kdleaf : KdTree[DistDocument]) extends RectangularCell(cellgrid) {

  def get_northeast_coord () : Coord = {
    new Coord(kdleaf.minLimit(0), kdleaf.minLimit(1))
  }
  
  def get_southwest_coord () : Coord = {
    new Coord(kdleaf.maxLimit(0), kdleaf.maxLimit(1))
  }
  
  def iterate_documents () : Iterable[DistDocument] = {
    kdleaf.getData()
  }

  override def get_center_coord () = {
    if (Params.kd_center_or_centroid == "center") {
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
      Coord(sum_lat / kdleaf.size, sum_long / kdleaf.size)
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
  def apply(table: DistDocumentTable, bucketSize: Int, splitMethod: String) : KdTreeCellGrid = {
    new KdTreeCellGrid(table, bucketSize, splitMethod match {
      case "halfway" => KdTree.SplitMethod.HALFWAY
      case "median" => KdTree.SplitMethod.MEDIAN
      case "maxmargin" => KdTree.SplitMethod.MAX_MARGIN
    })
  }
}

class KdTreeCellGrid(table: DistDocumentTable, bucketSize: Int, splitMethod: KdTree.SplitMethod)
    extends CellGrid(table) {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int = 0
  var kdtree : KdTree[DistDocument] = new KdTree[DistDocument](2, bucketSize, splitMethod);
  val leaves_to_cell : Map[KdTree[DistDocument], KdTreeCell] = Map();

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null.
   */
  def find_best_cell_for_coord(coord: Coord): KdTreeCell = {
    leaves_to_cell(kdtree.getLeaf(Array(coord.lat, coord.long)))
  }

  /**
   * Add the given document to the cell grid.
   */
  def add_document_to_cell(document: DistDocument): Unit = {
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

    for (leaf <- kdtree.getLeaves) {
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
  def iter_nonempty_cells(nonempty_word_dist: Boolean = false): Iterable[GeoCell] = {
    for (leaf <- kdtree.getLeaves
      if (leaf.size() > 0 || !nonempty_word_dist))
        yield leaves_to_cell(leaf)
  }
}

