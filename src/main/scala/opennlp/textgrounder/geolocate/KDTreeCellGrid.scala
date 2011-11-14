package opennlp.textgrounder.geolocate

import scala.collection.JavaConversions._
import scala.collection.immutable.Map

import ags.utils.KdTree

import opennlp.textgrounder.util.distances.Coord

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
    
    def describe_indices () : String = {
        "Placeholder"
    }
    
    def describe_location () : String = {
        get_boundary.toString
    }
}

class KdTreeCellGrid(table: DistDocumentTable, bucketSize: Int)
    extends CellGrid(table) {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int = 0
  var kdtree : KdTree[DistDocument] = new KdTree[DistDocument](2, bucketSize);

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null.
   */
  def find_best_cell_for_coord(coord: Coord): KdTreeCell = {
      new KdTreeCell(this, kdtree.getLeaf(Array(coord.lat, coord.long)))
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
      for (leaf <- kdtree.getLeaves)
          yield new KdTreeCell(this, leaf)
  }
}

