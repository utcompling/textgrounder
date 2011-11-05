package opennlp.textgrounder.geolocate

import scala.collection.JavaConversions._
import ags.utils.KdTree
import opennlp.textgrounder.geolocate.Distances.Coord
import scala.collection.immutable.Map

class KdTreeCell(
  cellgrid: KdTreeCellGrid,
  val kdleaf : KdTree[GeoArticle]) extends RectangularCell(cellgrid) {

    def get_northeast_coord () : Coord = {
        new Coord(kdleaf.minLimit(0), kdleaf.minLimit(1))
    }
    
    def get_southwest_coord () : Coord = {
        new Coord(kdleaf.maxLimit(0), kdleaf.maxLimit(1))
    }
    
    def iterate_articles () : Iterable[GeoArticle] = {
        kdleaf.getData()
    }
    
    def describe_indices () : String = {
        "Placeholder"
    }
    
    def describe_location () : String = {
        get_boundary.toString
    }
}

class KdTreeCellGrid(table: GeoArticleTable) extends CellGrid(table) {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int = 0
  var kdtree : KdTree[GeoArticle] = new KdTree[GeoArticle](2);

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null.
   */
  def find_best_cell_for_coord(coord: Coord): KdTreeCell = {
      new KdTreeCell(this, kdtree.getLeaf(Array(coord.lat, coord.long)))
  }

  /**
   * Add the given article to the cell grid.
   */
  def add_article_to_cell(article: GeoArticle): Unit = {
      kdtree.addPoint(Array(article.coord.lat, article.coord.long), article)
  }

  /**
   * Generate all non-empty cells.  This will be called once (and only once),
   * after all articles have been added to the cell grid by calling
   * `add_article_to_cell`.  The generation happens internally; but after
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
   *   one article in them. (Not all articles have word distributions, esp.
   *   when --max-time-per-stage has been set to a non-zero value so that we
   *   only load some subset of the word distributions for all articles.  But
   *   even when not set, some articles may be listed in the article-data file
   *   but have no corresponding word counts given in the counts file.)
   */
  def iter_nonempty_cells(nonempty_word_dist: Boolean = false): Iterable[GeoCell] = {
      for (leaf <- kdtree.getLeaves)
          yield new KdTreeCell(this, leaf)
  }
}

object KdTreeTest {
  def main(args:Array[String]) {
    val factory = new PseudoGoodTuringSmoothedWordDistFactory
    val table = new GeoArticleTable(factory)
    val grid = new KdTreeCellGrid(table)

    val emptyParams = Map[String, String]()

    val article1 = new GeoArticle(emptyParams)
    article1.coord = new Coord(31, -96, false)
    val article2 = new GeoArticle(emptyParams)
    article2.coord = new Coord(40, -74, false)

    grid.add_article_to_cell(article1)
    grid.add_article_to_cell(article2)

    grid.finish

    println(grid.find_best_cell_for_coord(new Coord(35, -98)).kdleaf)
    println(grid.find_best_cell_for_coord(new Coord(35, -98)))

  }
}
