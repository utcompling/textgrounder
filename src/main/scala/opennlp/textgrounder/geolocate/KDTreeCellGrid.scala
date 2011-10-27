package opennlp.textgrounder.geolocate

import opennlp.textgrounder.geolocate.Distances.Coord

class KDTreeCellGrid extends CellGrid {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int = 0

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null.
   */
  def find_best_cell_for_coord(coord: Coord): GeoCell = { null }

  /**
   * Add the given article to the cell grid.
   */
  def add_article_to_cell(article: GeoArticle): Unit = {}

  /**
   * Generate all non-empty cells.  This will be called once (and only once),
   * after all articles have been added to the cell grid by calling
   * `add_article_to_cell`.  The generation happens internally; but after
   * this, `iter_nonempty_cells` should work properly.
   */
  protected def initialize_cells: Unit = {}

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
  def iter_nonempty_cells(nonempty_word_dist: Boolean = false): Iterable[GeoCell] = { null }
}

class KDTreeCell(cellgrid: CellGrid,
                 val southwestCoord:Coord,
                 val northeastCoord:Coord)
extends RectangularCell(cellgrid) {
  def get_southwest_coord: Coord = southwestCoord
  def get_northeast_coord: Coord = northeastCoord

  /**
   * Return a string describing the location of the cell in its grid,
   * e.g. by its boundaries or similar.
   */
  def describe_location: String = "SW: "+get_southwest_coord+", NE: "+get_northeast_coord

  /**
   * Return a string describing the indices of the cell in its grid.
   * Only used for debugging.
   */
   def describe_indices: String = ""

  /**
   * Return an Iterable over articles, listing the articles in the cell.
   */
  def iterate_articles(): Iterable[GeoArticle] = null
}

object KDTreeTest {
  def main(args:Array[String]) {
    val grid = new KDTreeCellGrid

    val emptyParams = Map[String, String]()

    val article1 = new GeoArticle(emptyParams)
    article1.coord = new Coord(31, -96, false)
    val article2 = new GeoArticle(emptyParams)
    article2.coord = new Coord(40, -74, false)

    grid.add_article_to_cell(article1)
    grid.add_article_to_cell(article2)

    grid.finish

    println(grid.find_best_cell_for_coord(new Coord(35, -98)))

  }
}
