///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010, 2011 Ben Wing, The University of Texas at Austin
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
//////// MultiRegularCell.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import math._
import collection.mutable

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.printutil.{errout, errprint}

import GeolocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                         A regularly spaced grid                         //
/////////////////////////////////////////////////////////////////////////////

/* 
  We divide the earth's surface into "tiling cells", all of which are the
  same square size, running on latitude/longitude lines, and which have a
  constant number of degrees on a size, set using the value of the command-
  line option --degrees-per-cell. (Alternatively, the value of --miles-per-cell
  or --km-per-cell are converted into degrees using 'miles_per_degree' or
  'km_per_degree', respectively, which specify the size of a degree at
  the equator and is derived from the value for the Earth's radius.)

  In addition, we form a square of tiling cells in order to create a
  "multi cell", which is used to compute a distribution over words.  The
  number of tiling cells on a side of a multi cell is determined by
  --width-of-multi-cell.  Note that if this is greater than 1, different
  multi cells will overlap.

  To specify a cell, we use cell indices, which are derived from
  coordinates by dividing by degrees_per_cell.  Hence, if for example
  degrees_per_cell is 2.0, then cell indices are in the range [-45,+45]
  for latitude and [-90,+90) for longitude.  In general, an arbitrary
  coordinate will have fractional cell indices; however, the cell indices
  of the corners of a cell (tiling or multi) will be integers.  Normally,
  we use the southwest corner to specify a cell.

  Correspondingly, to convert a cell index to a SphereCoord, we multiply
  latitude and longitude by degrees_per_cell.

  Near the edges, tiling cells may be truncated.  Multi cells will
  wrap around longitudinally, and will still have the same number of
  tiling cells, but may be smaller.
  */

/**
 * The index of a regular cell, using "cell index" integers, as described
 * above.
 */
case class RegularCellIndex(latind: Int, longind: Int) {
  def toFractional() = FractionalRegularCellIndex(latind, longind)
}

/**
 * Similar to `RegularCellIndex`, but for the case where the indices are
 * fractional, representing a location other than at the corners of a
 * cell.
 */
case class FractionalRegularCellIndex(latind: Double, longind: Double) {
}

/**
 * A cell where the cell grid is a MultiRegularCellGrid. (See that class.)
 *
 * @param cell_grid The CellGrid object for the grid this cell is in,
 *   an instance of MultiRegularCellGrid.
 * @param index Index of this cell in the grid
 */

class MultiRegularCell(
  cell_grid: MultiRegularCellGrid,
  val index: RegularCellIndex
) extends RectangularCell(cell_grid) {

  def get_southwest_coord() =
    cell_grid.multi_cell_index_to_near_corner_coord(index)

  def get_northeast_coord() =
    cell_grid.multi_cell_index_to_far_corner_coord(index)

  def describe_location() = {
    "%s-%s" format (get_southwest_coord(), get_northeast_coord())
  }

  def describe_indices() = "%s,%s" format (index.latind, index.longind)

  def iterate_documents() = {
    val maxlatind = (
      (cell_grid.maximum_latind + 1) min
      (index.latind + cell_grid.width_of_multi_cell))

    if (debug("lots")) {
      errprint("Generating distribution for multi cell centered at %s",
        cell_grid.cell_index_to_coord(index))
    }

    // Process the tiling cells making up the multi cell;
    // but be careful around the edges.  Truncate the latitude, wrap the
    // longitude.
    for {
      // The use of view() here in both iterators causes this iterable to
      // be lazy; hence the print statement below doesn't get executed until
      // we actually process the documents in question.
      i <- (index.latind until maxlatind) view;
      rawj <- (index.longind until
        (index.longind + cell_grid.width_of_multi_cell)) view;
      val j = (if (rawj > cell_grid.maximum_longind) rawj - 360 else rawj)
      doc <- {
        if (debug("lots")) {
          errprint("--> Processing tiling cell %s",
            cell_grid.cell_index_to_coord(index))
        }
        cell_grid.tiling_cell_to_documents.getNoSet(RegularCellIndex(i, j))
      }
    } yield doc
  }
}

/**
 * Grid composed of possibly-overlapping multi cells, based on an underlying
 * grid of regularly-spaced square cells tiling the earth.  The multi cells,
 * over which word distributions are computed for comparison with the word
 * distribution of a given document, are composed of NxN tiles, where possibly
 * N > 1.
 *
 * FIXME: We should abstract out the concept of a grid composed of tiles and
 * a grid composed of overlapping conglomerations of tiles; this could be
 * useful e.g. for KD trees or other representations where we might want to
 * compare with cells at multiple levels of granularity.
 * 
 * @param degrees_per_cell Size of each cell in degrees.  Determined by the
 *   --degrees-per-cell option, unless --miles-per-cell is set, in which
 *   case it takes priority.
 * @param width_of_multi_cell Size of multi cells in tiling cells,
 *   determined by the --width-of-multi-cell option.
 */
class MultiRegularCellGrid(
  val degrees_per_cell: Double,
  val width_of_multi_cell: Int,
  table: SphereDocumentTable
) extends SphereCellGrid(table) {

  /**
   * Size of each cell (vertical dimension; horizontal dimension only near
   * the equator) in km.  Determined from degrees_per_cell.
   */
  val km_per_cell = degrees_per_cell * km_per_degree

  /* Set minimum, maximum latitude/longitude in indices (integers used to
     index the set of cells that tile the earth).   The actual maximum
     latitude is exactly 90 (the North Pole).  But if we set degrees per
     cell to be a number that exactly divides 180, and we use
     maximum_latitude = 90 in the following computations, then we would
     end up with the North Pole in a cell by itself, something we probably
     don't want.
   */
  val maximum_index =
    coord_to_tiling_cell_index(SphereCoord(maximum_latitude - 1e-10,
      maximum_longitude))
  val maximum_latind = maximum_index.latind
  val maximum_longind = maximum_index.longind
  val minimum_index =
    coord_to_tiling_cell_index(SphereCoord(minimum_latitude, minimum_longitude))
  val minimum_latind = minimum_index.latind
  val minimum_longind = minimum_index.longind

  /**
   * Mapping of cell->locations in cell, for cell-based Naive Bayes
   * disambiguation.  The key is a tuple expressing the integer indices of the
   * latitude and longitude of the southwest corner of the cell. (Basically,
   * given an index, the latitude or longitude of the southwest corner is
   * index*degrees_per_cell, and the cell includes all locations whose
   * latitude or longitude is in the half-open interval
   * [index*degrees_per_cell, (index+1)*degrees_per_cell).
   *
   * We don't just create an array because we expect many cells to have no
   * documents in them, esp. as we decrease the cell size.  The idea is that
   * the cells provide a first approximation to the cells used to create the
   * document distributions.
   */
  var tiling_cell_to_documents = bufmap[RegularCellIndex, SphereDocument]()

  /**
   * Mapping from index of southwest corner of multi cell to corresponding
   * cell object.  A "multi cell" is made up of a square of tiling cells,
   * with the number of cells on a side determined by `width_of_multi_cell'.
   * A word distribution is associated with each multi cell.
   */
  val corner_to_multi_cell = mutable.Map[RegularCellIndex, MultiRegularCell]()

  var total_num_cells = 0

  /********** Conversion between Cell indices and SphereCoords **********/

  /* The different functions vary depending on where in the particular cell
     the SphereCoord is wanted, e.g. one of the corners or the center. */

  /**
   * Convert a coordinate to the indices of the southwest corner of the
   * corresponding tiling cell.
   */
  def coord_to_tiling_cell_index(coord: SphereCoord) = {
    val latind = floor(coord.lat / degrees_per_cell).toInt
    val longind = floor(coord.long / degrees_per_cell).toInt
    RegularCellIndex(latind, longind)
  }

  /**
   * Convert a coordinate to the indices of the southwest corner of the
   * corresponding multi cell.
   */
  def coord_to_multi_cell_index(coord: SphereCoord) = {
    // When width_of_multi_cell = 1, don't subtract anything.
    // When width_of_multi_cell = 2, subtract 0.5*degrees_per_cell.
    // When width_of_multi_cell = 3, subtract degrees_per_cell.
    // When width_of_multi_cell = 4, subtract 1.5*degrees_per_cell.
    // In general, subtract (width_of_multi_cell-1)/2.0*degrees_per_cell.

    // Compute the indices of the southwest cell
    val subval = (width_of_multi_cell - 1) / 2.0 * degrees_per_cell
    coord_to_tiling_cell_index(
      SphereCoord(coord.lat - subval, coord.long - subval))
  }

  /**
   * Convert a fractional cell index to the corresponding coordinate.  Useful
   * for indices not referring to the corner of a cell.
   * 
   * @see #cell_index_to_coord
   */
  def fractional_cell_index_to_coord(index: FractionalRegularCellIndex,
    method: String = "coerce-warn") = {
    SphereCoord(index.latind * degrees_per_cell, index.longind * degrees_per_cell,
      method)
  }

  /**
   * Convert cell indices to the corresponding coordinate.  This can also
   * be used to find the coordinate of the southwest corner of a tiling cell
   * or multi cell, as both are identified by the cell indices of
   * their southwest corner.
   */
  def cell_index_to_coord(index: RegularCellIndex,
    method: String = "coerce-warn") =
    fractional_cell_index_to_coord(index.toFractional, method)

  /** 
   * Add 'offset' to both latind and longind of 'index' and then convert to a
   * coordinate.  Coerce the coordinate to be within bounds.
   */
  def offset_cell_index_to_coord(index: RegularCellIndex,
    offset: Double) = {
    fractional_cell_index_to_coord(
      FractionalRegularCellIndex(index.latind + offset, index.longind + offset),
      "coerce")
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * near (i.e. southwest) corner of the cell.
   */
  def tiling_cell_index_to_near_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(index)
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * center of the cell.
   */
  def tiling_cell_index_to_center_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, 0.5)
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * far (i.e. northeast) corner of the cell.
   */
  def tiling_cell_index_to_far_corner_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, 1.0)
  }
  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * near (i.e. southwest) corner of the cell.
   */
  def multi_cell_index_to_near_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(index)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * center of the cell.
   */
  def multi_cell_index_to_center_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, width_of_multi_cell / 2.0)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * far (i.e. northeast) corner of the cell.
   */
  def multi_cell_index_to_far_corner_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, width_of_multi_cell)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * northwest corner of the cell.
   */
  def multi_cell_index_to_nw_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(
      RegularCellIndex(index.latind + width_of_multi_cell, index.longind),
      "coerce")
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * southeast corner of the cell.
   */
  def multi_cell_index_to_se_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(
      RegularCellIndex(index.latind, index.longind + width_of_multi_cell),
      "coerce")
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * southwest corner of the cell.
   */
  def multi_cell_index_to_sw_corner_coord(index: RegularCellIndex) = {
    multi_cell_index_to_near_corner_coord(index)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * northeast corner of the cell.
   */
  def multi_cell_index_to_ne_corner_coord(index: RegularCellIndex) = {
    multi_cell_index_to_far_corner_coord(index)
  }

  /*************** End conversion functions *************/

  def find_best_cell_for_coord(coord: SphereCoord) = {
    val index = coord_to_multi_cell_index(coord)
    find_cell_for_cell_index(index)
  }

  protected def find_cell_for_cell_index(index: RegularCellIndex,
    create: Boolean = false) = {
    if (!create)
      assert(all_cells_computed)
    val statcell = corner_to_multi_cell.getOrElse(index, null)
    if (statcell != null)
      statcell
    else if (!create) null
    else {
      val newstat = new MultiRegularCell(this, index)
      newstat.generate_dist()
      if (newstat.word_dist_wrapper.is_empty())
        null
      else {
        num_non_empty_cells += 1
        corner_to_multi_cell(index) = newstat
        newstat
      }
    }
  }

  def add_document_to_cell(document: SphereDocument) {
    val index = coord_to_tiling_cell_index(document.coord)
    tiling_cell_to_documents(index) += document
  }

  protected def initialize_cells() {
    val task = new MeteredTask("Earth-tiling cell", "generating non-empty")

    for (i <- minimum_latind to maximum_latind view) {
      for (j <- minimum_longind to maximum_longind view) {
        total_num_cells += 1
        val cell = find_cell_for_cell_index(RegularCellIndex(i, j),
          create = true)
        if (debug("cell") && !cell.word_dist_wrapper.is_empty)
          errprint("--> (%d,%d): %s", i, j, cell)
        task.item_processed()
      }
    }
    task.finish()

    // Save some memory by clearing this after it's not needed
    tiling_cell_to_documents = null
  }

  def iter_nonempty_cells(nonempty_word_dist: Boolean = false) = {
    assert(all_cells_computed)
    for {
      v <- corner_to_multi_cell.values
      val empty = (
        if (nonempty_word_dist) v.word_dist_wrapper.is_empty_for_word_dist()
        else v.word_dist_wrapper.is_empty())
      if (!empty)
    } yield v
  }

  /**
   * Output a "ranking grid" of information so that a nice 3-D graph
   * can be created showing the ranks of cells surrounding the true
   * cell, out to a certain distance.
   * 
   * @param pred_cells List of predicted cells, along with their scores.
   * @param true_cell True cell.
   * @param grsize Total size of the ranking grid. (For example, a total size
   *   of 21 will result in a ranking grid with the true cell and 10
   *   cells on each side shown.)
   */
  def output_ranking_grid(pred_cells: Seq[(MultiRegularCell, Double)],
    true_cell: MultiRegularCell, grsize: Int) {
    val (true_latind, true_longind) =
      (true_cell.index.latind, true_cell.index.longind)
    val min_latind = true_latind - grsize / 2
    val max_latind = min_latind + grsize - 1
    val min_longind = true_longind - grsize / 2
    val max_longind = min_longind + grsize - 1
    val grid = mutable.Map[RegularCellIndex, (MultiRegularCell, Double, Int)]()
    for (((cell, value), rank) <- pred_cells zip (1 to pred_cells.length)) {
      val (la, lo) = (cell.index.latind, cell.index.longind)
      if (la >= min_latind && la <= max_latind &&
        lo >= min_longind && lo <= max_longind)
        grid(cell.index) = (cell, value, rank)
    }

    errprint("Grid ranking, gridsize %dx%d", grsize, grsize)
    errprint("NW corner: %s",
      multi_cell_index_to_nw_corner_coord(
        RegularCellIndex(max_latind, min_longind)))
    errprint("SE corner: %s",
      multi_cell_index_to_se_corner_coord(
        RegularCellIndex(min_latind, max_longind)))
    for (doit <- Seq(0, 1)) {
      if (doit == 0)
        errprint("Grid for ranking:")
      else
        errprint("Grid for goodness/distance:")
      for (lat <- max_latind to min_latind) {
        for (long <- fromto(min_longind, max_longind)) {
          val cellvalrank = grid.getOrElse(RegularCellIndex(lat, long), null)
          if (cellvalrank == null)
            errout(" %-8s", "empty")
          else {
            val (cell, value, rank) = cellvalrank
            val showit = if (doit == 0) rank else value
            if (lat == true_latind && long == true_longind)
              errout("!%-8.6s", showit)
            else
              errout(" %-8.6s", showit)
          }
        }
        errout("\n")
      }
    }
  }
}
