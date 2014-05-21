///////////////////////////////////////////////////////////////////////////////
//  MultiRegularCell.scala
//
//  Copyright (C) 2010-2014 Ben Wing, The University of Texas at Austin
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

import math._
import collection.mutable

import util.collection._
import util.debug._
import util.error._
import util.experiment._
import util.print.{errout, errprint}
import util.spherical._
import util.textdb.Row

import gridlocate.{DocStatus, GridDocFactory}

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
  "multi cell", which is used to compute a language model.  The
  number of tiling cells on a side of a multi cell is determined by
  --width-of-multi-cell.  Note that if this is greater than 1, different
  multi cells will overlap.

  To specify a cell, we use cell indices, which are derived from
  coordinates by dividing by degrees_per_cell.  Hence, if for example
  degrees_per_cell is 2.0, then cell indices are in the range [-45,+45]
  for latitude and [-90,+90) for longitude.  Correspondingly, to convert
  cell index to a SphereCoord, we multiply latitude and longitude by
  degrees_per_cell.

  In general, an arbitrary coordinate will have fractional cell indices;
  however, the cell indices of the corners of a cell (tiling or multi)
  will be integers.  Cells are canonically indexed and referred to by
  the index of the southwest corner.  In other words, for a given index,
  the latitude or longitude of the southwest corner of the corresponding
  cell (tiling or multi) is index*degrees_per_cell.  For a tiling cell,
  the cell includes all coordinates whose latitude or longitude is in
  the half-open interval [index*degrees_per_cell,
  (index+1)*degrees_per_cell).  For a multi cell, the cell includes all
  coordinates whose latitude or longitude is in the half-open interval
  [index*degrees_per_cell, (index+width_of_multi_cell)*degrees_per_cell).

  Near the edges, tiling cells may be truncated.  Multi cells will
  wrap around longitudinally, and will still have the same number of
  tiling cells, but may be smaller.
  */

/**
 * The index of a regular cell, using "cell index" integers, as described
 * above.
 *
 * Note that the constructor is marked as private, meaning that you cannot
 * create an index simply with a latitude and longitude. Instead, you must
 * either use the three-argument constructor (the `apply` function defined
 * in the companion class), which checks that the indices are valid, or
 * the `coerce` function, which forces the indices to be valid if they are
 * out of bounds (by wrapping or truncating).
 */
case class RegularCellIndex private (latind: Int, longind: Int) {
  def toFractional = FractionalRegularCellIndex(latind, longind)
}

object RegularCellIndex {
  /**
   * Construct a RegularCellIndex by coercing the given indices to be
   * in bounds if they aren't already. Latitude indices are truncated to
   * the maximum/minimum, and longitude indices are wrapped.
   */
  def apply(grid: MultiRegularGrid, latind: Int, longind: Int):
      RegularCellIndex = {
    //require(valid(grid, latind, longind),
    //  "Coordinate indices (%s,%s) invalid for grid %s"
    //    format (latind, longind, grid))
  /* SCALABUG: Why do I need to specify RegularCellIndex as the return type
     here?  And why is this not required in the almost identical construction
     in SphereCoord?  I get this error (not here, but where the object is
     created):

     [error] /Users/benwing/devel/textgrounder/src/main/scala/opennlp/textgrounder/geolocate/MultiRegularCell.scala:273: overloaded method apply needs result type
     [error]     RegularCellIndex(latind, longind)
     [error]     ^
     */
    if (!grid.initialized)
      new RegularCellIndex(latind, longind)
    else {
      val (newlatind, newlongind) = coerce_indices(grid, latind, longind)
      new RegularCellIndex(newlatind, newlongind)
    }
  }

  def valid(grid: MultiRegularGrid, latind: Int, longind: Int) = (
    latind >= grid.minimum_latind &&
    latind <= grid.maximum_latind &&
    longind >= grid.minimum_longind &&
    longind <= grid.maximum_longind
  )

  def coerce_indices(grid: MultiRegularGrid, latind: Int,
      longind: Int) = {
    var newlatind = latind
    var newlongind = longind
    if (newlatind > grid.maximum_latind)
      newlatind = grid.maximum_latind
    while (newlongind > grid.maximum_longind)
      newlongind -= (grid.maximum_longind - grid.minimum_longind + 1)
    if (newlatind < grid.minimum_latind)
      newlatind = grid.minimum_latind
    while (newlongind < grid.minimum_longind)
      newlongind += (grid.maximum_longind - grid.minimum_longind + 1)
    (newlatind, newlongind)
  }
}

/**
 * Similar to `RegularCellIndex`, but for the case where the indices are
 * fractional, representing a location other than at the corners of a
 * cell.
 */
case class FractionalRegularCellIndex(latind: Double, longind: Double) {
}

/**
 * A cell where the cell grid is a MultiRegularGrid. (See that class.)
 *
 * @param grid The Grid object for the grid this cell is in,
 *   an instance of MultiRegularGrid.
 * @param index Index of (the southwest corner of) this cell in the grid
 */

class MultiRegularCell(
  override val grid: MultiRegularGrid,
  val index: RegularCellIndex
) extends RectangularCell(grid) {

  def get_southwest_coord = grid.multi_cell_index_to_near_corner_coord(index)

  def get_northeast_coord = grid.multi_cell_index_to_far_corner_coord(index)

  def format_indices = "%s,%s" format (index.latind, index.longind)

  /**
   * For a given multi cell, iterate over the tiling cells in the multi cell.
   * The return values are the indices of the southwest corner of each
   * tiling cell.
   */
  def iter_tiling_cells = {
    // Be careful around the edges -- we need to truncate the latitude and
    // wrap the longitude.  The call to `RegularCellIndex()` will automatically
    // wrap the longitude, but we need to truncate the latitude ourselves,
    // or else we'll end up repeating cells.
    val max_offset = grid.width_of_multi_cell - 1
    val maxlatind = grid.maximum_latind min (index.latind + max_offset)

    for (
      i <- index.latind to maxlatind;
      j <- index.longind to (index.longind + max_offset)
    ) yield RegularCellIndex(grid, i, j)
  }
}

/**
 * Grid composed of possibly-overlapping multi cells, based on an underlying
 * grid of regularly-spaced square cells tiling the earth.  The multi cells,
 * over which language models are computed for comparison with the language
 * model of a given document, are composed of NxN tiles, where possibly
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
class MultiRegularGrid(
  val degrees_per_cell: Double,
  val cell_offset_degrees: SphereCoord,
  val width_of_multi_cell: Int,
  docfact: SphereDocFactory,
  id: String
) extends RealSphereGrid(docfact, id) {
  def short_type = "mreg"

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
  val modded_cod = SphereCoord(cell_offset_degrees.lat % degrees_per_cell,
    cell_offset_degrees.long % degrees_per_cell)
  // Bootstrapping to avoid problems creating a RegularCellIndex when
  // min/max indices not yet initialized
  var initialized = false
  val maximum_index =
    coord_to_tiling_cell_index(SphereCoord(maximum_latitude - 1e-10,
      maximum_longitude - 1e-10))
  val maximum_latind = maximum_index.latind
  val maximum_longind = maximum_index.longind
  val minimum_index =
    coord_to_tiling_cell_index(SphereCoord(minimum_latitude, minimum_longitude))
  val minimum_latind = minimum_index.latind
  val minimum_longind = minimum_index.longind
  initialized = true


  /**
   * Mapping from index of southwest corner of multi cell to corresponding
   * cell object.  A "multi cell" is made up of a square of tiling cells,
   * with the number of cells on a side determined by `width_of_multi_cell'.
   * A language model is associated with each multi cell.
   *
   * We don't just create an array because we expect many cells to have no
   * documents in them, esp. as we decrease the cell size.
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
    val jitter = 1e-8
    // Multiplying by the reciprocal seems to be more accurate than dividing.
    // For example, 175.0 / .14 = 1249.9999999999998 where the correct
    // answer is exactly 1250.0. If we directly call floor() we get 1249
    // instead of 1250, which screws up calculations in get_subdivided_cells(),
    // so that it may return 0 subcells in the case where the larger cell
    // being subdivided has only one document in it and that document's
    // coordinate falls exactly on the lower latitude or longitude boundary
    // of the cell. However, 175.0 * (1 / .14) = 1250.0, which works better.
    // Even if this doesn't always work and introduces floating-point errors
    // in some circumstances, we try to get around by adding a slight jitter
    // value.
    val recip = 1.0 / degrees_per_cell
    val latind = floor((coord.lat - modded_cod.lat) * recip + jitter).toInt
    val longind = floor((coord.long - modded_cod.long) * recip + jitter).toInt
    RegularCellIndex(this, latind, longind)
  }

  /**
   * Convert a coordinate to the indices of the southwest corner of the
   * corresponding multi cell.  Note that if `width_of_multi_cell` &gt; 1,
   * there will be more than one multi cell containing the coordinate.
   * In that case, we want the multi cell in which the coordinate is most
   * centered. (For example, if `width_of_multi_cell` = 3, then each multi
   * cell has 9 tiling cells in it, only one of which is in the center.
   * A given coordinate will belong to only one tiling cell, and we want
   * the multi cell which has that tiling cell in its center.)
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
    SphereCoord(index.latind * degrees_per_cell + modded_cod.lat,
      index.longind * degrees_per_cell + modded_cod.long, method)
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
      RegularCellIndex(this, index.latind + width_of_multi_cell,
        index.longind))
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * southeast corner of the cell.
   */
  def multi_cell_index_to_se_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(
      RegularCellIndex(this, index.latind,
        index.longind + width_of_multi_cell))
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

  def create_subdivided_grid(create_docfact: => GridDocFactory[SphereCoord],
      id: String) = {
    val new_docfact = create_docfact.asInstanceOf[SphereDocFactory]
    new MultiRegularGrid(degrees_per_cell /
        new_docfact.driver.params.subdivide_factor,
      cell_offset_degrees, width_of_multi_cell, new_docfact, id)
  }

  def get_subdivided_cells(cell: SphereCell) = {
    cell match {
      case rec: RectangularCell => {
        val jitter = 0.0001
        val sw = rec.get_southwest_coord
        val jitter_sw =
          SphereCoord(sw.lat + jitter, sw.long + jitter, "coerce")
        val sw_index = coord_to_multi_cell_index(jitter_sw)
        val ne = rec.get_northeast_coord
        // Need to coerce at least here or we will get errors when
        // the east edge is at 180 degrees because it shows up as
        // -180 and will get jittered to -180.0001, which is out of
        // bounds.
        val jitter_ne =
          SphereCoord(ne.lat - jitter, ne.long - jitter, "coerce")
        val ne_index = coord_to_multi_cell_index(jitter_ne)
        // Conceivably, the larger cell might wrap across 180 degrees,
        // meaning the NE longitude index will be less than the SW one.
        // We need to "unwrap" in that case so we are always iterating
        // upwards; we will re-wrap when RegularCellIndex() is called.
        var ne_index_longind = ne_index.longind
        while (ne_index_longind < sw_index.longind)
          ne_index_longind += (maximum_longind - minimum_longind + 1)
        assert_<=(sw_index.latind, ne_index.latind)
        val indices =
          for (
            i <- sw_index.latind to ne_index.latind;
            j <- sw_index.longind to ne_index_longind
          ) yield RegularCellIndex(this, i, j)
        // The southwest corner of the larger cell should map to one of
        // the subcells. Otherwise, if the larger cell contains only a
        // document whose coord lies on either the south or west border,
        // we will return no subcells, which is bad.
        assert(indices contains coord_to_multi_cell_index(sw),
          "Southwest coord %s with index %s not found in indices with range %s to %s" format (
            sw, coord_to_multi_cell_index(sw), sw_index, ne_index))
        val retval = indices.flatMap { index =>
          find_cell_for_cell_index(index, create = false,
            record_created_cell = false)
        }
        if (retval.size == 0) {
          errprint("Attempt to return no subcells")
          val centroid = cell.get_centroid
          errprint(s"For cell $cell at centroid $centroid")
          errprint("Indices are %s to %s", sw_index, ne_index)
          errprint(s"Centroid index ${coord_to_multi_cell_index(centroid)}")
          errprint(s"Centroid tiling index ${coord_to_tiling_cell_index(centroid)}")
          errprint("Centroid in cell %s",
            find_best_cell_for_coord(centroid, create_non_recorded = false))
          assert_>(retval.size, 0)
        }
        retval
      }
      case _ => ???
    }
  }

  /**
   * For a given coordinate, iterate over the multi cells containing the
   * coordinate.  This first finds the tiling cell containing the
   * coordinate and then finds the multi cells containing the tiling cell.
   * The returned values are the indices of the (southwest corner of the)
   * multi cells.
   */
  def iter_overlapping_multi_cells(coord: SphereCoord) = {
    // The logic is almost exactly the same as in iter_tiling_cells()
    // except that the offset is negative.
    val index = coord_to_tiling_cell_index(coord)
    // In order to handle coordinates near the edges of the grid, we need to
    // truncate the latitude ourselves, but RegularCellIndex() handles the
    // longitude wrapping.  See iter_tiling_cells().
    val max_offset = width_of_multi_cell - 1
    val minlatind = minimum_latind max (index.latind - max_offset)

    for (
      i <- minlatind to index.latind;
      j <- (index.longind - max_offset) to index.longind
    ) yield RegularCellIndex(this, i, j)
  }

  def find_best_cell_for_coord(coord: SphereCoord,
      create_non_recorded: Boolean) = {
    assert(all_cells_computed)
    val index = coord_to_multi_cell_index(coord)
    find_cell_for_cell_index(index, create = create_non_recorded,
      record_created_cell = false)
  }

  /**
   * For a given multi cell index, find the corresponding cell.
   * If no such cell exists, create one if `create` is true;
   * else, return None.  If a cell is created, record it in the
   * grid if `record_created_cell` is true.
   */
  def find_cell_for_cell_index(index: RegularCellIndex,
      create: Boolean, record_created_cell: Boolean) = {
    corner_to_multi_cell.get(index) match {
      case x@Some(cell) => x
      case None if !create => None
      case _ => {
        val newcell = new MultiRegularCell(this, index)
        if (record_created_cell) {
          corner_to_multi_cell(index) = newcell
        }
        Some(newcell)
      }
    }
  }

  def add_training_documents_to_grid(
      get_rawdocs: String => Iterator[DocStatus[Row]]) {
    default_add_training_documents_to_grid(get_rawdocs, doc =>
      for (index <- iter_overlapping_multi_cells(doc.coord)) {
        val cell = find_cell_for_cell_index(index, create = true,
          record_created_cell = true).get
        if (debug("cell"))
          errprint("Adding document %s to cell %s", doc, cell)
        cell.add_document(doc)
      }
    )
  }

  protected def initialize_cells() {
    val indices =
      for (i <- minimum_latind to maximum_latind;
           j <- minimum_longind to maximum_longind)
         yield RegularCellIndex(this, i, j)

    // This doesn't take much time so turn it off.
    // driver.show_progress("generating non-empty", "Earth-tiling cell").
    //  foreach(indices)
    indices.foreach { index =>
      total_num_cells += 1
      find_cell_for_cell_index(index, create = false,
        record_created_cell = false).foreach { cell =>
          cell.finish()
          if (debug("cell"))
            errprint("--> (%s,%s): %s", index.latind, index.longind, cell)
        }
    }
  }

  def iter_nonempty_cells = {
    assert(all_cells_computed)
    (for {
      v <- corner_to_multi_cell.values
      if (!v.is_empty)
    } yield v).toIndexedSeq
  }

  /**
   * Output a "ranking grid" of information so that a nice 3-D graph
   * can be created showing the ranks of cells surrounding the true
   * cell, out to a certain distance.
   *
   * @param pred_cells List of predicted cells, along with their scores.
   * @param correct_cell Correct cell.
   * @param grsize Total size of the ranking grid. (For example, a total size
   *   of 21 will result in a ranking grid with the correct cell and 10
   *   cells on each side shown.)
   */
  def output_ranking_grid(pred_cells: Iterable[(MultiRegularCell, Double)],
    correct_cell: MultiRegularCell, grsize: Int) {
    val (true_latind, true_longind) =
      (correct_cell.index.latind, correct_cell.index.longind)
    val min_latind = true_latind - grsize / 2
    val max_latind = min_latind + grsize - 1
    val min_longind = true_longind - grsize / 2
    val max_longind = min_longind + grsize - 1
    val grid = mutable.Map[RegularCellIndex, (MultiRegularCell, Double, Int)]()
    for (((cell, score), rank) <- pred_cells zip (1 to pred_cells.size)) {
      val (la, lo) = (cell.index.latind, cell.index.longind)
      if (la >= min_latind && la <= max_latind &&
        lo >= min_longind && lo <= max_longind)
        // FIXME: This assumes KL-divergence or similar scores, which have
        // been negated to make larger scores better.
        grid(cell.index) = (cell, -score, rank)
    }

    errprint("Grid ranking, gridsize %sx%s", grsize, grsize)
    errprint("NW corner: %s",
      multi_cell_index_to_nw_corner_coord(
        RegularCellIndex(this, max_latind, min_longind)))
    errprint("SE corner: %s",
      multi_cell_index_to_se_corner_coord(
        RegularCellIndex(this, min_latind, max_longind)))
    for (doit <- Seq(0, 1)) {
      if (doit == 0)
        errprint("Grid for ranking:")
      else
        errprint("Grid for goodness/distance:")
      for (lat <- max_latind to min_latind;
           long <- fromto(min_longind, max_longind)) {
        grid.get(RegularCellIndex(this, lat, long)) match {
          case None => errout(" %-8s", "empty")
          case Some((cell, value, rank)) => {
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
