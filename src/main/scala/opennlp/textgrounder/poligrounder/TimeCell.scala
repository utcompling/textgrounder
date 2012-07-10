///////////////////////////////////////////////////////////////////////////////
//  TimeCell.scala
//
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
//  Copyright (C) 2012 Stephen Roller, The University of Texas at Austin
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

package opennlp.textgrounder.poligrounder

import math._

import collection.mutable

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.textutil.format_float
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.printutil.{errout, errprint}
import opennlp.textgrounder.util.experiment._

import opennlp.textgrounder.gridlocate._
import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._
import opennlp.textgrounder.worddist._
import opennlp.textgrounder.worddist.WordDist.memoizer._

/////////////////////////////////////////////////////////////////////////////
//                             Cells in a grid                             //
/////////////////////////////////////////////////////////////////////////////

class TimeCell(
  from: Long,
  to: Long,
  cell_grid: TimeCellGrid
) extends GeoCell[TimeCoord, TimeDocument](cell_grid) {
  /**
   * Return the boundary of the cell as a pair of coordinates, specifying the
   * beginning and end.
   */
  def get_boundary() = (from, to)

  def contains(time: TimeCoord) = from <= time.millis && time.millis < to

  def get_center_coord = TimeCoord((to + from)/2)

  def describe_location =
    "%s - %s" format (format_time(from), format_time(to))

  def describe_indices =
    "%s/%s" format (format_time(from), format_interval(to - from))
}

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
 * Class for a "grid" of time intervals.
 */
class TimeCellGrid(
  from_chunk: (Long, Long),
  to_chunk: (Long, Long),
  override val table: TimeDocumentTable
) extends CellGrid[TimeCoord, TimeDocument, TimeCell](table) {
  var total_num_cells = 2
  val from_cell = new TimeCell(from_chunk._1, from_chunk._2, this)
  val to_cell = new TimeCell(to_chunk._1, to_chunk._2, this)

  def find_best_cell_for_coord(coord: TimeCoord,
      create_non_recorded: Boolean) = {
    assert(!create_non_recorded)
    if (from_cell contains coord) {
      if (debug("cell"))
        errprint("Putting document with coord %s in before-chunk %s", coord, from_chunk)
      from_cell
    } else if (to_cell contains coord) {
      if (debug("cell"))
        errprint("Putting document with coord %s in after-chunk %s", coord, to_chunk)
      to_cell
    } else {
      if (debug("cell"))
        errprint("Skipping document with coord %s because not in either before-chunk %s or after-chunk %s", coord, from_chunk, to_chunk)
      null
    }
  }

  def add_document_to_cell(doc: TimeDocument) {
    val cell = find_best_cell_for_coord(doc.coord, false)
    if (cell != null)
      cell.add_document(doc)
  }

  def iter_nonempty_cells(nonempty_word_dist: Boolean = false) = {
    for {
      v <- List(from_cell, to_cell)
      val empty = (
        if (nonempty_word_dist) v.combined_dist.is_empty_for_word_dist()
        else v.combined_dist.is_empty())
      if (!empty)
    } yield v
  }

  def initialize_cells() {
    from_cell.finish()
    to_cell.finish()
    /* FIXME!!!
      1. Should this be is_empty or is_empty_for_word_dist?  Do we even need
         this distinction?
      2. Computation of num_non_empty_cells should happen automatically!
    */
    if (!from_cell.combined_dist.is_empty_for_word_dist)
      num_non_empty_cells += 1
    if (!to_cell.combined_dist.is_empty_for_word_dist)
      num_non_empty_cells += 1
    for ((cell, name) <- Seq((from_cell, "from"), (to_cell, "to"))) {
      val comdist = cell.combined_dist
      errprint("Number of documents in %s-chunk: %s", name,
        comdist.num_docs_for_word_dist)
      errprint("Number of types in %s-chunk: %s", name,
        comdist.word_dist.num_word_types)
      errprint("Number of tokens in %s-chunk: %s", name,
        comdist.word_dist.num_word_tokens)
    }
  }
}

abstract class DistributionComparer(min_prob: Double, max_items: Int) {
  type Item
  type Dist

  def get_dist(cell: TimeCell): Dist =
    cell.combined_dist.word_dist.asInstanceOf[Dist]
  def get_keys(dist: Dist): collection.Set[Item]
  def lookup_item(dist: Dist, item: Item): Double
  def format_item(item: Item): String

  def compare_cells(grid: TimeCellGrid) {
    val fromdist = get_dist(grid.from_cell)
    val todist = get_dist(grid.to_cell)

    val itemdiff =
      for {
        item <- get_keys(fromdist) ++ get_keys(todist)
        p = lookup_item(fromdist, item)
        q = lookup_item(todist, item)
        if p >= min_prob || q >= min_prob
      } yield (item, p - q)
    val diff_up = itemdiff filter (_._2 > 0)
    val diff_down = itemdiff filter (_._2 < 0) map (x => (x._1, x._2.abs))
    def print_diffs(diffs: Iterable[(Item, Double)],
        incdec: String, updown: String) {
      println("")
      println("Items that %s in probability:" format incdec)
      println("------------------------------------")
      for ((item, prob) <- diffs.toSeq.sortWith(_._2 > _._2).take(max_items)) {
        println("%s: %s - %s = %s%s" format
          (format_item(item),
           format_float(lookup_item(fromdist, item)),
           format_float(lookup_item(todist, item)),
           updown, format_float(prob)))
      }
      println("")
    }
    print_diffs(diff_up, "increased", "+")
    print_diffs(diff_down, "decreased", "-")
  }
}

class UnigramComparer(min_prob: Double, max_items: Int) extends
    DistributionComparer(min_prob, max_items) {
  type Item = Word
  type Dist = UnigramWordDist

  def get_keys(dist: Dist) = dist.counts.keySet
  def lookup_item(dist: Dist, item: Item) = dist.lookup_word(item)
  def format_item(item: Item) = unmemoize_string(item)
}

class NgramComparer(min_prob: Double, max_items: Int) extends
    DistributionComparer(min_prob, max_items) {
  import NgramStorage.Ngram
  type Item = Ngram
  type Dist = NgramWordDist

  def get_keys(dist: Dist) = dist.model.iter_ngrams.map(_._1).toSet
  def lookup_item(dist: Dist, item: Item) = dist.lookup_ngram(item)
  def format_item(item: Item) = item mkString " "
}

object DistributionComparer {
  def compare_cells(grid: TimeCellGrid, min_prob: Double, max_items: Int) {
    grid.from_cell.combined_dist.word_dist match {
      case _: UnigramWordDist =>
        new UnigramComparer(min_prob, max_items).compare_cells(grid)
      case _: NgramWordDist =>
        new NgramComparer(min_prob, max_items).compare_cells(grid)
      case _ => throw new IllegalArgumentException("Don't know how to compare this type of word distribution")
    }
  }
}

