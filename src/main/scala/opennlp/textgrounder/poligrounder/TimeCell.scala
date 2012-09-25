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

/**
 * Pair of intervals for comparing before and after for a particular class of
 * tweets (e.g. those from liberal or conservative users).
 */
class TimeCellPair(
  val category: String,
  val before_chunk: (Long, Long),
  val after_chunk: (Long, Long),
  val grid: TimeCellGrid
) {
  val before_cell = new TimeCell(before_chunk._1, before_chunk._2, grid)
  val after_cell = new TimeCell(after_chunk._1, after_chunk._2, grid)

  def find_best_cell_for_coord(coord: TimeCoord) = {
    if (before_cell contains coord) {
      if (debug("cell"))
        errprint("Putting document with coord %s in category %s, before-chunk %s",
          category,  coord, before_chunk)
      before_cell
    } else if (after_cell contains coord) {
      if (debug("cell"))
        errprint("Putting document with coord %s in category %s, after-chunk %s",
          category, coord, after_chunk)
      after_cell
    } else {
      if (debug("cell"))
        errprint("Skipping document with category %s, coord %s because not in either before-chunk %s or after-chunk %s",
          category, coord, before_chunk, after_chunk)
      null
    }
  }
}

/**
 * Class for a "grid" of time intervals.
 */
class TimeCellGrid(
  before_chunk: (Long, Long),
  after_chunk: (Long, Long),
  categories: Seq[String],
  category_of_doc: TimeDocument => String,
  override val table: TimeDocumentTable
) extends CellGrid[TimeCoord, TimeDocument, TimeCell](table) {
  
  val pairs = categories.map {
    x => (x, new TimeCellPair(x, before_chunk, after_chunk, this))
  }.toMap
  var total_num_cells = 2 * categories.length

  def find_best_cell_for_document(doc: TimeDocument,
      create_non_recorded: Boolean) = {
    assert(!create_non_recorded)
    val category = category_of_doc(doc)
    if (category != null)
      pairs(category).find_best_cell_for_coord(doc.coord)
    else {
      if (debug("cell"))
        errprint("Skipping document %s because not in any category", doc)
      null
    }
  }

  def add_document_to_cell(doc: TimeDocument) {
    val cell = find_best_cell_for_document(doc, false)
    if (cell != null)
      cell.add_document(doc)
  }

  def iter_nonempty_cells(nonempty_word_dist: Boolean = false) = {
    for {
      category <- categories
      pair = pairs(category)
      v <- List(pair.before_cell, pair.after_cell)
      val empty = (
        if (nonempty_word_dist) v.combined_dist.is_empty_for_word_dist()
        else v.combined_dist.is_empty())
      if (!empty)
    } yield v
  }

  def initialize_cells() {
    for (category <- categories) {
      val pair = pairs(category)
      pair.before_cell.finish()
      pair.after_cell.finish()
      /* FIXME!!!
        1. Should this be is_empty or is_empty_for_word_dist?  Do we even need
           this distinction?
        2. Computation of num_non_empty_cells should happen automatically!
      */
      if (!pair.before_cell.combined_dist.is_empty_for_word_dist)
        num_non_empty_cells += 1
      if (!pair.after_cell.combined_dist.is_empty_for_word_dist)
        num_non_empty_cells += 1
      for ((cell, name) <-
          Seq((pair.before_cell, "before"), (pair.after_cell, "after"))) {
        val comdist = cell.combined_dist
        errprint("Number of documents in %s-chunk: %s", name,
          comdist.num_docs_for_word_dist)
        errprint("Number of types in %s-chunk: %s", name,
          comdist.word_dist.model.num_types)
        errprint("Number of tokens in %s-chunk: %s", name,
          comdist.word_dist.model.num_tokens)
      }
    }
  }
}

abstract class DistributionComparer(min_prob: Double, max_items: Int) {
  type Item
  type Dist <: WordDist

  def get_pair(grid: TimeCellGrid, category: String) =
    grid.pairs(category)
  def get_dist(cell: TimeCell): Dist =
    cell.combined_dist.word_dist.asInstanceOf[Dist]
  def get_keys(dist: Dist) = dist.model.iter_keys.toSet
  def lookup_item(dist: Dist, item: Item): Double
  def format_item(item: Item): String

  def compare_cells_2way(grid: TimeCellGrid, category: String) {
    val before_dist = get_dist(get_pair(grid, category).before_cell)
    val after_dist = get_dist(get_pair(grid, category).after_cell)

    val itemdiff =
      for {
        rawitem <- get_keys(before_dist) ++ get_keys(after_dist)
        item = rawitem.asInstanceOf[Item]
        p = lookup_item(before_dist, item)
        q = lookup_item(after_dist, item)
        if p >= min_prob || q >= min_prob
      } yield (item, before_dist.dunning_log_likelihood_2x1(item.asInstanceOf[before_dist.Item], after_dist), q - p)

    println("Items by 2-way log-likelihood for category '%s':" format category)
    for ((item, dunning, prob) <-
        itemdiff.toSeq.sortWith(_._2 > _._2).take(max_items)) {
      println("%7s: %-20s (%8s, %8s = %8s - %8s)" format
        (format_float(dunning),
         format_item(item),
         if (prob > 0) "increase" else "decrease", format_float(prob),
         format_float(lookup_item(before_dist, item)),
         format_float(lookup_item(after_dist, item))
       ))
    }
    println("")

    val diff_up = itemdiff filter (_._3 > 0)
    val diff_down = itemdiff filter (_._3 < 0) map (x => (x._1, x._2, x._3.abs))
    def print_diffs(diffs: Iterable[(Item, Double, Double)],
        incdec: String, updown: String) {
      println("")
      println("Items that %s in probability:" format incdec)
      println("------------------------------------")
      for ((item, dunning, prob) <-
          diffs.toSeq.sortWith(_._3 > _._3).take(max_items)) {
        println("%s: %s - %s = %s%s (LL %s)" format
          (format_item(item),
           format_float(lookup_item(before_dist, item)),
           format_float(lookup_item(after_dist, item)),
           updown, format_float(prob),
           format_float(dunning)))
      }
      println("")
    }
    print_diffs(diff_up, "increased", "+")
    print_diffs(diff_down, "decreased", "-")

  }

  def compare_cells_4way(grid: TimeCellGrid, category1: String,
      category2: String) {
    val before_dist_1 = get_dist(get_pair(grid, category1).before_cell)
    val after_dist_1 = get_dist(get_pair(grid, category1).after_cell)
    val before_dist_2 = get_dist(get_pair(grid, category2).before_cell)
    val after_dist_2 = get_dist(get_pair(grid, category2).after_cell)

    val cat13 = category1.slice(0,3)
    val cat23 = category2.slice(0,3)
    val cat18 = category1.slice(0,8)
    val cat28 = category2.slice(0,8)

    val itemdiff =
      for {
        rawitem <- get_keys(before_dist_1) ++ get_keys(after_dist_1) ++
          get_keys(before_dist_2) ++ get_keys(after_dist_2)
        item = rawitem.asInstanceOf[Item]
        p1 = lookup_item(before_dist_1, item)
        q1 = lookup_item(after_dist_1, item)
        p2 = lookup_item(before_dist_2, item)
        q2 = lookup_item(after_dist_2, item)
        if p1 >= min_prob || q1 >= min_prob || p2 >= min_prob || q2 >= min_prob
        abs1 = q1 - p1
        abs2 = q2 - p2
        pct1 = (q1 - p1)/p1*100
        pct2 = (q2 - p2)/p2*100
        change = {
          if (pct1 > 0 && pct2 <= 0) "+"+cat13
          else if (pct1 <= 0 && pct2 > 0) "+"+cat23
          else if (pct1 < 0 && pct2 < 0) "-both"
          else "+both"
        }
      } yield (item, before_dist_1.dunning_log_likelihood_2x2(
          item.asInstanceOf[before_dist_1.Item],
          after_dist_1, before_dist_2, after_dist_2),
          p1, q1, p2, q2, abs1, abs2, pct1, pct2, change
        )

    println("%24s change %7s%% (+-%7.7s) / %7s%% (+-%7.7s)" format (
      "Items by 4-way log-lhood:", cat13, cat18, cat23, cat28))
    def fmt(x: Double) = format_float(x, include_plus = true)
    for ((item, dunning, p1, q1, p2, q2, abs1, abs2, pct1, pct2, change) <-
        itemdiff.toSeq.sortWith(_._2 > _._2).take(max_items)) {
      println("%7s: %-15.15s %6s: %7s%% (%9s) / %7s%% (%9s)" format
        (format_float(dunning),
         format_item(item),
         change,
         fmt(pct1), fmt(abs1),
         fmt(pct2), fmt(abs2)
       ))
    }
    println("")

    type ItemDunProb =
      (Item, Double, Double, Double, Double, Double, Double,
        Double, Double, Double, String)
    val diff_cat1 = itemdiff filter (_._11 == "+"+cat13)
    val diff_cat2 = itemdiff filter (_._11 == "+"+cat23)
    def print_diffs(diffs: Iterable[ItemDunProb],
        category: String, updown: String) {
      def print_diffs_1(msg: String,
          comparefun: (ItemDunProb, ItemDunProb) => Boolean) {
        println("")
        println("%s leaning towards %8s:" format (msg, category))
        println("----------------------------------------------------------")
        for ((item, dunning, p1, q1, p2, q2, abs1, abs2, pct1, pct2, change) <-
            diffs.toSeq.sortWith(comparefun).take(max_items)) {
          println("%-15.15s = LL %7s (%%chg-diff %7s%% = %7s%% - %7s%%)" format
            (format_item(item), format_float(dunning),
              fmt(pct1 - pct2), fmt(pct1), fmt(pct2)))
        }
      }
      print_diffs_1("Items by 4-way log-lhood with difference", _._2 > _._2)
      // print_diffs_1("Items with greatest difference", _._3 > _._3)
    }
    print_diffs(diff_cat1, category1, "+")
    print_diffs(diff_cat2, category2, "-")
    println("")
    compare_cells_2way(grid, category1)
    println("")
    compare_cells_2way(grid, category2)
  }
}

class UnigramComparer(min_prob: Double, max_items: Int) extends
    DistributionComparer(min_prob, max_items) {
  type Item = Word
  type Dist = UnigramWordDist

  def lookup_item(dist: Dist, item: Item) = dist.lookup_word(item)
  def format_item(item: Item) = unmemoize_string(item)
}

class NgramComparer(min_prob: Double, max_items: Int) extends
    DistributionComparer(min_prob, max_items) {
  import NgramStorage.Ngram
  type Item = Ngram
  type Dist = NgramWordDist

  def lookup_item(dist: Dist, item: Item) = dist.lookup_ngram(item)
  def format_item(item: Item) = item mkString " "
}

object DistributionComparer {
  def get_comparer(grid: TimeCellGrid, category: String, min_prob: Double,
      max_items: Int) =
    grid.pairs(category).before_cell.combined_dist.word_dist match {
      case _: UnigramWordDist =>
        new UnigramComparer(min_prob, max_items)
      case _: NgramWordDist =>
        new NgramComparer(min_prob, max_items)
      case _ => throw new IllegalArgumentException("Don't know how to compare this type of word distribution")
    }

  def compare_cells_2way(grid: TimeCellGrid, category: String, min_prob: Double,
      max_items: Int) {
    val comparer = get_comparer(grid, category, min_prob, max_items)
    comparer.compare_cells_2way(grid, category)
  }

  def compare_cells_4way(grid: TimeCellGrid, category1: String,
      category2: String, min_prob: Double, max_items: Int) {
    val comparer = get_comparer(grid, category1, min_prob, max_items)
    comparer.compare_cells_4way(grid, category1, category2)
  }
}

