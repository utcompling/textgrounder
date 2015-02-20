///////////////////////////////////////////////////////////////////////////////
//  TimeCell.scala
//
//  Copyright (C) 2011-2014 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package poligrounder

import math._

import collection.mutable

import util.collection._
import util.numeric.format_double
import util.time._
import util.print.{errout, errprint}
import util.experiment._

import gridlocate._
import util.debug._
import langmodel._

/////////////////////////////////////////////////////////////////////////////
//                             Cells in a grid                             //
/////////////////////////////////////////////////////////////////////////////

class TimeCell(
  from: Long,
  to: Long,
  grid: TimeGrid
) extends GridCell[TimeCoord](grid) {
  /**
   * Return the boundary of the cell as a pair of coordinates, specifying the
   * beginning and end.
   */
  def get_boundary = (from, to)

  def contains(time: TimeCoord) = from <= time.millis && time.millis < to

  def get_true_center = TimeCoord((to + from)/2)

  // FIXME!!!!
  def get_centroid = TimeCoord((to + from)/2)

  def format_location =
    "%s - %s" format (format_time(from), format_time(to))

  def format_indices =
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
  val grid: TimeGrid
) {
  val before_cell = new TimeCell(before_chunk._1, before_chunk._2, grid)
  val after_cell = new TimeCell(after_chunk._1, after_chunk._2, grid)

  def find_best_cell_for_coord(coord: TimeCoord) = {
    if (before_cell contains coord) {
      if (debug("cell"))
        errprint("Putting document with coord %s in category %s, before-chunk %s",
          category,  coord, before_chunk)
      Some(before_cell)
    } else if (after_cell contains coord) {
      if (debug("cell"))
        errprint("Putting document with coord %s in category %s, after-chunk %s",
          category, coord, after_chunk)
      Some(after_cell)
    } else {
      if (debug("cell"))
        errprint("Skipping document with category %s, coord %s because not in either before-chunk %s or after-chunk %s",
          category, coord, before_chunk, after_chunk)
      None
    }
  }
}

/**
 * Class for a "grid" of time intervals. We actually have a set of named
 * categories and for each category we have two cells, corresponding to
 * two intervals (before and after).
 */
class TimeGrid(
  before_chunk: (Long, Long),
  after_chunk: (Long, Long),
  categories: IndexedSeq[String],
  category_of_doc: TimeDoc => String,
  override val docfact: TimeDocFactory,
  id: String
) extends Grid[TimeCoord](docfact, id) {
  def short_type = "time"

  val pairs = categories.map {
    x => (x, new TimeCellPair(x, before_chunk, after_chunk, this))
  }.toMap
  var total_num_cells = 2 * categories.length

  // FIXME!
  def create_subdivided_grid(create_docfact: => GridDocFactory[TimeCoord],
    id: String) = ???

  // FIXME!
  def imp_get_subdivided_cells(cell: GridCell[TimeCoord]) = ???

  def find_best_cell_for_coord(coord: TimeCoord,
      create_non_recorded: Boolean) = ???

  override def find_best_cell_for_document(doc: GridDoc[TimeCoord],
      create_non_recorded: Boolean) = {
    // Need to locate the cell in the document's category.
    assert(!create_non_recorded)
    val category = category_of_doc(doc.asInstanceOf[TimeDoc])
    if (category != null)
      pairs(category).find_best_cell_for_coord(doc.coord)
    else {
      if (debug("cell"))
        errprint("Skipping document %s because not in any category", doc)
      None
    }
  }

  override def add_salient_point(coord: TimeCoord, name: String,
      salience: Double) {
    // Need to add point to cells for all categories.
    pairs.map { case (cat, pair) =>
      pair.find_best_cell_for_coord(coord) map { cell =>
        cell.add_salient_point(name, salience)
      }
    }
  }

  def add_training_documents_to_grid(
      get_rawdocs: String => Iterator[DocStatus[RawDoc]]) {
    default_add_training_documents_to_grid(get_rawdocs, doc =>
      find_best_cell_for_document(doc,
        create_non_recorded = false) foreach (_.add_document(doc))
    )
  }

  def imp_iter_nonempty_cells = {
    for {
      category <- categories
      pair = pairs(category)
      v <- List(pair.before_cell, pair.after_cell)
      if (!v.is_empty)
    } yield v
  }

  def initialize_cells() {
    for (category <- categories) {
      val pair = pairs(category)
      pair.before_cell.finish()
      pair.after_cell.finish()
      for ((cell, name) <-
          Seq((pair.before_cell, "before"), (pair.after_cell, "after"))) {
        errprint("Number of documents in %s-chunk: %s", name,
          cell.num_docs)
        errprint("Number of types in %s-chunk grid lm: %s", name,
          cell.grid_lm.num_types)
        errprint("Number of tokens in %s-chunk grid lm: %s", name,
          cell.grid_lm.num_tokens)
        errprint("Number of types in %s-chunk rerank lm: %s", name,
          cell.rerank_lm.num_types)
        errprint("Number of tokens in %s-chunk rerank lm: %s", name,
          cell.rerank_lm.num_tokens)
      }
    }
  }
}

class LangModelComparer(min_prob: Double, max_grams: Int) {
  def get_pair(grid: TimeGrid, category: String) =
    grid.pairs(category)
  def get_keys(lm: LangModel) = lm.iter_keys.toSet

  def compare_cells_2way(grid: TimeGrid, category: String) {
    /* FIXME: What about rerank_lm? */
    val before_lm = get_pair(grid, category).before_cell.grid_lm
    val after_lm = get_pair(grid, category).after_cell.grid_lm

    val gramdiff =
      for {
        rawgram <- get_keys(before_lm) ++ get_keys(after_lm)
        gram = rawgram
        p = before_lm.gram_prob(gram)
        q = after_lm.gram_prob(gram)
        if p >= min_prob || q >= min_prob
      } yield (gram, before_lm.dunning_log_likelihood_2x1(
        gram, after_lm), q - p)

    println("Grams by 2-way log-likelihood for category '%s':" format category)
    for ((gram, dunning, prob) <-
        gramdiff.toSeq.sortWith(_._2 > _._2).take(max_grams)) {
      println("%7s: %-20s (%8s, %8s = %8s - %8s)" format
        (format_double(dunning),
         before_lm.gram_to_string(gram),
         if (prob > 0) "increase" else "decrease", format_double(prob),
         format_double(before_lm.gram_prob(gram)),
         format_double(after_lm.gram_prob(gram))
       ))
    }
    println("")

    val diff_up = gramdiff filter (_._3 > 0)
    val diff_down = gramdiff filter (_._3 < 0) map (x => (x._1, x._2, x._3.abs))
    def print_diffs(diffs: Iterable[(Gram, Double, Double)],
        incdec: String, updown: String) {
      println("")
      println("Grams that %s in probability:" format incdec)
      println("------------------------------------")
      for ((gram, dunning, prob) <-
          diffs.toSeq.sortWith(_._3 > _._3).take(max_grams)) {
        println("%s: %s - %s = %s%s (LL %s)" format
          (before_lm.gram_to_string(gram),
           format_double(before_lm.gram_prob(gram)),
           format_double(after_lm.gram_prob(gram)),
           updown, format_double(prob),
           format_double(dunning)))
      }
      println("")
    }
    print_diffs(diff_up, "increased", "+")
    print_diffs(diff_down, "decreased", "-")

  }

  def compare_cells_4way(grid: TimeGrid, category1: String,
      category2: String) {
    /* FIXME: What about rerank_lm? */
    val before_lm_1 = get_pair(grid, category1).before_cell.grid_lm
    val after_lm_1 = get_pair(grid, category1).after_cell.grid_lm
    val before_lm_2 = get_pair(grid, category2).before_cell.grid_lm
    val after_lm_2 = get_pair(grid, category2).after_cell.grid_lm

    val cat13 = category1.slice(0,3)
    val cat23 = category2.slice(0,3)
    val cat18 = category1.slice(0,8)
    val cat28 = category2.slice(0,8)

    val gramdiff =
      for {
        rawgram <- get_keys(before_lm_1) ++ get_keys(after_lm_1) ++
          get_keys(before_lm_2) ++ get_keys(after_lm_2)
        gram = rawgram
        p1 = before_lm_1.gram_prob(gram)
        q1 = after_lm_1.gram_prob(gram)
        p2 = before_lm_2.gram_prob(gram)
        q2 = after_lm_2.gram_prob(gram)
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
      } yield (gram, before_lm_1.dunning_log_likelihood_2x2(
          gram, after_lm_1, before_lm_2, after_lm_2),
          p1, q1, p2, q2, abs1, abs2, pct1, pct2, change
        )

    println("%24s change %7s%% (+-%7.7s) / %7s%% (+-%7.7s)" format (
      "Grams by 4-way log-lhood:", cat13, cat18, cat23, cat28))
    def fmt(x: Double) = format_double(x, include_plus = true)
    for ((gram, dunning, p1, q1, p2, q2, abs1, abs2, pct1, pct2, change) <-
        gramdiff.toSeq.sortWith(_._2 > _._2).take(max_grams)) {
      println("%7s: %-15.15s %6s: %7s%% (%9s) / %7s%% (%9s)" format
        (format_double(dunning),
         before_lm_1.gram_to_string(gram),
         change,
         fmt(pct1), fmt(abs1),
         fmt(pct2), fmt(abs2)
       ))
    }
    println("")

    type GramDunProb =
      (Gram, Double, Double, Double, Double, Double, Double,
        Double, Double, Double, String)
    val diff_cat1 = gramdiff filter (_._11 == "+"+cat13)
    val diff_cat2 = gramdiff filter (_._11 == "+"+cat23)
    def print_diffs(diffs: Iterable[GramDunProb],
        category: String, updown: String) {
      def print_diffs_1(msg: String,
          comparefun: (GramDunProb, GramDunProb) => Boolean) {
        println("")
        println("%s leaning towards %8s:" format (msg, category))
        println("----------------------------------------------------------")
        for ((gram, dunning, p1, q1, p2, q2, abs1, abs2, pct1, pct2, change) <-
            diffs.toSeq.sortWith(comparefun).take(max_grams)) {
          println("%-15.15s = LL %7s (%%chg-diff %7s%% = %7s%% - %7s%%)" format
            (before_lm_1.gram_to_string(gram), format_double(dunning),
              fmt(pct1 - pct2), fmt(pct1), fmt(pct2)))
        }
      }
      print_diffs_1("Grams by 4-way log-lhood with difference", _._2 > _._2)
      // print_diffs_1("Grams with greatest difference", _._3 > _._3)
    }
    print_diffs(diff_cat1, category1, "+")
    print_diffs(diff_cat2, category2, "-")
    println("")
    compare_cells_2way(grid, category1)
    println("")
    compare_cells_2way(grid, category2)
  }
}

object LangModelComparer {
  def get_comparer(min_prob: Double, max_grams: Int) =
    new LangModelComparer(min_prob, max_grams)

  def compare_cells_2way(grid: TimeGrid, category: String, min_prob: Double,
      max_grams: Int) {
    val comparer = get_comparer(min_prob, max_grams)
    comparer.compare_cells_2way(grid, category)
  }

  def compare_cells_4way(grid: TimeGrid, category1: String,
      category2: String, min_prob: Double, max_grams: Int) {
    val comparer = get_comparer(min_prob, max_grams)
    comparer.compare_cells_4way(grid, category1, category2)
  }
}
