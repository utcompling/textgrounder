//  DatedCorpusToDTM.scala
//
//  Copyright (C) 2015 Ben Wing, The University of Texas at Austin
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
package preprocess

import collection.mutable

import util.argparser._
import util.collection._
import util.experiment._
import util.io.localfh
import util.math._
import util.metering._
import util.print._
import util.table._
import util.textdb._

import util.debug._

import langmodel.StringGramAsIntMemoizer

/**
 * See description under `DatedCorpusToDTM`.
 */
class DatedCorpusToDTMParameters(ap: ArgParser) {
  var input = ap.multiPositional[String]("input",
    help = """Files to analyze -- TextDB corpora. The values can be any of
the following: Either the data or schema file of the database; the common
prefix of the two; or the directory containing them, provided there is only
one textdb in the directory.""")

  var group = ap.option[String]("group", "g",
    choices = Seq("all", "grid", "country"),
    default = "all",
    help = """How to group documents by coordinate (all, by grid coordinate
according to the grid size given in `--grid-size`, or by country according to
the file in `--country-file`.""")

  var grid_size = ap.option[Double]("grid-size", "s", "gs",
    default = 5.0,
    help = """Grid size in degrees for grouping documents.""")

  var country_file = ap.option[String]("country-file", "c", "cf",
    help = """File containing mapping from coordinates to countries.""")

  var time_size = ap.option[Double]("time-size", "t", "ts",
    default = 10.0,
    help = """Size of time chunks in years for grouping documents.""")

  var output_prefix = ap.option[String]("output-prefix", "o", "op",
    help = """Output prefix for DTM files. If omitted, no files are output,
but statistics are still printed.""")

  var preserve_case = ap.flag("preserve-case",
    help = """Preserve case of words when converting to DTM.""")

  var from_to_timeslice = ap.flag("from-to-timeslice", "fts",
    help = """If specified, output timeslices in FROM-TO format instead of just FROM.""")

  var latex = ap.flag("latex",
    help = """Output stats in LaTeX format instead of raw human-readable.""")

  var stopwords_file = ap.option[String]("stopwords-file", "sf",
    help = """File containing stopwords.""")
}

case class DMYDate(year: Int, month: Int, day: Int) {
  def toMDY(short: Boolean = false) =
    if (short)
      "%02d/%02d/%d" format (month, day, year)
    else
      "%s %s, %s" format (DMYDate.index_month_map(month), day, year)
  def toDouble = {
    val cum_lengths =
      if (year % 4 == 0) DMYDate.cum_leap_month_lengths
      else DMYDate.cum_month_lengths
    val year_days = if (year % 4 == 0) 366 else 365
    val day_of_year = cum_lengths(month - 1) + day - 1
    year + day_of_year.toDouble / year_days
  }
}

object DMYDate {
  implicit val ordering = Ordering[(Int, Int, Int)].on((x:DMYDate) => (x.year, x.month, x.day))
  val months = Seq("january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december")
  val month_index_map = (months zip Stream.from(1)).toMap
  val index_month_map = month_index_map.map { case (x,y) => (y,x) }
  val month_lengths = Seq(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
  val cum_month_lengths = month_lengths.foldLeft(IndexedSeq[Int](0))(
    (sums, x) => sums :+ (sums.last + x))
  val leap_month_lengths = Seq(31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
  val cum_leap_month_lengths = leap_month_lengths.foldLeft(IndexedSeq[Int](0))(
    (sums, x) => sums :+ (sums.last + x))
  def parse(str: String) = {
    val mdyre = """^([A-Za-z]+) ([0-9]+), ([0-9]+)$""".r
    val myre = """^([A-Za-z]+),? ([0-9]+)$""".r
    val yre = """^([0-9]+)$""".r
    val res =
      str match {
        case mdyre(m, d, y) => Some(DMYDate(y.toInt, month_index_map(m.toLowerCase), d.toInt))
        case myre(m, y) => Some(DMYDate(y.toInt, month_index_map(m.toLowerCase), 1))
        case yre(y) => Some(DMYDate(y.toInt, 1, 1))
        case "" => None
        case _ => {
          errprint("Unable to parse date: %s" format str)
          None
        }
      }
    if (res != None && res.get.day > 31) {
      errprint("Day of month too large: %s" format str)
      None
    } else
      res
  }
}

/**
 * Convert a corpus in TextDB format with dates and coordinates into dynamic
 * DTM format. This can operate in three modes:
 *
 * 1. We group all paragraphs of all coordinates together into one document.
 * 2. We create separate documents for batches of points according to a grid,
 *    e.g. 5x5 degrees.
 * 3. We create separate documents for points according to country.
 *
 * We work as follows:
 *
 * 1. Read in all the paragraphs with their coordinates, from all files.
 * 2. For each coordinate, find the grid cell and time slice.
 * 3. For each grid cell across time slices, generate DTM data.
 */
object DatedCorpusToDTM extends ExperimentApp("DatedCorpusToDTM") {

  type TParam = DatedCorpusToDTMParameters

  def create_param_object(ap: ArgParser) = new DatedCorpusToDTMParameters(ap)

  case class Document(title: String, coord: String, date: DMYDate, counts: String)

  // Map from cell ID's to years to sequences of documents.
  val slice_counts = bufmapmap[String, Int, Document]()

  def coord_to_cell(coord: String) = {
    params.group match {
      case "all" => "all"
      case "grid" => {
        val Array(lat, long) = coord.split(",").map(_.toDouble)
        val roundlat = (lat / params.grid_size).toInt * params.grid_size
        val roundlong = (long / params.grid_size).toInt * params.grid_size
        "%s,%s" format (roundlat, roundlong)
      }
      case "country" => ???
    }
  }

  def date_to_time_slice(date: DMYDate) = {
    (date.toDouble / params.time_size).toInt
  }

  protected def read_stopwords = {
    if (params.stopwords_file == null)
      Set[String]()
    else
      localfh.openr(params.stopwords_file).toSet
  }

  lazy val the_stopwords = read_stopwords

  def process_file(file: String) {
    errprint("Processing %s ...", file)
    val task = new Meter("reading", "line")
    TextDB.read_textdb(localfh, file).foreachMetered(task) { row =>
      val title = row.gets("title")
      val coord = row.gets("coord")
      val date = DMYDate.parse(row.gets("date"))
      val counts = row.gets("unigram-counts")
      if (counts.size > 0 && date != None)
        slice_counts(coord_to_cell(coord))(date_to_time_slice(date.get)) +=
          Document(title, coord, date.get, counts)
    }
  }

  def counts_to_lda(memoizer: StringGramAsIntMemoizer, countstr: String) = {
    val counts = Decoder.count_map_seq(countstr)
    val tokens = counts.map(_._2).sum
    val processed_counts = intmap[String]()
    for ((word, count) <- counts) {
      if (the_stopwords.size == 0 || !the_stopwords(word.toLowerCase)) {
        if (params.preserve_case)
          processed_counts(word) += count
        else
          processed_counts(word.toLowerCase) += count
      }
    }
    val ldastr =
      processed_counts.toSeq.sortBy(-_._2).map { case (word, count) =>
        "%s:%s" format (memoizer.to_index(word), count)
      }.mkString(" ")
    processed_counts.size + " " + ldastr
  }

  def run_program(args: Array[String]) = {
    for (file <- params.input)
      process_file(file)
    /* For each geographic slice, we output five files:
     *
     * 1. foo-mult.dat: DTM document file, listing documents, one per line,
     *    sorted by timeslice. The format of a line is
     *
     *    NUMTOKENS INDEX:COUNT ...
     *
     *    where NUMTOKENS is the total number of tokens in the document,
     *    and each INDEX:COUNT describes the count of a single word, with the
     *    word converted into a zero-based index.
     *
     * 2. foo-seq.dat: DTM sequence file, listing the number of documents in
     *    each successive timeslice. The first line is the number of
     *    timeslices, and each successive line is the number of documents in
     *    a given timeslice.
     *
     * 3. foo-vocab.dat: Listing of vocabulary, ordered by index.
     *
     * 4. foo-doc.dat: Description of each document in the DTM document file.
     *
     * 5. foo-timeslice.dat: Description of each timeslice.
     */
    for ((cell, timeslice_map) <- slice_counts) {
      val pref = Option(params.output_prefix)
      val multfile = pref.map(p => localfh.openw(p + "." + cell + "-mult.dat"))
      val seqfile = pref.map(p => localfh.openw(p + "." + cell + "-seq.dat"))
      val vocabfile =
        pref.map(p => localfh.openw(p + "." + cell + "-vocab.dat"))
      val docfile = pref.map(p => localfh.openw(p + "." + cell + "-doc.dat"))
      val tsfile =
        pref.map(p => localfh.openw(p + "." + cell + "-timeslice.dat"))

      val sorted_timeslices = timeslice_map.toSeq.sortBy(_._1)
      seqfile.foreach(_.println(sorted_timeslices.size.toString))
      val memoizer = new StringGramAsIntMemoizer
      if (params.latex) {
        errprint("""\begin{tabular}{|c|c|c|}
\hline
From & To & #Docs \\
\hline""")
      }
      for ((sliceindex, docs) <- sorted_timeslices) {
        seqfile.foreach(_.println(docs.size.toString))
        import DMYDate.ordering
        val mindate = docs.map(_.date).min
        val maxdate = docs.map(_.date).max
        val tslice =
          if (params.from_to_timeslice)
            "%s-%s" format (mindate.toMDY(), maxdate.toMDY())
          else
            "%s" format mindate.toMDY()
        tsfile.foreach(_.println(tslice))
        if (params.latex) {
          errprint("""%s & %s & %s \\""" format (
            mindate.toMDY(true), maxdate.toMDY(true), docs.size)
          )
        } else {
          errprint("For cell %s, processing slice %s, count = %s" format
            (cell, tslice, docs.size))
        }
        for (doc <- docs) {
          multfile.foreach(_.println(counts_to_lda(memoizer, doc.counts)))
          docfile.foreach(_.println("%s\t%s\t%s\t%s" format (doc.title, doc.coord,
            doc.date.toMDY(), doc.counts)))
        }
      }
      for (i <- 0 to memoizer.maximum_index) {
        vocabfile.foreach(_.println(memoizer.to_raw(i)))
      }
      multfile.foreach(_.close())
      seqfile.foreach(_.close())
      vocabfile.foreach(_.close())
      docfile.foreach(_.close())
      tsfile.foreach(_.close())
      if (params.latex) {
        errprint("""\hline
\end{tabular}""")
      }
    }
    0
  }
}
