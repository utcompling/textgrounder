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

import com.sromku.polygon._
import net.liftweb

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
according to the grid size given in `--grid-size`, or by country -- not
implemented.""")

  var grid_size = ap.option[Double]("grid-size", "s", "gs",
    default = 5.0,
    help = """Grid size in degrees for grouping documents.""")

  var region_slice = ap.flag("region-slice",
    help = """Create slices across regions rather than time.""")

  var region_file = ap.option[String]("region-file", "r", "rf",
    help = """File listing Civil War regions, in a specific GeoJSON format.""")

  var region_list = ap.option[String]("region-list", "rl",
    help = """List of regions to use, in order, separated by commas.""")

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

  var filter_regex = ap.option[String]("filter-regex", "fr",
    help = """Regex to use to filter text of input documents. Not anchored at
either end.""")

  var max_lines = ap.option[Int]("max-lines", "ml",
    help = """Max documents to process, for debugging.""")

  var min_word_count = ap.option[Int]("min-word-count", "mwc",
    help = """Minimum word count in corpus to keep word.""",
    default = 1)
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
 * 3. We create separate documents for points according to regions.
 *
 * We work as follows:
 *
 * 1. Read in all the paragraphs with their coordinates, from all files.
 * 2. For each coordinate, find the grid cell and slice.
 * 3. For each grid cell across slices, generate DTM data.
 */
object DatedCorpusToDTM extends ExperimentApp("DatedCorpusToDTM") {

  type TParam = DatedCorpusToDTMParameters

  def create_param_object(ap: ArgParser) = new DatedCorpusToDTMParameters(ap)

  case class Document(title: String, coord: String, date: Option[DMYDate], counts: String)

  def read_json_polygons(file: String) = {
    val contents = reflect.io.File(file).slurp
    val parsed = liftweb.json.parse(contents)
    val regions =
      (parsed \\ "features").values.asInstanceOf[List[Map[String, Any]]]
    for (region <- regions) yield {
      val props = region("properties").asInstanceOf[Map[String, Any]]
      val id = props("id").asInstanceOf[BigInt].toInt
      val name = props("name").asInstanceOf[String]
      val coords = region("geometry").
        asInstanceOf[Map[String,Any]]("coordinates").
        asInstanceOf[List[List[List[Double]]]]
      assert(coords.size == 1)
      val polygon = Polygon.Builder()
      for (List(long, lat) <- coords(0))
        polygon.addVertex(new Point(long.toFloat, lat.toFloat))
      (id, name, polygon.build())
    }
  }

  lazy val regions = read_json_polygons(params.region_file)
  lazy val region_list = params.region_list.split(",")
  lazy val region_to_id = region_list.zipWithIndex.toMap
  lazy val id_to_region = region_to_id.map { case (x,y) => (y,x) }

  // Map from cell ID's to slice indices (e.g. years) to sequences of documents.
  val slice_counts = bufmapmap[String, Int, Document]()

  val corpus_word_counts = intmap[String]()

  def coord_to_cell(coord: String): String = {
    val Array(lat, long) = coord.split(",").map(_.toDouble)
    params.group match {
      case "all" => "all"
      case "grid" => {
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

  def coord_to_region_id(coord: String): String = {
    val Array(lat, long) = coord.split(",").map(_.toFloat)
    for ((id, name, polygon) <- regions) {
      // Check the point, but also check slightly jittered points in
      // each of four directions in case we're exactly on a line (in
      // which case we might get a false value for the regions on both
      // sides of the line).
      if (polygon.contains(new Point(long, lat)) ||
          polygon.contains(new Point(long + 0.0001f, lat)) ||
          polygon.contains(new Point(long - 0.0001f, lat)) ||
          polygon.contains(new Point(long, lat + 0.0001f)) ||
          polygon.contains(new Point(long, lat - 0.0001f)))
        return name
    }
    return "unknown"
  }

  def coord_to_slice_id(coord: String) = {
    val region_id = coord_to_region_id(coord)
    // errprint("For coord %s, region_id %s", coord, region_id)
    region_to_id.get(region_id)
  }

  protected def read_stopwords = {
    if (params.stopwords_file == null)
      Set[String]()
    else
      localfh.openr(params.stopwords_file).toSet
  }

  lazy val the_stopwords = read_stopwords
  lazy val non_anchored_regex =
    if (params.filter_regex == null) null
    else "(?s).*" + params.filter_regex + ".*"

  def process_file(file: String, process_row: Row => Unit) {
    errprint("Processing %s ...", file)
    val task = new Meter("reading", "line")
    TextDB.read_textdb(localfh, file).zipWithIndex.
        foreachMetered(task) { case (row, index) =>
      if (params.max_lines > 0 && index >= params.max_lines)
        return
      // errprint(Decoder.string(row.gets("text")))
      if (non_anchored_regex == null || Decoder.string(row.gets("text")).
          matches(non_anchored_regex)) {
        process_row(row)
      }
    }
  }

  def process_file_for_min_word_count(file: String) {
    process_file(file, row => {
      val counts = row.gets("unigram-counts")
      for ((word, count) <- words_and_counts(counts, false))
        corpus_word_counts(word) += count
    })
  }

  def process_file_for_slices(file: String) {
    process_file(file, row => {
      val title = row.gets("title")
      val coord = row.gets("coord")
      val date: Option[DMYDate] = DMYDate.parse(row.gets("date"))
      val counts = row.gets("unigram-counts")
      if (counts.size > 0) {
        val slice_id: Option[Int] =
          if (params.region_slice) {
            if (coord != "") coord_to_slice_id(coord)
            else None
          } else
            date.map(x => date_to_time_slice(x))
        slice_id.foreach { x =>
          slice_counts(coord_to_cell(coord))(x) +=
            Document(title, coord, date, counts)
        }
      }
    })
  }

  /**
   * Yield words and counts in the given count map, canonicalizing the
   * words in various ways, filtering stopwords, optionally lowercasing
   * and optionally filtering words below the minimum count. The same
   * word may be yielded more than once.
   */
  def words_and_counts(countstr: String, filter_min: Boolean) = {
    val counts = Decoder.count_map_seq(countstr)
    for ((rawword, count) <- counts;
         // Do some processing on the words to handle most mistakes in
         // word splitting
         canonword = rawword.replace(".-", " ").
           replaceAll("[^-a-zA-Z0-9,$']", " ").
           replaceAll("([^0-9]),([^0-9])", "$1 $2").
           replaceAll("""\$([^0-9]),([^0-9])""", "$1 $2").
           replaceAll("-+", "-").
           replaceAll("[-,$]$", "").
           replaceAll("'s$", ""). // Remove possessive 's
           trim;
         word <- canonword.split(" +");
         if word.size > 1;
         if the_stopwords.size == 0 || !the_stopwords(word.toLowerCase);
         nocaseword = if (params.preserve_case) word else word.toLowerCase;
         if (!filter_min ||
           corpus_word_counts(nocaseword) >= params.min_word_count)
        ) yield (nocaseword, count)
  }

  def counts_to_lda(memoizer: StringGramAsIntMemoizer, countstr: String) = {
    val processed_counts = intmap[String]()
    for ((word, count) <- words_and_counts(countstr, true))
      processed_counts(word) += count
    val ldastr =
      processed_counts.toSeq.sortBy(-_._2).map { case (word, count) =>
        "%s:%s" format (memoizer.to_index(word), count)
      }.mkString(" ")
    val numtokens = processed_counts.size
    if (numtokens == 0) None
    else Some(processed_counts.size + " " + ldastr)
  }

  def run_program(args: Array[String]) = {
    for (file <- params.input)
      process_file_for_min_word_count(file)
    for (file <- params.input)
      process_file_for_slices(file)
    /* For each geographic slice, we output five files:
     *
     * 1. foo-mult.dat: DTM document file, listing documents, one per line,
     *    sorted by slice. The format of a line is
     *
     *    NUMTOKENS INDEX:COUNT ...
     *
     *    where NUMTOKENS is the total number of tokens in the document,
     *    and each INDEX:COUNT describes the count of a single word, with the
     *    word converted into a zero-based index.
     *
     * 2. foo-seq.dat: DTM sequence file, listing the number of documents in
     *    each successive lice. The first line is the number of
     *    lices, and each successive line is the number of documents in
     *    a given slice.
     *
     * 3. foo-vocab.dat: Listing of vocabulary, ordered by index.
     *
     * 4. foo-doc.dat: Description of each document in the DTM document file.
     *
     * 5. foo-slice.dat: Description of each slice.
     */
    for ((cell, slice_map) <- slice_counts) {
      val pref = Option(params.output_prefix)
      val multfile = pref.map(p => localfh.openw(p + "." + cell + "-mult.dat"))
      val seqfile = pref.map(p => localfh.openw(p + "." + cell + "-seq.dat"))
      val vocabfile =
        pref.map(p => localfh.openw(p + "." + cell + "-vocab.dat"))
      val docfile = pref.map(p => localfh.openw(p + "." + cell + "-doc.dat"))
      val tsfile =
        pref.map(p => localfh.openw(p + "." + cell + "-slice.dat"))

      val sorted_slices = slice_map.toSeq.sortBy(_._1)
      seqfile.foreach(_.println(sorted_slices.size.toString))
      val memoizer = new StringGramAsIntMemoizer
      if (params.latex) {
        errprint("""\begin{tabular}{%s|c|c|c|}
\hline
%sFrom & To & #Docs \\
\hline""",
          if (params.region_slice) "|c" else "",
          if (params.region_slice) "Region & " else ""
        )
      }
      for ((sliceindex, docs) <- sorted_slices) {
        seqfile.foreach(_.println(docs.size.toString))
        import DMYDate.ordering
        val mindate = docs.flatMap(_.date).min
        val maxdate = docs.flatMap(_.date).max
        val slice_name =
          if (params.region_slice)
            id_to_region(sliceindex)
          else if (params.from_to_timeslice)
            "%s-%s" format (mindate.toMDY(), maxdate.toMDY())
          else
            "%s" format mindate.toMDY()
        tsfile.foreach(_.println(slice_name))
        var doccount = 0
        for (doc <- docs;
             // Skip documents with no words after filtering for stopwords
             // and --min-word-count
             ldastr <- counts_to_lda(memoizer, doc.counts)) {
          doccount += 1
          multfile.foreach(_.println(ldastr))
          docfile.foreach(_.println("%s\t%s\t%s\t%s" format (doc.title, doc.coord,
            doc.date.map(_.toMDY()).getOrElse(""), doc.counts)))
        }
        if (params.latex) {
          errprint("""%s%s & %s & %s \\""",
            if (params.region_slice) slice_name + " & " else "",
            mindate.toMDY(true), maxdate.toMDY(true), doccount
          )
        } else {
          errprint("For cell %s, processing slice %s, count = %s" format
            (cell, slice_name, doccount))
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
