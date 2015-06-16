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
import util.error.assert_==
import util.experiment._
import util.io.localfh
import util.math._
import util.metering._
import util.numeric.pretty_double
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
implemented). This is independent of the slicing specified using '--slice';
rather, separate sets of DTM files are generated for each group. Never
really used.""")

  var grid_size = ap.option[Double]("grid-size", "s", "gs",
    default = 5.0,
    help = """Grid size in degrees for grouping documents using '--group'.""")

  var slice = ap.option[String]("slice",
    choices = Seq("year", "region", "latitude", "longitude"),
    default = "year",
    help = """How to create slices. Default is 'year' (time in years); also
'latitude', 'longitude' and 'region' (arbitrarily-shaped regions, as
specified by '--region-file' and '--region-list'). For 'year', 'latitude'
and 'longitude', see '--slice-size', '--min-slice-value',
'--max-slice-value', '--min-slice-count'.""")

  var region_file = ap.option[String]("region-file", "r", "rf",
    help = """File listing Civil War regions, in a specific GeoJSON format.""")

  var region_list = ap.option[String]("region-list", "rl",
    help = """List of regions to use, in order, separated by commas.""")

  var slice_size = ap.option[Double]("slice-size", "ss",
    default = 1.0,
    help = """Size of slices for grouping documents. Not applicable to
'--slice region'.""")

  var output_prefix = ap.option[String]("output-prefix", "o", "op",
    help = """Output prefix for DTM files. If omitted, no files are output,
but statistics are still printed.""")

  var preserve_case = ap.flag("preserve-case",
    help = """Preserve case of words when converting to DTM.""")

  var from_to_timeslice = ap.flag("from-to-timeslice", "fts",
    help = """If specified, output timeslices in FROM-TO format instead of numeric.""")

  var from_timeslice = ap.flag("from-timeslice", "fs",
    help = """If specified, output timeslices in FROM format instead of numeric.""")

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

  var min_slice_count = ap.option[Int]("min-slice-count", "msc",
    help = """Minimum document count in slice to keep it.""",
    default = 1)

  var min_slice_value = ap.option[Double]("min-slice-value", "minsv",
    help = """Minimum value of slice (year, latitude, longitude) to keep.""",
    default = Double.MinValue)

  var max_slice_value = ap.option[Double]("max-slice-value", "maxsv",
    help = """Maximum value of slice (year, latitude, longitude) to keep.""",
    default = Double.MaxValue)

  var random_sample = ap.option[Double]("random-sample", "rs",
    help = """Select a random sample of lines, of the specified percent.""",
    default = 100.0)
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
        case "" => {
          errprint("Blank date")
          None
        }
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

  lazy val region_to_polygon = read_json_polygons(params.region_file).map {
      case (origid, name, polygon) => (name, polygon)
    }.toMap
  lazy val indexed_region_list =
    params.region_list.split(",").toSeq.zipWithIndex
  lazy val regions = indexed_region_list.map {
      case (region, id) => (id, region, region_to_polygon(region))
    }
  lazy val id_to_region = indexed_region_list.map { case (x,y) => (y,x) }.toMap

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
    (date.toDouble / params.slice_size).toInt
  }

  // Convert coordinate to list of regions. We can have multiple regions
  // to allow e.g. for a circular specification of regions, with the same
  // region appearing at the beginning and the end.
  def coord_to_region_ids(coord: String) = {
    val Array(lat, long) = coord.split(",").map(_.toFloat)
    for ((id, name, polygon) <- regions;
        // Check the point, but also check slightly jittered points in
        // each of four directions in case we're exactly on a line (in
        // which case we might get a false value for the regions on both
        // sides of the line).
        if polygon.contains(new Point(long, lat)) ||
           polygon.contains(new Point(long + 0.00001f, lat)) ||
           polygon.contains(new Point(long - 0.00001f, lat)) ||
           polygon.contains(new Point(long, lat + 0.00001f)) ||
           polygon.contains(new Point(long, lat - 0.00001f)))
      yield (id, name)
  }

  def coord_to_slice_ids(coord: String) = {
    val Array(lat, long) = coord.split(",").map(_.toDouble)
    params.slice match {
      case "region" => {
        val region_ids = coord_to_region_ids(coord)
        // errprint("For coord %s, region_ids %s", coord, region_ids)
        region_ids.map { case (id, name) => id }
      }
      case "latitude" => Seq((lat / params.slice_size).toInt)
      case "longitude" => Seq((long / params.slice_size).toInt)
    }
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

  def process_file_for_min_word_count(file: String, stats: WordStats) {
    process_file(file, row => {
      val counts = row.gets("unigram-counts")
      var filtered_row_counts = 0
      for ((word, count) <- words_and_counts(counts, false, stats)) {
        filtered_row_counts += count
        corpus_word_counts(word) += count
      }
      if (filtered_row_counts > 0)
        stats.num_docs_post_filtering += 1
    })
  }

  def process_file_for_slices(file: String) = {
    var sampled_lines = 0
    def process_row(row: Row) {
      val title = row.gets("title")
      val coord = row.gets("coord")
      val date: Option[DMYDate] = DMYDate.parse(row.gets("date"))
      val counts = row.gets("unigram-counts")
      if (counts.size > 0) {
        val slice_ids =
          if (params.slice == "year")
            date.map(x => date_to_time_slice(x)).toSeq
          else if (coord != "") coord_to_slice_ids(coord)
          else Seq[Int]()
        slice_ids.foreach { x =>
          slice_counts(coord_to_cell(coord))(x) +=
            Document(title, coord, date, counts)
        }
        sampled_lines += 1
      } else {
         errprint("Skipped document %s, %s, %s because empty counts",
           title, coord, date)
       }
    }

    if (params.random_sample < 100.0) {
      val rows = mutable.Buffer[Row]()
      process_file(file, row => {
        rows += row
      })
      val rand = new scala.util.Random()
      rand.setSeed(2349876134L)
      rand.shuffle(rows).take(
        (rows.size * (params.random_sample / 100.0)).toInt).
        foreach(process_row)
    } else
      process_file(file, process_row)

    sampled_lines
  }

  class WordStats {
    var raw_numtokens = 0
    val raw_types = mutable.Set[String]()
    var canon_numtokens = 0
    val canon_types = mutable.Set[String]()
    var stopped_numtokens = 0
    val stopped_types = mutable.Set[String]()
    var lc_numtokens = 0
    val lc_types = mutable.Set[String]()
    var filtered_numtokens = 0
    val filtered_types = mutable.Set[String]()
    var num_docs_pre_filtering = 0
    var num_docs_post_filtering = 0

    def print(prefix: String) {
      errprint("%sRaw number of tokens: %s", prefix, raw_numtokens)
      errprint("%sRaw number of types: %s", prefix, raw_types.size)
      errprint("%sCanonicalized number of tokens: %s", prefix, canon_numtokens)
      errprint("%sCanonicalized number of types: %s", prefix, canon_types.size)
      errprint("%sStopped number of tokens: %s", prefix, stopped_numtokens)
      errprint("%sStopped number of types: %s", prefix, stopped_types.size)
      errprint("%sLowercase number of tokens: %s", prefix, lc_numtokens)
      errprint("%sLowercase number of types: %s", prefix, lc_types.size)
      errprint("%sFiltered number of tokens: %s", prefix, filtered_numtokens)
      errprint("%sFiltered number of types: %s", prefix, filtered_types.size)
      errprint("%sNumber of documents pre-filtering: %s", prefix,
        num_docs_pre_filtering)
      errprint("%sNumber of documents post-filtering: %s", prefix,
        num_docs_post_filtering)
    }

    def add(y: WordStats) {
      raw_numtokens += y.raw_numtokens
      raw_types ++= y.raw_types
      canon_numtokens += y.canon_numtokens
      canon_types ++= y.canon_types
      stopped_numtokens += y.stopped_numtokens
      stopped_types ++= y.stopped_types
      lc_numtokens += y.lc_numtokens
      lc_types ++= y.lc_types
      filtered_numtokens += y.filtered_numtokens
      filtered_types ++= y.filtered_types
      num_docs_pre_filtering += y.num_docs_pre_filtering
      num_docs_post_filtering += y.num_docs_post_filtering
    }
  }

  /**
   * Yield words and counts in the given count map, canonicalizing the
   * words in various ways, filtering stopwords, optionally lowercasing
   * and optionally filtering words below the minimum count. The same
   * word may be yielded more than once.
   */
  def words_and_counts(countstr: String, filter_min: Boolean,
      stats: WordStats) = {
    val counts = Decoder.count_map_seq(countstr)
    stats.num_docs_pre_filtering += 1
    for ((rawword0, count) <- counts;
         rawword1 = { stats.raw_numtokens += count;
                     stats.raw_types += rawword0; rawword0 };
         // Do some processing on the words to handle most mistakes in
         // word splitting
         canonword = rawword1.replace(".-", " ").
           replaceAll("[^-a-zA-Z0-9,$']", " ").
           replaceAll("([^0-9]),([^0-9])", "$1 $2").
           replaceAll("""\$([^0-9]),([^0-9])""", "$1 $2").
           replaceAll("-+", "-").
           replaceAll("[-,$]$", "").
           replaceAll("'s$", ""). // Remove possessive 's
           trim;
         word0 <- canonword.split(" +");
         if word0.size > 1;
         word1 = { stats.canon_numtokens += count;
                   stats.canon_types += word0; word0 };
         if the_stopwords.size == 0 || !the_stopwords(word1.toLowerCase);
         word2 = { stats.stopped_numtokens += count;
                   stats.stopped_types += word1; word1 };
         nocaseword0 = if (params.preserve_case) word2 else word2.toLowerCase;
         nocaseword1 = { stats.lc_numtokens += count;
                         stats.lc_types += nocaseword0; nocaseword0 };
         if (!filter_min || params.min_word_count <= 1 ||
           corpus_word_counts(nocaseword1) >= params.min_word_count);
         nocaseword2 = { stats.filtered_numtokens += count;
                         stats.filtered_types += nocaseword1; nocaseword1 }
        ) yield (nocaseword2, count)
  }

  def counts_to_processed_countmap(countstr: String, stats: WordStats) = {
    val processed_counts = intmap[String]()
    for ((word, count) <- words_and_counts(countstr, true, stats))
      processed_counts(word) += count
    val numtypes = processed_counts.size
    if (numtypes == 0) {
      errprint("Skipped document with words [%s] because no tokens after stopwording/filtering",
        countstr)
      None
    } else {
      stats.num_docs_post_filtering += 1
      Some(processed_counts)
    }
  }

  def countmap_to_lda(memoizer: StringGramAsIntMemoizer,
      counts: collection.Map[String, Int]) = {
    val ldastr =
      counts.toSeq.sortBy(-_._2).map { case (word, count) =>
        "%s:%s" format (memoizer.to_index(word), count)
      }.mkString(" ")
    val numtypes = counts.size
    assert(numtypes > 0)
    "%s %s" format (numtypes, ldastr)
  }

  def run_program(args: Array[String]) = {
    if (params.min_word_count > 1) {
      val stats = new WordStats
      for (file <- params.input)
        process_file_for_min_word_count(file, stats)
      errprint("Statistics on full corpus:")
      stats.print("  ")
    }
    var sampled_lines = 0
    for (file <- params.input)
      sampled_lines += process_file_for_slices(file)
    errprint("Number of random-sampled lines in full corpus: %s",
      sampled_lines)
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

      // Keep track of the slice counts for slices we keep, then write
      // them out at the end to the sequence file. We do that because we
      // need to write out the count of slices before the individual slice
      // counts, and we don't know how many till after we've processed
      // all the slices.
      val slice_counts = mutable.Buffer[Int]()
      val sorted_slices = slice_map.toSeq.sortBy(_._1)
      val memoizer = new StringGramAsIntMemoizer
      if (params.latex) {
        outprint("""\begin{tabular}{%s|c|c|c|}
\hline
%sFrom & To & #Docs \\
\hline""",
          if (params.slice != "year") "|c" else "",
          if (params.slice != "year") "Region & " else ""
        )
      }
      // Stats on all slices
      val all_slice_stats = new WordStats
      // Stats on slices actually sent to LDA
      val lda_slice_stats = new WordStats
      var num_lda_docs = 0
      for ((sliceindex, docs) <- sorted_slices) {
        var this_slice_stats = new WordStats
        import DMYDate.ordering
        val mindate = docs.flatMap(_.date).min
        val maxdate = docs.flatMap(_.date).max
        val slice_value = sliceindex * params.slice_size
        val slice_name =
          if (params.slice == "region")
            id_to_region(sliceindex)
          else if (params.slice == "year") {
            if (params.from_to_timeslice)
              "%s-%s" format (mindate.toMDY(true), maxdate.toMDY(true))
            else if (params.from_timeslice)
              "%s" format mindate.toMDY(true)
            else
              pretty_double(slice_value)
          } else
            pretty_double(slice_value)
        var docs_countmap =
          for (doc <- docs;
               // Skip documents with no words after filtering for stopwords
               // and --min-word-count
               countmap <- counts_to_processed_countmap(doc.counts,
                 this_slice_stats))
            yield (doc, countmap)
        all_slice_stats.add(this_slice_stats)
        if (docs_countmap.size < params.min_slice_count) {
          errprint("Skipped because count %s < %s: cell %s, slice %s",
            docs_countmap.size, params.min_slice_count, cell, slice_name)
        } else if (slice_value < params.min_slice_value) {
          errprint("Skipped because value %s < min value %s (count %s): cell %s slice %s",
            slice_value, params.min_slice_value, docs_countmap.size, cell,
            slice_name)
        } else if (slice_value > params.max_slice_value) {
          errprint("Skipped because value %s > max value %s (count %s): cell %s slice %s",
            slice_value, params.max_slice_value, docs_countmap.size, cell,
            slice_name)
        } else {
          tsfile.foreach(_.println(slice_name))
          slice_counts += docs_countmap.size
          num_lda_docs += docs_countmap.size
          lda_slice_stats.add(this_slice_stats)
          for ((doc, countmap) <- docs_countmap) {
            multfile.foreach(_.println(countmap_to_lda(memoizer, countmap)))
            docfile.foreach(_.println("%s\t%s\t%s\t%s" format (doc.title, doc.coord,
              doc.date.map(_.toMDY()).getOrElse(""), doc.counts)))
          }
          if (params.latex) {
            outprint("""%s%s & %s & %s \\""",
              if (params.slice != "year") slice_name + """\dgr & """ else "",
              mindate.toMDY(true), maxdate.toMDY(true), docs_countmap.size
            )
          } else {
            errprint("For cell %s, processing slice %s, count = %s" format
              (cell, slice_name, docs_countmap.size))
          }
        }
      }
      errprint("Statistics on all slices in random-sampled portion of corpus (minus docs with unparsable dates), cell %s:", cell)
      all_slice_stats.print("  ")
      errprint("Statistics on slices sent to LDA in random-sampled portion of corpus (minus docs with unparsable dates), cell %s:", cell)
      lda_slice_stats.print("  ")
      errprint("Number of documents sent to LDA: %s", num_lda_docs)
      errprint("Number of slices sent to LDA: %s", slice_counts.size)
      errprint("Size of vocabulary sent to LDA: %s", memoizer.number_of_indices)
      for (i <- 0 to memoizer.maximum_index) {
        vocabfile.foreach(_.println(memoizer.to_raw(i)))
      }
      seqfile.foreach(_.println(slice_counts.size.toString))
      var all_slice_counts = 0
      for (count <- slice_counts) {
        all_slice_counts += count
        seqfile.foreach(_.println(count.toString))
      }
      assert_==(all_slice_counts, num_lda_docs)
      multfile.foreach(_.close())
      seqfile.foreach(_.close())
      vocabfile.foreach(_.close())
      docfile.foreach(_.close())
      tsfile.foreach(_.close())
      if (params.latex) {
        outprint("""\hline
\end{tabular}""")
      }
    }
    0
  }
}
