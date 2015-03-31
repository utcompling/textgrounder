//  PCLCorpusToDTM.scala
//
//  Copyright (C) 2014 Ben Wing, The University of Texas at Austin
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
import util.print._
import util.table._
import util.textdb._

import util.debug._

import langmodel.StringGramAsIntMemoizer

/**
 * See description under `PCLCorpusToDTM`.
 */
class PCLCorpusToDTMParameters(ap: ArgParser) {
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

  var time_size = ap.option[Int]("time-size", "t", "ts",
    default = 10,
    help = """Size of time chunks in years for grouping documents.""")

  var output_prefix = ap.option[String]("output-prefix", "o", "op",
    must = be_specified,
    help = """Output prefix for DTM files.""")
}

/**
 * Convert a version of the PCL corpus in TextDB format into dynamic DTM
 * format. This can operate in three modes:
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
object PCLCorpusToDTM extends ExperimentApp("PCLCorpusToDTM") {

  type TParam = PCLCorpusToDTMParameters

  def create_param_object(ap: ArgParser) = new PCLCorpusToDTMParameters(ap)

  case class Document(title: String, coord: String, date: Int, counts: String)

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

  def date_to_time_slice(date: String) = {
    date.toInt / params.time_size
  }

  def process_file(file: String) {
    errprint("Processing %s ...", file)
    for (row <- TextDB.read_textdb(localfh, file)) {
      val title = row.gets("title")
      val coord = row.gets("coord")
      val date = row.gets("date")
      val counts = row.gets("unigram-counts")
      if (counts.size > 0)
        slice_counts(coord_to_cell(coord))(date_to_time_slice(date)) +=
          Document(title, coord, date.toInt, counts)
    }
  }

  def counts_to_lda(memoizer: StringGramAsIntMemoizer, countstr: String) = {
    val counts = Decoder.count_map_seq(countstr)
    val tokens = counts.map(_._2).sum
    val ldastr =
      counts.map { case (word, count) =>
        "%s:%s" format (memoizer.to_index(word), count)
      }.mkString(" ")
    tokens.toString + " " + ldastr
  }

  def run_program(args: Array[String]) = {
    for (file <- params.input)
      process_file(file)
    /* For each slice, we output five files:
     *
     * 1. foo.mult: DTM document file, listing documents, one per line,
     *    sorted by timeslice. The format of a line is
     *
     *    NUMTOKENS INDEX:COUNT ...
     *
     *    where NUMTOKENS is the total number of tokens in the document,
     *    and each INDEX:COUNT describes the count of a single word, with the
     *    word converted into a zero-based index.
     *
     * 2. foo.seq: DTM sequence file, listing the number of documents in
     *    each successive timeslice. The first line is the number of
     *    timeslices, and each successive line is the number of documents in
     *    a given timeslice.
     *
     * 3. foo.vocab: Listing of vocabulary, ordered by index.
     *
     * 4. foo.doc: Description of each document in the DTM document file.
     *
     * 5. foo.timeslice: Description of each timeslice.
     */
    for ((cell, timeslice_map) <- slice_counts) {
      val multfile = localfh.openw(params.output_prefix + "." + cell + ".mult")
      val seqfile = localfh.openw(params.output_prefix + "." + cell + ".seq")
      val vocabfile =
        localfh.openw(params.output_prefix + "." + cell + ".vocab")
      val docfile = localfh.openw(params.output_prefix + "." + cell + ".doc")
      val tsfile =
        localfh.openw(params.output_prefix + "." + cell + ".timeslice")

      val sorted_timeslices = timeslice_map.toSeq.sortBy(_._1)
      seqfile.println(sorted_timeslices.size.toString)
      val memoizer = new StringGramAsIntMemoizer
      for ((sliceindex, docs) <- sorted_timeslices) {
        seqfile.println(docs.size.toString)
        val mindate = docs.map(_.date).min
        val maxdate = docs.map(_.date).max
        tsfile.println("%s-%s" format (mindate, maxdate))
        for (doc <- docs) {
          multfile.println(counts_to_lda(memoizer, doc.counts))
          docfile.println("%s\t%s\t%s\t%s" format (doc.title, doc.coord,
            doc.date, doc.counts))
        }
      }
      for (i <- 0 to memoizer.maximum_index) {
        vocabfile.println(memoizer.to_raw(i))
      }
      multfile.close()
      seqfile.close()
      vocabfile.close()
      docfile.close()
      tsfile.close()
    }
    0
  }
}
