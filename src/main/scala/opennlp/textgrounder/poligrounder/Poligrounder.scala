///////////////////////////////////////////////////////////////////////////////
//  Poligrounder.scala
//
//  Copyright (C) 2010, 2011, 2012 Ben Wing, The University of Texas at Austin
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

/*

Basic idea:

1. We specify a corpus and times to compare, e.g.

poligrounder -i twitter-spritzer --from 201203051627/-1h --to 201203051801/3h

will operate on the twitter-spritzer corpus and compare the hour
directly preceding March 5, 2012, 4:27pm with the three hours directly
following March 5, 2012, 6:01pm.

Time can be specified either as simple absolute times (e.g. 201203051627)
or as a combination of a time and an offset, e.g. 201203051800-10h3m5s means
10 hours 3 minutes 5 seconds prior to 201203051800 (March 5, 2012, 6:00pm).
Absolute times are specified as YYYYMMDD[hh[mm[ss]]], i.e. a specific day
must be given, with optional hours, minutes or seconds, defaulting to the
earliest possible time when a portion is left out.  Offsets and lengths
are specified using one or more combinations of number (possibly floating
point) and designator:

s = second
m or mi = minute
h = hour
d = day
mo = month
y = year

2. There may be different comparison methods, triggered by different command
line arguments.

3. Currently we have code in `gridlocate` that reads documents in from a
corpus and amalgamates them using a grid of some sort.  We can reuse this
to amalgate documents by time.  E.g. if we want to compare two specific time
periods, we will have two corresponding cells, one for each period, and
throw away the remaining documents.  In other cases where we might want to
look at distributions over a period of time, we will have more cells, at
(possibly more or less) regular intervals.

*/
package opennlp.textgrounder.poligrounder

import util.matching.Regex
import util.Random
import math._
import collection.mutable

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil.{FileHandler, LocalFileHandler}
import opennlp.textgrounder.util.osutil.output_resource_usage
import opennlp.textgrounder.util.printutil.errprint

import opennlp.textgrounder.gridlocate._
import GridLocateDriver.Debug._

import opennlp.textgrounder.worddist.{WordDist,WordDistFactory}
import opennlp.textgrounder.worddist.WordDist.memoizer._

/*

This module is the main driver module for the Poligrounder subproject.
See GridLocate.scala.

*/

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

/**
 * Class retrieving command-line arguments or storing programmatic
 * configuration parameters.
 *
 * @param parser If specified, should be a parser for retrieving the
 *   value of command-line arguments from the command line.  Provided
 *   that the parser has been created and initialized by creating a
 *   previous instance of this same class with the same parser (a
 *   "shadow field" class), the variables below will be initialized with
 *   the values given by the user on the command line.  Otherwise, they
 *   will be initialized with the default values for the parameters.
 *   Because they are vars, they can be freely set to other values.
 *
 */
class PoligrounderParameters(parser: ArgParser = null) extends
    GridLocateParameters(parser) {
  var from =
    ap.option[String]("f", "from",
      help = """Chunk of start time to compare.""")

  var to =
    ap.option[String]("t", "to",
      help = """Chunk of end time to compare.""")

  var min_prob =
    ap.option[Double]("min-prob", "mp", default = 0.0,
      help = """Mininum probability when comparing distributions.
      Default is 0.0, which means no restrictions.""")

  var max_items =
    ap.option[Int]("max-items", "mi", default = 200,
      help = """Maximum number of items (words or n-grams) to output when
      comparing distributions.  Default is %default.  This applies separately
      to those items that have increased and decreased, meaning the total
      number counting both kinds may be as much as twice the maximum.""")
}

class PoligrounderDriver extends
    GridLocateDriver with StandaloneExperimentDriverStats {
  type TParam = PoligrounderParameters
  type TRunRes = Unit
  type TGrid = TimeCellGrid
  type TDoc = TimeDocument
  type TDocTable = TimeDocumentTable

  var degrees_per_cell = 0.0
  var from_chunk: (Long, Long) = _
  var to_chunk: (Long, Long) = _

  /**
   * Parse a date and return a time as milliseconds since the Epoch
   * (Jan 1, 1970).  Accepts various formats, all variations of the
   * following:
   *
   * 20100802180502PST (= August 2, 2010, 18:05:02 Pacific Standard Time)
   * 20100802060502pmPST (= same)
   * 20100802100502pm (= same if current time zone is Eastern Daylight)
   *
   * That is, either 12-hour or 24-hour time can be given, and the time
   * zone can be omitted.  In addition, part or all of the time of day
   * (hours, minutes, seconds) can be omitted.  Years must always be
   * full (i.e. 4 digits).
   */
  def parse_date(datestr: String): Long = {
    // Variants for the hour-minute-second portion
    val hms_variants = List("", "HH", "HHmm", "HHmmss", "hhaa", "hhmmaa",
      "hhmmssaa")
    // Fully-specified format including date
    val full_fmt = hms_variants.map("yyyyMMdd"+_)
    // All formats, including variants with time zone specified
    val all_fmt = full_fmt ++ full_fmt.map(_+"zz")
    for (fmt <- all_fmt) {
      val pos = new java.text.ParsePosition(0)
      val formatter = new java.text.SimpleDateFormat(fmt)
      // (Possibly we shouldn't do this?) This rejects nonstandardness, e.g.
      // out-of-range values such as month 13 or hour 25; that's useful for
      // error-checking in case someone messed up entering the date.
      formatter.setLenient(false)
      val date = formatter.parse(datestr, pos)
      if (date != null && pos.getIndex == datestr.length)
        return date.getTime
    }
    param_error("Can't parse time '%s'; should be something like 201008021805pm"
      format datestr)
  }

  /**
   * Parse an interval specification, e.g. "5h" for 5 hours or "3m2s" for
   * "3 minutes 2 seconds".
   *
   * Return Long.MinValue if can't parse.
   */
  def parse_interval(interval: String): Long = {
    // FIXME: Write this!
    interval.toInt * 1000 
  }

  def parse_date_and_interval(str: String) = {
    val date_interval = str.split("/")
    if (date_interval.length != 2)
      param_error("Time chunk %s must be of the format 'START/INTERVAL'"
        format str)
    else {
      val Array(datestr, intervalstr) = date_interval
      val date = parse_date(datestr)
      (date, date + parse_interval(intervalstr))
    }
  }

  override def handle_parameters() {
    from_chunk = parse_date_and_interval(params.from)
    to_chunk = parse_date_and_interval(params.to)

    super.handle_parameters()
  }

  protected def initialize_document_table(word_dist_factory: WordDistFactory) = {
    new TimeDocumentTable(this, word_dist_factory)
  }

  protected def initialize_cell_grid(table: TimeDocumentTable) = {
    new TimeCellGrid(from_chunk, to_chunk, table)
  }

  def run_after_setup() {
    DistributionComparer.compare_cells(cell_grid.asInstanceOf[TimeCellGrid],
      params.min_prob, params.max_items)
  }
}

object PoligrounderApp extends GridLocateApp("poligrounder") {
  type TDriver = PoligrounderDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver()
}

