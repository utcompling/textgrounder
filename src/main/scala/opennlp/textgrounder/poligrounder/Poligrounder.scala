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

import opennlp.textgrounder.{util=>tgutil}
import tgutil.argparser._
import tgutil.collectionutil._
import tgutil.corpusutil._
import tgutil.distances._
import tgutil.experiment._
import tgutil.ioutil.{FileHandler, LocalFileHandler}
import tgutil.osutil.output_resource_usage
import tgutil.printutil.errprint
import tgutil.timeutil._

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
  var from = ap.option[String]("f", "from",
    help = """Chunk of start time to compare.""")

  var to = ap.option[String]("t", "to",
    help = """Chunk of end time to compare.""")

  var min_prob = ap.option[Double]("min-prob", "mp", default = 0.0,
    help = """Mininum probability when comparing distributions.
    Default is 0.0, which means no restrictions.""")

  var max_items = ap.option[Int]("max-items", "mi", default = 200,
    help = """Maximum number of items (words or n-grams) to output when
    comparing distributions.  Default is %default.  This applies separately
    to those items that have increased and decreased, meaning the total
    number counting both kinds may be as much as twice the maximum.""")

  var ideological_user_corpus = ap.option[String](
    "ideological-user-corpus", "iuc",
    help="""File containing corpus output from FindPolitical, listing
    users and associated ideologies.""")

  var ideological_users: Map[String, Double] = _
  var ideological_users_liberal: Map[String, Double] = _
  var ideological_users_conservative: Map[String, Double] = _
  var ideological_categories: Seq[String] = _

  var mode = ap.option[String]("m", "mode",
    default = "combined",
    choices = Seq("combined", "ideo-users"),
    help = """How to compare distributions.  Possible values are
    
    'combined': For a given time period, combine all users into a single
    distribution.
    
    'ideo-users': Retrieve the ideology of the users and use that to
    separate the users into liberal and conservative, and compare those
    separately.""")
}

/**
 * A simple field-text file processor that just records the users and ideology.
 *
 * @param suffix Suffix used to select document metadata files in a directory
 */
class IdeoUserFileProcessor extends
    CorpusFieldFileProcessor[(String, Double)]("ideo-users") {
  def handle_row(fieldvals: Seq[String]) = {
    val user = schema.get_field(fieldvals, "user")
    val ideology =
      schema.get_field(fieldvals, "ideology").toDouble
    Some((user, ideology))
  }
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

  override def handle_parameters() {
    def parse_interval(param: String) = {
      parse_date_interval(param) match {
        case (Some((start, end)), "") => (start, end)
        case (None, errmess) => param_error(errmess)
      }
    }
    from_chunk = parse_interval(params.from)
    to_chunk = parse_interval(params.to)

    if (params.ideological_user_corpus != null) {
      val processor = new IdeoUserFileProcessor
      val users =
        processor.read_corpus(new LocalFileHandler,
          params.ideological_user_corpus).flatten.toMap
      params.ideological_users = users
      params.ideological_users_liberal =
        users filter { case (u, ideo) => ideo < 0.33 }
      params.ideological_users_conservative =
        users filter { case (u, ideo) => ideo > 0.66 }
      params.ideological_categories = Seq("liberal", "conservative")
    } else
      params.ideological_categories = Seq("all")

    super.handle_parameters()
  }

  override protected def initialize_word_dist_suffix() = {
    super.initialize_word_dist_suffix() + "-tweets"
  }

  protected def initialize_document_table(word_dist_factory: WordDistFactory) = {
    new TimeDocumentTable(this, word_dist_factory)
  }

  protected def initialize_cell_grid(table: TimeDocumentTable) = {
    if (params.ideological_user_corpus == null)
      new TimeCellGrid(from_chunk, to_chunk, Seq("all"), x => "all", table)
    else
      new TimeCellGrid(from_chunk, to_chunk, Seq("liberal", "conservative"),
        x => {
          if (params.ideological_users_liberal contains x.user)
            "liberal"
          else if (params.ideological_users_conservative contains x.user)
            "conservative"
          else
            null
        }, table)
  }

  def run_after_setup() {
    if (params.ideological_user_corpus == null)
      DistributionComparer.compare_cells(cell_grid.asInstanceOf[TimeCellGrid],
        "all", params.min_prob, params.max_items)
    else
      DistributionComparer.compare_cells_2way(
        cell_grid.asInstanceOf[TimeCellGrid], "liberal", "conservative",
        params.min_prob, params.max_items)
  }
}

object PoligrounderApp extends GridLocateApp("poligrounder") {
  type TDriver = PoligrounderDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver()
}

