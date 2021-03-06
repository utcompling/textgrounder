///////////////////////////////////////////////////////////////////////////////
//  Poligrounder.scala
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
look at language models over a period of time, we will have more cells, at
(possibly more or less) regular intervals.

*/
package opennlp.textgrounder
package poligrounder

import scala.util.matching.Regex
import scala.util.Random
import math._
import collection.mutable

import util.argparser._
import util.collection._
import util.io
import util.textdb._
import util.experiment._
import util.print.errprint
import util.error.internal_error
import util.time._

import gridlocate._
import util.debug._

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
class PoligrounderParameters(val parser: ArgParser = null) extends
    GridLocateParameters {
  var from = ap.option[String]("f", "from",
    must = be_specified,
    help = """Chunk of start time to compare.""")

  var to = ap.option[String]("t", "to",
    must = be_specified,
    help = """Chunk of end time to compare.""")

  var min_prob = ap.option[Double]("min-prob", "mp", default = 0.0,
    must = be_within(0.0, 1.0),
    help = """Mininum probability when comparing language models.
    Default is 0.0, which means no restrictions.""")

  var max_grams = ap.option[Int]("max-grams", "mg", default = 200,
    must = be_>(0),
    help = """Maximum number of grams (words or n-grams) to output when
    comparing language models.  Default is %default.  This applies separately
    to those grams that have increased and decreased, meaning the total
    number counting both kinds may be as much as twice the maximum.""")

  var ideological_user_corpus = ap.option[String](
    "ideological-user-corpus", "iuc",
    help="""Textdb containing corpus output from FindPolitical,
    listing users and associated ideologies. The value can be any of
  the following: Either the data or schema file of the database;
  the common prefix of the two; or the directory containing them, provided
  there is only one textdb in the directory.""")

  var ideological_users: Map[String, Double] = _
  var ideological_users_liberal: Map[String, Double] = _
  var ideological_users_conservative: Map[String, Double] = _
  var ideological_categories: Seq[String] = _

  if (ap.parsedValues) {
    if (ideological_user_corpus != null) {
      val rows = TextDB.read_textdb(io.localfh,
          ideological_user_corpus, suffix_re = "ideo-users")
      val users =
        (for (row <- rows) yield {
          val user = row.gets("user")
          val ideology = row.get[Double]("ideology")
          (user, ideology)
        }).toMap
      ideological_users = users
      ideological_users_liberal =
        users filter { case (u, ideo) => ideo < 0.33 }
      ideological_users_conservative =
        users filter { case (u, ideo) => ideo > 0.66 }
      ideological_categories = Seq("liberal", "conservative")
    } else
      ideological_categories = Seq("all")
  }

  // Unused, determined by --ideological-user-corpus.
//  var mode = ap.option[String]("m", "mode",
//    default = "combined",
//    choices = Seq("combined", "ideo-users"),
//    help = """How to compare language models.  Possible values are
//
//    'combined': For a given time period, combine all users into a single
//    language model.
//
//    'ideo-users': Retrieve the ideology of the users and use that to
//    separate the users into liberal and conservative, and compare those
//    separately.""")
}

class PoligrounderDriver extends
    GridLocateDriver[TimeCoord] with StandaloneExperimentDriverStats {
  type TParam = PoligrounderParameters
  type TRunRes = Unit

  var from_chunk: (Long, Long) = _
  var to_chunk: (Long, Long) = _

  def deserialize_coord(coord: String) = TimeCoord.deserialize(coord)

  protected def create_document_factory(lang_model_factory: DocLangModelFactory) =
    new TimeDocFactory(this, lang_model_factory)

  protected def create_empty_grid(
      create_docfact: => GridDocFactory[TimeCoord],
      id: String
  ) = {
    val time_docfact = create_docfact.asInstanceOf[TimeDocFactory]
    if (params.ideological_user_corpus == null)
      new TimeGrid(from_chunk, to_chunk, IndexedSeq("all"), x => "all", time_docfact, id)
    else
      new TimeGrid(from_chunk, to_chunk, IndexedSeq("liberal", "conservative"),
        x => {
          if (params.ideological_users_liberal contains x.user)
            "liberal"
          else if (params.ideological_users_conservative contains x.user)
            "conservative"
          else
            null
        }, time_docfact, id)
  }

  // FIXME!
  def create_rough_ranker(args: Array[String]) = ???

  def run() {
    def parse_interval(param: String) = {
      parse_date_interval(param) match {
        case (Some((start, end)), "") => (start, end)
        case (None, errmess) => param_error(errmess)
        case _ => ???
      }
    }
    from_chunk = parse_interval(params.from)
    to_chunk = parse_interval(params.to)
    val grid = initialize_grid
    if (params.ideological_user_corpus == null)
      LangModelComparer.compare_cells_2way(
        grid.asInstanceOf[TimeGrid], "all",
        params.min_prob, params.max_grams)
    else
      LangModelComparer.compare_cells_4way(
        grid.asInstanceOf[TimeGrid], "liberal", "conservative",
        params.min_prob, params.max_grams)
  }
}

object Poligrounder extends GridLocateApp("Poligrounder") {
  type TDriver = PoligrounderDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

