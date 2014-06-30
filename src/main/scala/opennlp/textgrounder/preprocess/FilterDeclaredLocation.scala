//  FilterDeclaredLocation.scala
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

import scala.collection.mutable
import scala.util.Random

import net.liftweb

import util.argparser._
import util.collection._
import util.experiment._
import util.io.localfh
import util.print._

class FilterDeclaredLocationParameters(ap: ArgParser) {
  var city = ap.option[String]("city",
    default = "springfield,portland",
    help = """Ambiguous city or cities that the location field must refer to.
Multiple cities should be separated by commas. NOTE: Currently only supports
single-word city names.""")
  var filtered_tweet_file = ap.option[String]("filtered-tweet-file", "ftf",
    must = be_size_>[String](0), // Allows for unspecified file.
    help = """File to which filtered tweets are written to.""")
  var input = ap.multiPositional[String]("input",
    must = be_specified,
    help = """Tweet file(s) to analyze.""")
  var excluded_cities = ap.option[String]("excluded-cities",
    default = "",
    help = """Cities to exclude from filtered tweets. Separate with semicolons.
Should be of the form 'springfield, pennsylvania', all in lowercase.""")
  var max_users = ap.option[Int]("max-users",
    must = be_>(0),
    default = 100,
    help = "Maximum number of users to select per city.")
  var min_tweets = ap.option[Int]("min-tweets",
    must = be_>(0),
    default = 5,
    help = "Minimum number of tweets for selecting users.")
}

/**
 * An application to analyze the different values of some field seen in
 * a textdb corpus, displaying minimum, maximum, quantiles, etc.
 */
object FilterDeclaredLocation extends ExperimentApp("FilterDeclaredLocation") {

  type TParam = FilterDeclaredLocationParameters

  def create_param_object(ap: ArgParser) = new FilterDeclaredLocationParameters(ap)

  val states = Map(
    "AL" -> "Alabama",
    "AK" -> "Alaska",
    "AZ" -> "Arizona",
    "AR" -> "Arkansas",
    "CA" -> "California",
    "CO" -> "Colorado",
    "CT" -> "Connecticut",
    "DC" -> "District of Columbia",
    "DE" -> "Delaware",
    "FL" -> "Florida",
    "GA" -> "Georgia",
    "HI" -> "Hawaii",
    "ID" -> "Idaho",
    "IL" -> "Illinois",
    "IN" -> "Indiana",
    "IA" -> "Iowa",
    "KS" -> "Kansas",
    "KY" -> "Kentucky",
    "LA" -> "Louisiana",
    "ME" -> "Maine",
    "MD" -> "Maryland",
    "MA" -> "Massachusetts",
    "MI" -> "Michigan",
    "MN" -> "Minnesota",
    "MS" -> "Mississippi",
    "MO" -> "Missouri",
    "MT" -> "Montana",
    "NE" -> "Nebraska",
    "NV" -> "Nevada",
    "NH" -> "New Hampshire",
    "NJ" -> "New Jersey",
    "NM" -> "New Mexico",
    "NY" -> "New York",
    "NC" -> "North Carolina",
    "ND" -> "North Dakota",
    "OH" -> "Ohio",
    "OK" -> "Oklahoma",
    "OR" -> "Oregon",
    "PA" -> "Pennsylvania",
    "PR" -> "Puerto Rico",
    "RI" -> "Rhode Island",
    "SC" -> "South Carolina",
    "SD" -> "South Dakota",
    "TN" -> "Tennessee",
    "TX" -> "Texas",
    "UT" -> "Utah",
    "VT" -> "Vermont",
    "VA" -> "Virginia",
    "VI" -> "Virgin Islands",
    "WA" -> "Washington",
    "WV" -> "West Virginia",
    "WI" -> "Wisconsin",
    "WY" -> "Wyoming"
  ).map { case (abbrev, full) => (abbrev.toLowerCase, full.toLowerCase) }

  /**
   * Retrieve a string along a path, checking to make sure the path
   * exists.
   */
  def force_string(value: liftweb.json.JValue, fields: String*): String = {
    var fieldval = value
    var path = List[String]()
    for (field <- fields) {
      path :+= field
      fieldval \= field
      if (fieldval == liftweb.json.JNothing) {
        val fieldpath = path mkString "."
        errprint("Can't find field path %s in tweet", fieldpath)
        return ""
      }
    }
    val values = fieldval.values
    if (values == null) {
      errprint("Null value from path %s", fields mkString ".")
      ""
    } else
      fieldval.values.toString
  }

  def run_program(args: Array[String]) = {
    val excluded_cities = params.excluded_cities.split(";").toSet
    // Map from users to map of cities seen for that user and # tweets seen
    // for each city, where "city" is actually a city/state combination
    val users_cities_tweetcounts_map = intmapmap[String, String]()
    // Map of # tweets seen for each city overall, where "city" is
    // actually a city/state combination
    val cities_tweetcounts = intmap[String]()
    // List of ambiguous cities in question
    val toponyms = params.city.split(""",\s*""")
    // Set of abbreviations for states
    val states_abbrev = states.keySet
    // Set of full names for states
    val states_full = states.values.toSet
    for (file <- params.input) {
      for (line <- localfh.openr(file)) {
        val parsed = liftweb.json.parse(line)
        val user = (parsed \ "user" \ "screen_name" values).toString
        val location = force_string(parsed, "user", "location")
        val loc1 = location.toLowerCase.replaceAll("""[^a-z]""", " ").trim
        val locwords = loc1.split("""\s+""")
        if (locwords.size >= 2 && (toponyms contains locwords(0))) {
          var state = ""
          if (states contains locwords(1))
            state = states(locwords(1))
          else if (states_full contains locwords(1))
            state = locwords(1)
          else if (locwords.size >= 3) {
            val two_word_state = locwords.slice(1, 3).mkString(" ")
            if (states_full contains two_word_state)
              state = two_word_state
          } else if (locwords.size >= 4) {
            val three_word_state = locwords.slice(1, 4).mkString(" ")
            if (states_full contains three_word_state)
              state = three_word_state
          }
          if (state.length > 0) {
            val city_state = s"${locwords(0)}, $state"
            users_cities_tweetcounts_map(user)(city_state) += 1
            cities_tweetcounts(city_state) += 1
          } else
            errprint("Unrecognized state: %s", location)
        }
      }
    }
    errprint("Cities tweetcounts:")
    for ((city, count) <- cities_tweetcounts.toSeq.sortBy(-_._2)) {
      errprint("  %s: %s", city, count)
    }
    errprint("")
    errprint("Users cities:")
    // List of tuples of (user, city, tweetcount) for users that have only
    // one city listed.
    val users_cities_tweetcounts_list = mutable.Buffer[(String, String, Int)]()
    for ((user, cities) <- users_cities_tweetcounts_map) {
      if (cities.size == 1) {
        val (city, count) = cities.head
        users_cities_tweetcounts_list += ((user, city, count))
      } else {
        errprint("  User %s has %s locations:", user, cities.size)
        for ((city, count) <- cities)
          errprint("    %s = %s", city, count)
      }
    }
    // Map of cities and counts of users in those cities
    val city_usercount = intmap[String]()
    // Map of cities and counts of users in those cities with >= 10 tweets
    val city_usercount_10 = intmap[String]()
    // Map of cities and counts of users in those cities with >= 5 tweets
    val city_usercount_5 = intmap[String]()
    // Map of cities and lists of (user, tweetcount) tuples in that city.
    val cities_users = bufmap[String, (String, Int)]()
    for ((user, city, count) <- users_cities_tweetcounts_list.toSeq.sortBy(-_._3)) {
      if (count >= 10)
        city_usercount_10(city) += 1
      if (count >= 5)
        city_usercount_5(city) += 1
      city_usercount(city) += 1
      if (count >= params.min_tweets)
        cities_users(city) += ((user, count))
      errprint("  User %s: %s = %s", user, city, count)
    }
    errprint("Cities usercounts:")
    for ((city, count) <- city_usercount.toSeq.sortBy(-_._2)) {
      errprint("  %s: %s", city, count)
    }
    errprint("Cities usercounts with >= 10 tweets:")
    for ((city, count) <- city_usercount_10.toSeq.sortBy(-_._2)) {
      errprint("  %s: %s", city, count)
    }
    errprint("Cities usercounts with >= 5 tweets:")
    for ((city, count) <- city_usercount_5.toSeq.sortBy(-_._2)) {
      errprint("  %s: %s", city, count)
    }

    // Select the users for each city
    val selected_users = mutable.Set[String]()
    val selected_cities = mutable.Buffer[String]()
    errprint("Selected users for cities with >= %s tweets:",
      params.min_tweets)
    for ((city, users) <- cities_users) {
      val selected = (new Random).shuffle(users).take(params.max_users)
      // We only select cities with enough users
      if (selected.size == params.max_users &&
          !(excluded_cities contains city)) {
        selected_users ++= selected.map(_._1)
        selected_cities += city
      }
      errprint("  City %s, %s users, %.2f average tweets/user:",
        city, selected.size, selected.map(_._2).sum.toDouble / selected.size)
      for (((user, tweetcount), index) <- (selected zip Stream.from(1)))
        errprint("    #%s: %s = %s", index, user, tweetcount)
    }
    errprint("Selected cities:")
    for (city <- selected_cities)
      errprint("  %s", city)

    // Now go through the tweets again and select only those users we want
    if (params.filtered_tweet_file != null) {
      val outfile = localfh.openw(params.filtered_tweet_file)
      for (file <- params.input) {
        for (line <- localfh.openr(file)) {
          val parsed = liftweb.json.parse(line)
          val user = (parsed \ "user" \ "screen_name" values).toString
          if (selected_users contains user)
            outfile.println(line)
        }
      }
    }
    0
  }
}
