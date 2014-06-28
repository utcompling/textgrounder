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

import collection.mutable

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
  var location_file = ap.option[String]("location-file",
    must = be_size_>[String](0), // Allows for unspecified file.
    help = """File to which cleaned-up locations and counts are written to.""")
  var input = ap.multiPositional[String]("input",
    must = be_specified,
    help = """Tweet file(s) to analyze.""")
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
    // Map from users to cities seen for that user
    val users_cities = intmapmap[String, String]()
    val city_counts = intmap[String]()
    val cities = params.city.split(""",\s*""")
    val states_abbrev = states.keySet
    val states_full = states.values.toSet
    for (file <- params.input) {
      for (line <- localfh.openr(file)) {
        val parsed = liftweb.json.parse(line)
        val user = (parsed \ "user" \ "screen_name" values).toString
        val location = force_string(parsed, "user", "location")
        val loc1 = location.toLowerCase.replaceAll("""[^a-z]""", " ").trim
        val locwords = loc1.split("""\s+""")
        if (locwords.size >= 2 && (cities contains locwords(0))) {
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
            users_cities(user)(city_state) += 1
            city_counts(city_state) += 1
          } else
            errprint("Unrecognized state: %s", location)
        }
      }
    }
    errprint("Cities counts:")
    for ((city, count) <- city_counts.toSeq.sortBy(-_._2)) {
      errprint("  %s: %s", city, count)
    }
    errprint("")
    errprint("Users cities:")
    val user_city_count = mutable.Buffer[(String, String, Int)]()
    for ((user, cities) <- users_cities) {
      if (cities.size == 1) {
        val (city, count) = cities.head
        user_city_count += ((user, city, count))
      } else {
        errprint("  User %s has %s locations:", user, cities.size)
        for ((city, count) <- cities)
          errprint("    %s = %s", city, count)
      }
    }
    val city_usercount = intmap[String]()
    val city_usercount_10 = intmap[String]()
    val city_usercount_5 = intmap[String]()
    for ((user, city, count) <- user_city_count.toSeq.sortBy(-_._3)) {
      if (count >= 10)
        city_usercount_10(city) += 1
      if (count >= 5)
        city_usercount_5(city) += 1
      city_usercount(city) += 1
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
    0
  }
}
