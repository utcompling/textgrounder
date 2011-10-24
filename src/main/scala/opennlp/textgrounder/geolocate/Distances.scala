///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.geolocate

import math._
import NlpUtil.warning

/*
  The coordinates of a point are spherical coordinates, indicating a
  latitude and longitude.  Latitude ranges from -90 degrees (south) to
  +90 degrees (north), with the Equator at 0 degrees.  Longitude ranges
  from -180 degrees (west) to +179.9999999.... degrees (east). -180 and +180
  degrees correspond to the same north-south parallel, and we arbitrarily
  choose -180 degrees over +180 degrees.  0 degrees longitude has been
  arbitrarily chosen as the north-south parallel that passes through
  Greenwich, England (near London).  Note that longitude wraps around, but
  latitude does not.  Furthermore, the distance between latitude lines is
  always the same (about 69 miles per degree), but the distance between
  longitude lines varies according to the latitude, ranging from about
  69 miles per degree at the Equator to 0 miles at the North and South Pole.
*/

/**
  Singleton object holding information of various sorts related to distances
  on the Earth and coordinates, objects for handling coordinates and cell
  indices, and miscellaneous functions for computing distance and converting
  between different coordinate formats.

  The following is contained:

  1. Fixed information: e.g. radius of Earth in miles, number of miles per
     degree at the Equator, number of kilometers per mile, minimum/maximum
     latitude/longitude.

  2. The Coord class (holding a latitude/longitude pair)
  
  3. Function spheredist() to compute spherical (great-circle) distance
     between two Coords; likewise degree_dist() to compute degree distance
     between two Coords
 */
object Distances {
 
  /***** Fixed values *****/

  val minimum_latitude = -90.0
  val maximum_latitude = 90.0
  val minimum_longitude = -180.0
  val maximum_longitude = 180.0 - 1e-10

  // Radius of the earth in miles.  Used to compute spherical distance in miles,
  // and miles per degree of latitude/longitude.
  val earth_radius_in_miles = 3963.191
 
  // Number of kilometers per mile.
  val km_per_mile = 1.609

  // Number of miles per degree, at the equator.  For longitude, this is the
  // same everywhere, but for latitude it is proportional to the degrees away
  // from the equator.
  val miles_per_degree = Pi * 2 * earth_radius_in_miles / 360.
 
  // A 2-dimensional coordinate.
  //
  // The following fields are defined:
  //
  //   lat, long: Latitude and longitude of coordinate.

  case class Coord(lat: Double, long: Double,
      validate: Boolean = true) {
    if (validate) {
      // Not sure why this code was implemented with coerce_within_bounds,
      // but either always coerce, or check the bounds ...
      require(lat >= minimum_latitude)
      require(lat <= maximum_latitude)
      require(long >= minimum_longitude)
      require(long <= maximum_longitude)
    }
    override def toString() = "(%.2f,%.2f)".format(lat, long)
  }

  object Coord {
    // Create a coord, with METHOD defining how to handle coordinates
    // out of bounds.  If METHOD = "accept", just accept them; if
    // "validate", check within bounds, and abort if not.  If "coerce",
    // coerce within bounds (latitudes are cropped, longitudes are taken
    // mod 360).
    def apply(lat: Double, long: Double, method: String) = {
      var validate = false
      val (newlat, newlong) =
        method match {
          case "coerce-warn" => {
            if (!valid(lat, long))
              warning("Coordinates out of bounds: (%.2f,%.2f)", lat, long)
            coerce(lat, long)
          }
          case "coerce" => coerce(lat, long)
          case "validate" => { validate = true; (lat, long) }
          case "accept" => { (lat, long) }
          case _ => { require(false,
                              "Invalid method to Coord(): %s" format method)
                      (0.0, 0.0) }
        }
      new Coord(newlat, newlong, validate = validate)
    }

    def valid(lat: Double, long: Double) = (
      lat >= minimum_latitude &&
      lat <= maximum_latitude &&
      long >= minimum_longitude &&
      long <= maximum_longitude
    )

    def coerce(lat: Double, long: Double) = {
      var newlat = lat
      var newlong = long
      if (newlat > maximum_latitude) newlat = maximum_latitude
      while (newlong > maximum_longitude) newlong -= 360.
      if (newlat < minimum_latitude) newlat = minimum_latitude
      while (newlong < minimum_longitude) newlong += 360.
      (newlat, newlong)
    }
  }

  // Compute spherical distance in miles (along a great circle) between two
  // coordinates.
  
  def spheredist(p1: Coord, p2: Coord): Double = {
    if (p1 == null || p2 == null) return 1000000.
    val thisRadLat = (p1.lat / 180.) * Pi
    val thisRadLong = (p1.long / 180.) * Pi
    val otherRadLat = (p2.lat / 180.) * Pi
    val otherRadLong = (p2.long / 180.) * Pi
          
    val anglecos = (sin(thisRadLat)*sin(otherRadLat)
                + cos(thisRadLat)*cos(otherRadLat)*
                  cos(otherRadLong-thisRadLong))
    // If the values are extremely close to each other, the resulting cosine
    // value will be extremely close to 1.  In reality, however, if the values
    // are too close (e.g. the same), the computed cosine will be slightly
    // above 1, and acos() will complain.  So special-case this.
    if (abs(anglecos) > 1.0) {
      if (abs(anglecos) > 1.000001) {
        warning("Something wrong in computation of spherical distance, out-of-range cosine value %f",
          anglecos)
        return 1000000.
      } else
        return 0.
    }
    return earth_radius_in_miles * acos(anglecos)
  }
  
  def degree_dist(c1: Coord, c2: Coord) = {
    sqrt((c1.lat - c2.lat) * (c1.lat - c2.lat) +
      (c1.long - c2.long) * (c1.long - c2.long))
  }
}
