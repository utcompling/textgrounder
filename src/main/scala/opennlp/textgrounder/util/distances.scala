///////////////////////////////////////////////////////////////////////////////
//  distances.scala
//
//  Copyright (C) 2011, 2012 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.util

import math._

import printutil.warning
import mathutil.MeanShift

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
  always the same (about 111 km per degree, or about 69 miles per degree),
  but the distance between longitude lines varies according to the
  latitude, ranging from about 111 km per degree at the Equator to 0 km
  at the North and South Pole.
*/

/**
  Singleton object holding information of various sorts related to distances
  on the Earth and coordinates, objects for handling coordinates and cell
  indices, and miscellaneous functions for computing distance and converting
  between different coordinate formats.

  The following is contained:

  1. Fixed information: e.g. radius of Earth in kilometers (km), number of
     km per degree at the Equator, number of km per mile, minimum/maximum
     latitude/longitude.

  2. The SphereCoord class (holding a latitude/longitude pair)
  
  3. Function spheredist() to compute spherical (great-circle) distance
     between two SphereCoords; likewise degree_dist() to compute degree
     distance between two SphereCoords
 */
package object distances {
 
  /***** Fixed values *****/

  val minimum_latitude = -90.0
  val maximum_latitude = 90.0
  val minimum_longitude = -180.0
  val maximum_longitude = 180.0 - 1e-10

  // Radius of the earth in km.  Used to compute spherical distance in km,
  // and km per degree of latitude/longitude.
  // val earth_radius_in_miles = 3963.191
  val earth_radius_in_km = 6376.774
 
  // Number of kilometers per mile.
  val km_per_mile = 1.609

  // Number of km per degree, at the equator.  For longitude, this is the
  // same everywhere, but for latitude it is proportional to the degrees away
  // from the equator.
  val km_per_degree = Pi * 2 * earth_radius_in_km / 360.
 
  // Number of miles per degree, at the equator.
  val miles_per_degree = km_per_degree / km_per_mile

  def km_and_miles(kmdist: Double) = {
    "%.2f km (%.2f miles)" format (kmdist, kmdist / km_per_mile)
  }

  // A 2-dimensional coordinate.
  //
  // The following fields are defined:
  //
  //   lat, long: Latitude and longitude of coordinate.

  case class SphereCoord(lat: Double, long: Double) {
    // Not sure why this code was implemented with coerce_within_bounds,
    // but either always coerce, or check the bounds ...
    require(SphereCoord.valid(lat, long))
    override def toString() = "(%.2f,%.2f)".format(lat, long)
  }

  implicit object SphereCoord extends Serializer[SphereCoord] {
    // Create a coord, with METHOD defining how to handle coordinates
    // out of bounds.  If METHOD =  "validate", check within bounds,
    // and abort if not.  If "coerce", coerce within bounds (latitudes
    // are cropped, longitudes are taken mod 360).  If "coerce-warn",
    // same as "coerce" but also issue a warning when coordinates are
    // out of bounds.
    def apply(lat: Double, long: Double, method: String) = {
      val (newlat, newlong) =
        method match {
          case "coerce-warn" => {
            if (!valid(lat, long))
              warning("Coordinates out of bounds: (%.2f,%.2f)", lat, long)
            coerce(lat, long)
          }
          case "coerce" => coerce(lat, long)
          case "validate" => (lat, long)
          case _ => {
            require(false,
                    "Invalid method to SphereCoord(): %s" format method)
            (0.0, 0.0)
          }
        }
      new SphereCoord(newlat, newlong)
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

    def deserialize(foo: String) = {
      val Array(lat, long) = foo.split(",", -1)
      SphereCoord(lat.toDouble, long.toDouble)
    }
    
    def serialize(foo: SphereCoord) = "%s,%s".format(foo.lat, foo.long)
  }

  // A 1-dimensional coordinate (year value, expressed as floating point).
  // Note that having this here is an important check on the correctness
  // of the code elsewhere -- if by mistake you leave off the type
  // parameters when calling a function, and the function asks for a type
  // with a serializer and there's only one such type available, Scala
  // automatically uses that one type.  Hence code may work fine until you
  // add a second serializable type, and then lots of compile errors.

  case class Year(year: Double) { }

  implicit object YearSerializer extends Serializer[Year] {
    def deserialize(foo: String) = Year(foo.toDouble)
    def serialize(foo: Year) = "%s".format(foo.year)
  }

  // Compute spherical distance in km (along a great circle) between two
  // coordinates.
  
  def spheredist(p1: SphereCoord, p2: SphereCoord): Double = {
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
    return earth_radius_in_km * acos(anglecos)
  }
  
  def degree_dist(c1: SphereCoord, c2: SphereCoord) = {
    sqrt((c1.lat - c2.lat) * (c1.lat - c2.lat) +
      (c1.long - c2.long) * (c1.long - c2.long))
  }

  /**
   * Square area in km^2 of a rectangle on the surface of a sphere made up
   * of latitude and longitude lines. (Although the parameters below are
   * described as bottom-left and top-right, respectively, the function as
   * written is in fact insensitive to whether bottom-left/top-right or
   * top-left/bottom-right pairs are given, and which order they are
   * given.  All that matters is that opposite corners are supplied.  The
   * use of `abs` below takes care of this.)
   *
   * @param botleft Coordinate of bottom left of rectangle
   * @param topright Coordinate of top right of rectangle
   */
  def square_area(botleft: SphereCoord, topright: SphereCoord) = {
    var (lat1, lon1) = (botleft.lat, botleft.long)
    var (lat2, lon2) = (topright.lat, topright.long)
    lat1 = (lat1 / 180.) * Pi
    lat2 = (lat2 / 180.) * Pi
    lon1 = (lon1 / 180.) * Pi
    lon2 = (lon2 / 180.) * Pi

    (earth_radius_in_km * earth_radius_in_km) *
      abs(sin(lat1) - sin(lat2)) *
      abs(lon1 - lon2)
  }

  /**
   * Average two longitudes.  This is a bit tricky because of the way
   * they wrap around.
   */
  def average_longitudes(long1: Double, long2: Double): Double = {
    if (long1 - long2 > 180.)
      average_longitudes(long1 - 360., long2)
    else if (long2 - long1 > 180.)
      average_longitudes(long1, long2 - 360.)
    else
      (long1 + long2) / 2.0
  }

  class SphereMeanShift(
    h: Double = 1.0,
    max_stddev: Double = 1e-10,
    max_iterations: Int = 100
  ) extends MeanShift[SphereCoord](h, max_stddev, max_iterations) {
    def squared_distance(x: SphereCoord, y:SphereCoord) = {
      val dist = spheredist(x, y)
      dist * dist
    }

    def weighted_sum(weights:Array[Double], points:Array[SphereCoord]) = {
      val len = weights.length
      var lat = 0.0
      var long = 0.0
      for (i <- 0 until len) {
        val w = weights(i)
        val c = points(i)
        lat += c.lat * w
        long += c.long * w
      }
      SphereCoord(lat, long)
    }

    def scaled_sum(scalar:Double, points:Array[SphereCoord]) = {
      var lat = 0.0
      var long = 0.0
      for (c <- points) {
        lat += c.lat * scalar
        long += c.long * scalar
      }
      SphereCoord(lat, long)
    }
  }
}
