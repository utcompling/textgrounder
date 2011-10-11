package opennlp.textgrounder.geolocate

import math._
import NlpUtil.warning

object Distances {
  /////////////////////////////////////////////////////////////////////////////
  //                       Coordinates and regions                           //
  /////////////////////////////////////////////////////////////////////////////
  
  // The coordinates of a point are spherical coordinates, indicating a
  // latitude and longitude.  Latitude ranges from -90 degrees (south) to
  // +90 degrees (north), with the Equator at 0 degrees.  Longitude ranges
  // from -180 degrees (west) to +179.9999999.... degrees (east). -180 and +180
  // degrees correspond to the same north-south parallel, and we arbitrarily
  // choose -180 degrees over +180 degrees.  0 degrees longitude has been
  // arbitrarily chosen as the north-south parallel that passes through
  // Greenwich, England (near London).  Note that longitude wraps around, but
  // latitude does not.  Furthermore, the distance between latitude lines is
  // always the same (about 69 miles per degree), but the distance between
  // longitude lines varies according to the latitude, ranging from about
  // 69 miles per degree at the Equator to 0 miles at the North and South Pole.
  //
  // We divide the earth's surface into "tiling regions", using the value
  // of --region-size, which is specified in miles; we convert it to degrees
  // using 'miles_per_degree', which is derived from the value for the
  // Earth's radius in miles.  In addition, we form a square of tiling regions
  // in order to create a "statistical region", which is used to compute a
  // distribution over words.  The numbe of tiling regions on a side is
  // determined by --width-of-stat-region.  Note that if this is greater than
  // 1, different statistical regions will overlap.
  //
  // To specify a region, we use region indices, which are derived from
  // coordinates by dividing by degrees_per_region.  Hence, if for example
  // degrees_per_region is 2, then region indices are in the range [-45,+45]
  // for latitude and [-90,+90) for longitude.  In general, an arbitrary
  // coordinate will have fractional region indices; however, the region
  // indices of the corners of a region (tiling or statistical) will be
  // integers.  Normally, we use the southwest corner to specify a region.
  //
  // Near the edges, tiling regions may be truncated.  Statistical regions
  // will wrap around longitudinally, and will still have the same number
  // of tiling regions, but may be smaller.

  type Regind = Int
  
  // Size of each region in degrees.  Determined by the --region-size option
  // (however, that option is expressed in miles).
  var degrees_per_region = 0.0
  
  val minimum_latitude = -90.0
  val maximum_latitude = 90.0
  val minimum_longitude = -180.0
  val maximum_longitude = 180.0 - 1e-10

  // Minimum, maximum latitude/longitude in indices (integers used to index the
  // set of regions that tile the earth)
  var minimum_latind:Regind = 0
  var maximum_latind:Regind = 0
  var minimum_longind:Regind = 0
  var maximum_longind:Regind = 0
  
  // Radius of the earth in miles.  Used to compute spherical distance in miles,
  // and miles per degree of latitude/longitude.
  val earth_radius_in_miles = 3963.191
  
  // Number of miles per degree, at the equator.  For longitude, this is the
  // same everywhere, but for latitude it is proportional to the degrees away
  // from the equator.
  val miles_per_degree = Pi * 2 * earth_radius_in_miles / 360.
 
  var width_of_stat_region = 1

  // A 2-dimensional coordinate.
  //
  // The following fields are defined:
  //
  //   lat, long: Latitude and longitude of coordinate.

  case class Coord(lat:Double, long:Double,
      validate:Boolean = true) {
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
    def apply(lat:Double, long:Double, method:String) = {
      var newlat = lat
      var newlong = long
      var validate = false
      method match {
        case "coerce" => {
          if (newlat > maximum_latitude) newlat = maximum_latitude
          while (newlong > maximum_longitude) newlong -= 360.
          if (newlat < minimum_latitude) newlat = minimum_latitude
          while (newlong < minimum_longitude) newlong += 360.
        }
        case "validate" => { validate = true }
        case "accept" => { }
        case _ => { require(method == "coerce" || method == "validate" ||
                            method == "accept") }
      }
      new Coord(newlat, newlong, validate = validate)
    }

    def valid(lat:Double, long:Double) = (
      lat >= minimum_latitude &&
      lat <= maximum_latitude &&
      long >= minimum_longitude &&
      long <= maximum_longitude
    )
  }

  // Compute spherical distance in miles (along a great circle) between two
  // coordinates.
  
  def spheredist(p1:Coord, p2:Coord):Double = {
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
        warning("Something wrong in computation of spherical distance, out-of-range cosine value %f", anglecos)
        return 1000000.
      } else
        return 0.
    }
    return earth_radius_in_miles * acos(anglecos)
  }
  
  // Convert a coordinate to the indices of the southwest corner of the
  // corresponding tiling region.
  def coord_to_tiling_region_indices(coord:Coord) = {
    val latind:Regind = floor(coord.lat / degrees_per_region).toInt
    val longind:Regind = floor(coord.long / degrees_per_region).toInt
    (latind, longind)
  }
  
  // Convert a coordinate to the indices of the southwest corner of the
  // corresponding statistical region.
  def coord_to_stat_region_indices(coord:Coord) = {
    // When width_of_stat_region = 1, don't subtract anything.
    // When width_of_stat_region = 2, subtract 0.5*degrees_per_region.
    // When width_of_stat_region = 3, subtract degrees_per_region.
    // When width_of_stat_region = 4, subtract 1.5*degrees_per_region.
    // In general, subtract (width_of_stat_region-1)/2.0*degrees_per_region.
  
    // Compute the indices of the southwest region
    val subval = (width_of_stat_region-1)/2.0*degrees_per_region
    val lat = coord.lat - subval
    val long = coord.long - subval
  
    coord_to_tiling_region_indices(Coord(lat, long))
  }
  
  // Convert region indices to the corresponding coordinate.  This can also
  // be used to find the coordinate of the southwest corner of a tiling region
  // or statistical region, as both are identified by the region indices of
  // their southwest corner.  Values are double since we may be requesting the
  // coordinate of a location not exactly at a region index (e.g. the center
  // point).
  def region_indices_to_coord(latind:Double, longind:Double,
      method:String = "validate") = {
    Coord(latind * degrees_per_region, longind * degrees_per_region,
          method)
  }
  
  // Add 'offset' to both latind and longind and then convert to a
  // coordinate.  Coerce the coordinate to be within bounds.
  def offset_region_indices_to_coord(latind:Regind, longind:Regind,
      offset:Double) = {
    region_indices_to_coord(latind + offset, longind + offset, "coerce")
  }
  
  // Convert region indices of a tiling region to the coordinate of the
  // near (i.e. southwest) corner of the region.
  def tiling_region_indices_to_near_corner_coord(latind:Regind, longind:Regind) = {
    region_indices_to_coord(latind, longind)
  }
  
  // Convert region indices of a tiling region to the coordinate of the
  // center of the region.
  def tiling_region_indices_to_center_coord(latind:Regind, longind:Regind) = {
    offset_region_indices_to_coord(latind, longind, 0.5)
  }
  
  // Convert region indices of a tiling region to the coordinate of the
  // far (i.e. northeast) corner of the region.
  def tiling_region_indices_to_far_corner_coord(latind:Regind, longind:Regind) = {
    offset_region_indices_to_coord(latind, longind, 1)
  }
  
  // Convert region indices of a tiling region to the coordinate of the
  // near (i.e. southwest) corner of the region.
  def stat_region_indices_to_near_corner_coord(latind:Regind, longind:Regind) = {
    region_indices_to_coord(latind, longind)
  }
  
  // Convert region indices of a statistical region to the coordinate of the
  // center of the region.
  def stat_region_indices_to_center_coord(latind:Regind, longind:Regind) = {
    offset_region_indices_to_coord(latind, longind,
        width_of_stat_region/2.0)
  }
  
  // Convert region indices of a statistical region to the coordinate of the
  // far (i.e. northeast) corner of the region.
  def stat_region_indices_to_far_corner_coord(latind:Regind, longind:Regind) = {
    offset_region_indices_to_coord(latind, longind,
        width_of_stat_region)
  }
  
  // Convert region indices of a statistical region to the coordinate of the
  // northwest corner of the region.
  def stat_region_indices_to_nw_corner_coord(latind:Regind, longind:Regind) = {
    region_indices_to_coord(latind + width_of_stat_region, longind, "coerce")
  }
  
  // Convert region indices of a statistical region to the coordinate of the
  // southeast corner of the region.
  def stat_region_indices_to_se_corner_coord(latind:Regind, longind:Regind) = {
    region_indices_to_coord(latind, longind + width_of_stat_region, "coerce")
  }
  
  // Convert region indices of a statistical region to the coordinate of the
  // southwest corner of the region.
  def stat_region_indices_to_sw_corner_coord(latind:Regind, longind:Regind) = {
    stat_region_indices_to_near_corner_coord(latind, longind)
  }
  
  // Convert region indices of a statistical region to the coordinate of the
  // northeast corner of the region.
  def stat_region_indices_to_ne_corner_coord(latind:Regind, longind:Regind) = {
    stat_region_indices_to_far_corner_coord(latind, longind)
  }
}

