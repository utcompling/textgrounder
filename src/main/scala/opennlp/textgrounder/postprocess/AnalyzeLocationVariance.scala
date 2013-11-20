//  AnalyzeLocationVariance.scala
//
//  Copyright (C) 2013 Ben Wing, The University of Texas at Austin
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
package postprocess

import collection.mutable

import util.argparser._
import util.collection._
import util.spherical._
import util.experiment._
import util.io
import util.math._
import util.print._
import util.Serializer
import util.textdb._

import util.debug._

class AnalyzeLocationVarianceParameters(ap: ArgParser) {
  var output =
    ap.option[String]("o", "output",
      metavar = "FILE",
      help = """File prefix of written-out documents. Documents are written
as a textdb corpus, i.e. two files will be written, formed by adding
`WORD.data.txt` and `WORD.schema.txt` to the prefix, with the former
storing the data as tab-separated fields and the latter naming the fields.""")

  var input = ap.positional[String]("input",
    help = """Results file to analyze, a textdb database. The value can be
  any of the following: Either the data or schema file of the database;
  the common prefix of the two; or the directory containing them, provided
  there is only one textdb in the directory.""")
}

/**
 * An application to analyze the different positions seen in a given user's
 * tweets. This assumes that ParseTweets has been run with the argument
 * --output-fields 'default positions' so that a field is included that
 * contains all the locations in a user's tweets.
 */
object AnalyzeLocationVariance extends ExperimentApp("classify") {

  type TParam = AnalyzeLocationVarianceParameters
  type Timestamp = Long

  def create_param_object(ap: ArgParser) = new AnalyzeLocationVarianceParameters(ap)

  def initialize_parameters() {
  }

  def cartesian_product[T1, T2](A: Seq[T1], B: Seq[T2]): Iterable[(T1, T2)] = {
    for (a <- A; b <- B) yield (a, b)
  }

  def output_fraction(frac: (Double, Double)) =
    "%s,%s" format (frac._1, frac._2)

  def fraction_from_bounding_box(sw: SphereCoord, ne: SphereCoord,
      point: SphereCoord) = {
    ((point.lat - sw.lat) / (ne.lat - sw.lat),
     (point.long - sw.long) / (ne.long - sw.long))
  }

  case class LocationStats(
    val bounding_box_sw: SphereCoord,
    val bounding_box_ne: SphereCoord,
    val dist_across_bounding_box: Double,
    val centroid: SphereCoord,
    val centroid_fraction: (Double, Double),
    val earliest: SphereCoord,
    val earliest_dist_from_centroid: Double,
    val latest: SphereCoord,
    val latest_dist_from_centroid: Double,
    val avgdist_from_centroid: Double,
    val mindist_from_centroid: Double,
    val quantile25_dist_from_centroid: Double,
    val median_dist_from_centroid: Double,
    val quantile75_dist_from_centroid: Double,
    val maxdist_from_centroid: Double,
    val maxdist_between_points: Double,
    val dist_variance: Double
  ) {
    def to_row = {
      Seq("bounding-box-sw" -> bounding_box_sw,
         "bounding-box-ne" -> bounding_box_ne,
         "dist-across-bounding-box" -> dist_across_bounding_box,
         "centroid" -> centroid,
         "centroid-fraction" -> output_fraction(centroid_fraction),
         "earliest" -> earliest,
         "earliest-dist-from-centroid" -> earliest_dist_from_centroid,
         "latest" -> latest,
         "latest-dist-from-centroid" -> latest_dist_from_centroid,
         "avgdist-from-centroid" -> avgdist_from_centroid,
         "mindist-from-centroid" -> mindist_from_centroid,
         "quantile25-dist-from-centroid" -> quantile25_dist_from_centroid,
         "median-dist-from-centroid" -> median_dist_from_centroid,
         "quantile75-dist-from-centroid" -> quantile75_dist_from_centroid,
         "maxdist-from-centroid" -> maxdist_from_centroid,
         "maxdist-between-points" -> maxdist_between_points,
         "dist-variance" -> dist_variance
        )
    }
  }

  /** Compute centroid, average distance from centroid, max distance
    * from centroid, variance of distances, max distance between any two
    * points. */
  def compute_location_stats(ts_points: IndexedSeq[(Timestamp, SphereCoord)]
    ) = {
    val points = ts_points.map(_._2)
    val centroid = SphereCoord.centroid(points)
    val raw_distances = ts_points.map {
      case (ts, coord) => (ts, spheredist(coord, centroid))
    }
    val distances = raw_distances.sortBy(_._2).map(_._2)
    val ts_points_by_time = ts_points.sortBy(_._1).map(_._2)
    val earliest = ts_points_by_time.head
    val latest = ts_points_by_time.last
    val bounding_box_sw = SphereCoord.bounding_box_sw(points)
    val bounding_box_ne = SphereCoord.bounding_box_ne(points)

    LocationStats(
      bounding_box_sw = bounding_box_sw,
      bounding_box_ne = bounding_box_ne,
      dist_across_bounding_box = spheredist(bounding_box_sw, bounding_box_ne),
      centroid = centroid,
      centroid_fraction = fraction_from_bounding_box(bounding_box_sw,
        bounding_box_ne, centroid),
      earliest = earliest,
      earliest_dist_from_centroid = spheredist(earliest, centroid),
      latest = latest,
      latest_dist_from_centroid = spheredist(latest, centroid),
      avgdist_from_centroid = mean(distances),
      mindist_from_centroid = distances.head,
      quantile25_dist_from_centroid = quantile_at(distances, 0.25),
      median_dist_from_centroid = quantile_at(distances, 0.5),
      quantile75_dist_from_centroid = quantile_at(distances, 0.75),
      maxdist_from_centroid = distances.last,
      maxdist_between_points = cartesian_product(points, points).map {
        case (a, b) => spheredist(a, b)
      }.max,
      dist_variance = variance(distances)
    )
  }

  def run_program(args: Array[String]) = {
    val filehand = io.localfh
    val newrows = TextDB.read_textdb(filehand, params.input) map { row =>
      val positions = Decoder.string_map_seq(row.gets("positions")).
        map {
          case (timestamp, coord) => (
            timestamp.toLong, SphereCoord.deserialize(coord))
        }
      row.clone_with_added_values(compute_location_stats(positions).to_row)
    }
    TextDB.write_textdb(filehand, params.output, newrows)
    0
  }
}
